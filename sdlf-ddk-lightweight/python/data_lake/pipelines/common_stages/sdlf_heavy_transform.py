# Copyright 2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License").
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, cast

import aws_cdk as cdk
import aws_cdk.aws_events as events
import aws_cdk.aws_events_targets as targets
import aws_cdk.aws_iam as iam
import aws_cdk.aws_kms as kms
import aws_cdk.aws_lambda as lmbda
import aws_cdk.aws_s3 as s3
import aws_cdk.aws_ssm as ssm
import aws_cdk.aws_stepfunctions as sfn
import aws_cdk.aws_stepfunctions_tasks as tasks
from aws_cdk.custom_resources import Provider
from aws_ddk_core import StateMachineStage
from constructs import Construct


@dataclass
class SDLFHeavyTransformConfig:
    team: str
    pipeline: str
    stage_bucket: s3.IBucket
    stage_bucket_key: kms.IKey
    data_lake_lib: lmbda.ILayerVersion
    register_provider: Provider
    wrangler_layer: lmbda.ILayerVersion
    runtime: lmbda.Runtime


class SDLFHeavyTransform(StateMachineStage):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        name: str,
        prefix: str,
        environment_id: str,
        config: SDLFHeavyTransformConfig,
        props: Dict[str, Any],
        **kwargs: Any,
    ) -> None:
        super().__init__(scope, id=construct_id, name=name, **kwargs)

        self._config: SDLFHeavyTransformConfig = config
        self._environment_id: str = environment_id
        self._props: Dict[str, Any] = props
        self._prefix = prefix
        self.team = self._config.team
        self.pipeline = self._config.pipeline

        # register heavy transform details in DDB octagon table
        self._register_octagon_config()

        # create lambda execution role
        self._lambda_role = self._create_lambda_role()

        # routing functions
        self._redrive_lambda = self._create_lambda_function("redrive")
        self._routing_lambda = self._create_lambda_function("routing")

        # state machine steps
        process_task = self._create_lambda_task("process", "$.body.job")
        postupdate_task = self._create_lambda_task("postupdate", "$.statusCode")
        error_task = self._create_lambda_task("error", None)
        check_job_task = self._create_lambda_task("check-job", "$.body.job")

        # build state machine
        self._build_state_machine(
            process_task, postupdate_task, error_task, check_job_task
        )

    def _build_state_machine(
        self,
        process_task: tasks.LambdaInvoke,
        postupdate_task: tasks.LambdaInvoke,
        error_task: tasks.LambdaInvoke,
        check_job_task: tasks.LambdaInvoke,
    ):
        # Success/Failure/Wait States
        success_state = sfn.Succeed(
            self, f"{self._prefix}-{self.team}-{self.pipeline}-success"
        )
        fail_state = sfn.Fail(
            self, f"{self._prefix}-{self.team}-{self.pipeline}-fail", error="States.ALL"
        )
        job_fail_state = sfn.Fail(
            self,
            f"{self._prefix}-{self.team}-{self.pipeline}-job-failed-sm-b",
            cause="Job failed, please check the logs",
            error="Job Failed",
        )
        wait_state = sfn.Wait(
            self,
            f"{self._prefix}-{self.team}-{self.pipeline}-wait-state",
            time=sfn.WaitTime.duration(cdk.Duration.seconds(15)),
        )

        # CREATE PARALLEL STATE DEFINITION
        parallel_state = sfn.Parallel(
            self, f"{self._prefix}-{self.team}-{self.pipeline}-ParallelSM-B"
        )

        parallel_state.branch(
            process_task.next(wait_state)
            .next(check_job_task)
            .next(
                sfn.Choice(
                    self, f"{self._prefix}-{self.team}-{self.pipeline}-is job-complete?"
                )
                .when(
                    sfn.Condition.string_equals(
                        "$.body.job.Payload.jobDetails.jobStatus", "SUCCEEDED"
                    ),
                    postupdate_task,
                )
                .when(
                    sfn.Condition.string_equals(
                        "$.body.job.Payload.jobDetails.jobStatus", "FAILED"
                    ),
                    job_fail_state,
                )
                .otherwise(wait_state)
            )
        )

        parallel_state.next(success_state)

        parallel_state.add_catch(
            error_task, errors=["States.ALL"], result_path=sfn.JsonPath.DISCARD
        )

        error_task.next(fail_state)

        state_object = self._create_state_machine(
            name=f"{self._prefix}-{self.team}-{self.pipeline}-state-machine-b",
            definition=(parallel_state),
            additional_role_policy_statements=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=["lambda:InvokeFunction"],
                    resources=[
                        f"arn:aws:lambda:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:"
                        + f"function:{self._prefix}-{self.team}-{self.pipeline}-*"
                    ],
                )
            ],
        )

        self._event_pattern, self._state_machine, self._targets = (
            state_object.event_pattern,
            state_object.state_machine,
            state_object.targets,
        )

        ssm.StringParameter(
            self,
            f"{self._prefix}-{self.team}-{self.pipeline}-state-machine-b-ssm",
            parameter_name=f"/SDLF/SM/{self.team}/{self.pipeline}StageBSM",
            string_value=self._state_machine.state_machine_arn,
        )

    def _register_octagon_config(self):
        service_setup_properties = {"RegisterProperties": json.dumps(self._props)}

        cdk.CustomResource(
            self,
            f"{self._props['id']}-{self._props['type']}-custom-resource",
            service_token=self._config.register_provider.service_token,
            properties=service_setup_properties,
        )

    def _create_lambda_role(self) -> iam.IRole:
        role = cast(
            iam.IRole,
            iam.Role(
                self,
                f"{self._prefix}-lambda-role-{self.team}-{self.pipeline}-b",
                role_name=f"{self._prefix}-lambda-role-{self.team}-{self.pipeline}-b",
                assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
                managed_policies=[
                    iam.ManagedPolicy.from_aws_managed_policy_name(
                        "service-role/AWSLambdaBasicExecutionRole"
                    )
                ],
            ),
        )

        iam.ManagedPolicy(
            self,
            f"{self._prefix}-lambda-policy-{self.team}-{self.pipeline}-b",
            managed_policy_name=f"{self._prefix}-lambda-policy-{self.team}-{self.pipeline}-b",
            roles=[role],
            document=iam.PolicyDocument(
                statements=[
                    iam.PolicyStatement(
                        effect=iam.Effect.ALLOW,
                        actions=["glue:StartJobRun", "glue:GetJobRun"],
                        resources=["*"],
                    ),
                    iam.PolicyStatement(
                        effect=iam.Effect.ALLOW,
                        actions=[
                            "kms:CreateGrant",
                            "kms:Decrypt",
                            "kms:DescribeKey",
                            "kms:Encrypt",
                            "kms:GenerateDataKey*",
                            "kms:ReEncrypt*",
                        ],
                        resources=["*"],
                        conditions={
                            "ForAnyValue:StringLike": {
                                "kms:ResourceAliases": [
                                    f"alias/{self._prefix}-octagon-*",
                                    f"alias/{self._prefix}-{self.team}-*",
                                ]
                            }
                        },
                    ),
                    iam.PolicyStatement(
                        effect=iam.Effect.ALLOW,
                        actions=[
                            "dynamodb:BatchGetItem",
                            "dynamodb:GetItem",
                            "dynamodb:GetRecords",
                            "dynamodb:GetShardIterator",
                            "dynamodb:Query",
                            "dynamodb:Scan",
                            "dynamodb:BatchWriteItem",
                            "dynamodb:PutItem",
                            "dynamodb:UpdateItem",
                            "dynamodb:DeleteItem",
                            "dynamodb:DescribeTable",
                        ],
                        resources=[
                            f"arn:aws:dynamodb:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:table/octagon-*"
                        ],
                    ),
                    iam.PolicyStatement(
                        effect=iam.Effect.ALLOW,
                        actions=["ssm:GetParameter", "ssm:GetParameters"],
                        resources=[
                            f"arn:aws:ssm:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:parameter/*"
                        ],
                    ),
                    iam.PolicyStatement(
                        effect=iam.Effect.ALLOW,
                        actions=[
                            "sqs:ChangeMessageVisibility",
                            "sqs:SendMessage",
                            "sqs:ReceiveMessage",
                            "sqs:DeleteMessage",
                            "sqs:GetQueueAttributes",
                            "sqs:ListQueues",
                            "sqs:GetQueueUrl",
                            "sqs:ListDeadLetterSourceQueues",
                            "sqs:ListQueueTags",
                        ],
                        resources=[
                            f"arn:aws:sqs:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:{self._prefix}-{self.team}-*"
                        ],
                    ),
                    iam.PolicyStatement(
                        effect=iam.Effect.ALLOW,
                        actions=["states:StartExecution"],
                        resources=[
                            f"arn:aws:states:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:stateMachine:{self._prefix}*"
                        ],
                    ),
                ]
            ),
        )
        self._config.stage_bucket_key.grant_encrypt_decrypt(role)
        self._config.stage_bucket.grant_read_write(role)

        return role

    def _create_lambda_function(self, step_name: str) -> lmbda.IFunction:
        return lmbda.Function(
            self,
            f"{self._prefix}-{self.team}-{self.pipeline}-{step_name}-b",
            function_name=f"{self._prefix}-{self.team}-{self.pipeline}-{step_name}-b",
            code=lmbda.Code.from_asset(
                f"data_lake/src/lambdas/sdlf_heavy_transform/{step_name}"
            ),
            handler="handler.lambda_handler",
            environment={
                "TEAM": self.team,
                "PIPELINE": self.pipeline,
                "STAGE": "StageB",
            },
            role=self._lambda_role,
            description=f"exeute {step_name} step of heavy transform.",
            timeout=cdk.Duration.minutes(15),
            memory_size=256,
            layers=[self._config.data_lake_lib, self._config.wrangler_layer],
            runtime=self._config.runtime,
        )

    def _create_lambda_task(
        self, step_name: str, result_path: Optional[str]
    ) -> tasks.LambdaInvoke:
        lambda_function = self._create_lambda_function(step_name)

        lambda_task = tasks.LambdaInvoke(
            self,
            f"{self._prefix}-{self.team}-{self.pipeline}-{step_name}-task-b",
            lambda_function=lambda_function,
            result_path=result_path,
        )

        return lambda_task

    @property
    def targets(self) -> Optional[List[events.IRuleTarget]]:
        return [
            targets.LambdaFunction(self._routing_lambda),
        ]

    @property
    def state_machine(self):
        return self._state_machine

    @property
    def event_pattern(self):
        return self._event_pattern

    @property
    def routing_lambda(self):
        return self._routing_lambda
