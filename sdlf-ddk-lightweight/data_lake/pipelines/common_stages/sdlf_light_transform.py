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


from dataclasses import dataclass
from typing import Any, Dict, List, Optional, cast
import json

import aws_cdk as cdk
from aws_ddk_core.pipelines import StateMachineStage
from aws_ddk_core.stages import SqsToLambdaStage
from aws_ddk_core.resources import KMSFactory, SQSFactory, LambdaFactory
from aws_cdk.custom_resources import Provider
import aws_cdk.aws_stepfunctions_tasks as tasks
import aws_cdk.aws_stepfunctions as sfn
import aws_cdk.aws_kms as kms
import aws_cdk.aws_events as events
import aws_cdk.aws_events_targets as targets
import aws_cdk.aws_iam as iam
import aws_cdk.aws_lambda as lmbda
import aws_cdk.aws_s3 as s3
import aws_cdk.aws_ssm as ssm
import aws_cdk.aws_sqs as sqs
from constructs import Construct


@dataclass
class SDLFLightTransformConfig:
    team: str
    pipeline: str
    raw_bucket: s3.IBucket
    raw_bucket_key: kms.IKey
    stage_bucket: s3.IBucket
    stage_bucket_key: kms.IKey
    routing_lambda: lmbda.IFunction
    data_lake_lib: lmbda.ILayerVersion
    register_provider: Provider
    wrangler_layer: lmbda.ILayerVersion
    runtime: lmbda.Runtime


class SDLFLightTransform(StateMachineStage):

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        name: str,
        prefix: str,
        environment_id: str,
        config: SDLFLightTransformConfig,
        props: Dict[str, Any],
        **kwargs: Any,
    ) -> None:
        super().__init__(scope, construct_id, name, **kwargs)

        self._config: SDLFLightTransformConfig = config
        self._environment_id: str = environment_id
        self._prefix = prefix

        self._props: Dict[str, Any] = props

        service_setup_properties = {"RegisterProperties": json.dumps(self._props)}

        cdk.CustomResource(
            self,
            f"{self._props['id']}-{self._props['type']}-custom-resource",
            service_token=self._config.register_provider.service_token,
            properties=service_setup_properties
        )

        self.team = self._config.team
        self.pipeline = self._config.pipeline

        self._routing_queue, self._routing_dlq, self._sqs_key = self._create_routing_queues()
        self._lambda_role = self._create_lambda_role()

        routing_lambda = self._create_lambda_function("routing", timeout_minutes=1)
        SqsToLambdaStage(
            self,
            id=f"{self._prefix}-routing-{self.team}-{self.pipeline}-sqs-lambda",
            environment_id=self._environment_id,
            lambda_function=routing_lambda,
            sqs_queue=self._routing_queue
        )

        self._create_lambda_function("redrive")

        preupdate_task = self._create_lambda_task("preupdate", None)
        process_task = self._create_lambda_task("process", "$.Payload.body.processedKeys", memory_size=1536)
        postupdate_task = self._create_lambda_task("postupdate", "$.statusCode")
        error_task = self._create_lambda_task("error", None)

        self._create_state_machine(preupdate_task, process_task, postupdate_task, error_task)

    def _create_state_machine(
        self,
        preupdate_task: tasks.LambdaInvoke,
        process_task: tasks.LambdaInvoke,
        postupdate_task: tasks.LambdaInvoke,
        error_task: tasks.LambdaInvoke
    ):
        # Success/Failure States
        success_state = sfn.Succeed(self, f"{self._prefix}-{self.team}-{self.pipeline}-success")
        fail_state = sfn.Fail(self, f"{self._prefix}-{self.team}-{self.pipeline}-fail", error="States.ALL")

        # CREATE PARALLEL STATE DEFINITION
        parallel_state = sfn.Parallel(self, f"{self._prefix}-{self.team}-{self.pipeline}-ParallelSM-A")

        parallel_state.branch(
            preupdate_task
            .next(process_task)
            .next(postupdate_task)
        )

        parallel_state.next(success_state)

        parallel_state.add_catch(
            error_task,
            errors=["States.ALL"],
            result_path=sfn.JsonPath.DISCARD
        )

        error_task.next(fail_state)

        self.build_state_machine(
            id=f"{self._prefix}-{self.team}-{self.pipeline}-state-machine-a",
            environment_id=self._environment_id,
            definition=(
                parallel_state
            ),
            additional_role_policy_statements=[iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "lambda:InvokeFunction"
                ],
                resources=[
                    f"arn:aws:lambda:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}"
                    + f":function:{self._prefix}-{self.team}-{self.pipeline}-*"
                ],
            )]
        )
        ssm.StringParameter(
            self,
            f"{self._prefix}-{self.team}-{self.pipeline}-state-machine-a-ssm",
            parameter_name=f"/SDLF/SM/{self.team}/{self.pipeline}StageASM",
            string_value=self.state_machine.state_machine_arn,
        )

    def _create_lambda_role(self) -> iam.IRole:
        role = cast(
            iam.IRole,
            iam.Role(
                self,
                f"{self._prefix}-role-{self.team}-{self.pipeline}-a",
                role_name=f"{self._prefix}-role-{self.team}-{self.pipeline}-a",
                assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
                managed_policies=[
                    iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole")
                ]
            )
        )

        iam.ManagedPolicy(
            self,
            f"{self._prefix}-policy-{self.team}-{self.pipeline}-a",
            managed_policy_name=f"{self._prefix}-policy-{self.team}-{self.pipeline}-a",
            roles=[role],
            document=iam.PolicyDocument(
                statements=[
                    iam.PolicyStatement(
                        effect=iam.Effect.ALLOW,
                        actions=[
                            "dynamodb:BatchGetItem",
                            "dynamodb:GetRecords",
                            "dynamodb:GetShardIterator",
                            "dynamodb:Query",
                            "dynamodb:GetItem",
                            "dynamodb:Scan",
                            "dynamodb:BatchWriteItem",
                            "dynamodb:PutItem",
                            "dynamodb:UpdateItem",
                            "dynamodb:DeleteItem",
                            "dynamodb:DescribeTable"
                        ],
                        resources=[f"arn:aws:dynamodb:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:table/octagon-*"],
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
                            "kms:ListAliases"
                        ],
                        resources=["*"],
                        conditions={
                            "ForAnyValue:StringLike": {
                                "kms:ResourceAliases": [
                                    f"alias/{self._prefix}-{self.team}-*",
                                    f"alias/{self._prefix}-octagon-*"
                                ]
                            }
                        }
                    ),
                    iam.PolicyStatement(
                        effect=iam.Effect.ALLOW,
                        actions=[
                            "ssm:GetParameter",
                            "ssm:GetParameters"
                        ],
                        resources=[f"arn:aws:ssm:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:parameter/*"],
                    ),
                    iam.PolicyStatement(
                        effect=iam.Effect.ALLOW,
                        actions=[
                            "s3:Get*",
                            "s3:List*",
                            "s3-object-lambda:Get*",
                            "s3-object-lambda:List*"
                        ],
                        resources=["*"],
                    ),
                    iam.PolicyStatement(
                        effect=iam.Effect.ALLOW,
                        actions=[
                            "sqs:SendMessage",
                            "sqs:GetQueueAttributes",
                            "sqs:ListQueues",
                            "sqs:GetQueueUrl",
                            "sqs:ListDeadLetterSourceQueues",
                            "sqs:ListQueueTags"
                        ],
                        resources=[f"arn:aws:sqs:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:{self._prefix}-{self.team}-*"],
                    ),
                    iam.PolicyStatement(
                        effect=iam.Effect.ALLOW,
                        actions=[
                            "states:StartExecution"
                        ],
                        resources=[
                            f"arn:aws:states:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:stateMachine:{self._prefix}*"
                        ],
                    )
                ]
            ),
        )
        self._config.raw_bucket_key.grant_decrypt(role)
        self._config.raw_bucket.grant_read(role)
        self._config.stage_bucket_key.grant_encrypt(role)
        self._config.stage_bucket.grant_write(role)
        self._routing_queue.grant_send_messages(role)
        self._routing_queue.grant_consume_messages(role)
        self._routing_dlq.grant_send_messages(role)
        self._routing_dlq.grant_consume_messages(role)
        self._sqs_key.grant_encrypt_decrypt(role)

        return role

    def _create_lambda_function(
        self,
        step_name: str,
        memory_size: int = 256,
        timeout_minutes: int = 15
    ) -> lmbda.IFunction:
        return LambdaFactory.function(
            self,
            f"{self._prefix}-{self.team}-{self.pipeline}-{step_name}",
            environment_id=self._environment_id,
            function_name=f"{self._prefix}-{self.team}-{self.pipeline}-{step_name}-a",
            code=lmbda.Code.from_asset(
                f"data_lake/src/lambdas/sdlf_light_transform/{step_name}"
            ),
            handler="handler.lambda_handler",
            environment={
                "stage_bucket": f"{self._prefix}-{self._environment_id}-{cdk.Aws.REGION}-{cdk.Aws.ACCOUNT_ID}-stage",
                "TEAM": self.team,
                "PIPELINE": self.pipeline,
                "STAGE": "StageA"
            },
            role=self._lambda_role,
            description=f"execute {step_name} step of light transform",
            timeout=cdk.Duration.minutes(timeout_minutes),
            memory_size=memory_size,
            runtime=self._config.runtime,
            layers=[self._config.wrangler_layer, self._config.data_lake_lib]
        )

    def _create_lambda_task(
        self,
        step_name: str,
        result_path: Optional[str],
        memory_size: int = 256
    ) -> tasks.LambdaInvoke:

        lambda_function = self._create_lambda_function(step_name, memory_size)

        return tasks.LambdaInvoke(
            self,
            f"{self._prefix}-{self.team}-{self.pipeline}-{step_name}-task",
            lambda_function=lambda_function,
            result_path=result_path
        )

    def _create_routing_queues(self):
        sqs_key = KMSFactory.key(
            self,
            f"{self._prefix}-{self.team}-{self.pipeline}-sqs-key-a",
            environment_id=self._environment_id,
            description=f"{self._prefix} SQS Key Stage A",
            alias=f"{self._prefix}-{self.team}-{self.pipeline}-sqs-stage-a-key",
            enable_key_rotation=True,
            pending_window=cdk.Duration.days(30),
            removal_policy=cdk.RemovalPolicy.DESTROY,
        )

        routing_dead_letter_queue = SQSFactory.queue(
            self,
            id=f'{self._prefix}-{self.team}-{self.pipeline}-dlq-a.fifo',
            environment_id=self._environment_id,
            queue_name=f'{self._prefix}-{self.team}-{self.pipeline}-dlq-a.fifo',
            fifo=True,
            visibility_timeout=cdk.Duration.seconds(60),
            encryption=sqs.QueueEncryption.KMS,
            encryption_master_key=sqs_key
        )

        routing_dlq = sqs.DeadLetterQueue(
            max_receive_count=1,
            queue=routing_dead_letter_queue
        )

        ssm.StringParameter(
            self,
            f'{self._prefix}-{self.team}-{self.pipeline}-dlq-a.fifo-ssm',
            parameter_name=f"/SDLF/SQS/{self.team}/{self.pipeline}StageADLQ",
            string_value=f'{self._prefix}-{self.team}-{self.pipeline}-dlq-a.fifo',
        )

        routing_queue = SQSFactory.queue(
            self,
            id=f'{self._prefix}-{self.team}-{self.pipeline}-queue-a.fifo',
            environment_id=self._environment_id,
            queue_name=f'{self._prefix}-{self.team}-{self.pipeline}-queue-a.fifo',
            fifo=True,
            content_based_deduplication=True,
            visibility_timeout=cdk.Duration.seconds(60),
            encryption=sqs.QueueEncryption.KMS,
            encryption_master_key=sqs_key,
            dead_letter_queue=routing_dlq)

        ssm.StringParameter(
            self,
            f'{self._prefix}-{self.team}-{self.pipeline}-queue-a.fifo-ssm',
            parameter_name=f"/SDLF/SQS/{self.team}/{self.pipeline}StageAQueue",
            string_value=f'{self._prefix}-{self.team}-{self.pipeline}-queue-a.fifo',
        )

        return routing_queue, routing_dead_letter_queue, sqs_key

    def get_targets(self) -> Optional[List[events.IRuleTarget]]:
        return [targets.LambdaFunction(self._config.routing_lambda)]
