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


import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional
from aws_cdk.aws_stepfunctions_tasks import LambdaInvoke, CallAwsService
from aws_cdk.aws_glue import CfnCrawler
from aws_cdk.aws_stepfunctions import JsonPath
from aws_cdk.aws_kms import IKey
from aws_ddk_core.pipelines import StateMachineStage
from aws_cdk.aws_events import EventPattern, IRuleTarget
from aws_cdk.aws_events_targets import LambdaFunction
from aws_cdk.aws_iam import Effect, PolicyStatement
from aws_cdk.aws_lambda import Code, LayerVersion, Runtime, ILayerVersion, IFunction
from aws_cdk import aws_stepfunctions as sfn
from aws_cdk.aws_iam import Effect, ManagedPolicy, PolicyDocument, PolicyStatement, Role, ServicePrincipal
from aws_cdk.aws_s3 import IBucket
from aws_cdk.aws_ssm import StringParameter
import aws_cdk as cdk
from aws_cdk.custom_resources import Provider


from aws_ddk_core.resources import LambdaFactory


@dataclass
class SDLFHeavyTransformConfig:
    team: str
    pipeline: str
    stage_bucket: IBucket
    stage_bucket_key: IKey
    data_lake_lib: ILayerVersion
    register_provider : Provider
    wrangler_layer: ILayerVersion
    database_crawler_name: CfnCrawler

class SDLFHeavyTransform(StateMachineStage):
    def __init__(
        self,
        scope,
        name: str,
        prefix: str,
        id: str,
        environment_id: str,
        config: SDLFHeavyTransformConfig,
        props: Dict[str, Any],
        **kwargs: Any,
    ) -> None:
        super().__init__(scope, id, name, **kwargs)

        self._config: SDLFHeavyTransformConfig = config
        self._environment_id: str = environment_id
        self._props: Dict[str, Any] = props
        self._prefix = prefix

        service_setup_properties = {"RegisterProperties": self._props}

        cdk.CustomResource(
            self,
            f"{self._props['id']}-{self._props['type']}-custom-resource",
            service_token=self._config.register_provider.service_token,
            properties=service_setup_properties
        )

        self.team = self._config.team
        self.pipeline = self._config.pipeline
                
        process_task = self._create_process_lambda(self.team, self.pipeline)

        postupdate_task = self._create_postupdate_lambda(self.team, self.pipeline)

        error_task = self._create_error_lambda(self.team, self.pipeline)

        check_job_task = self._create_check_job_lambda(self.team, self.pipeline)

        crawler_task = self._create_crawler_task(self.team, self.pipeline)
        
        self._create_redrive_lambda(self.team, self.pipeline)
        
        self._routing_lambda = self._create_routing_lambda(self.team, self.pipeline)

        # SUCCEED STATE
        success_state = sfn.Succeed(self, f"{self._prefix}-{self.team}-{self.pipeline}-success")

        # FAIL STATE
        fail_state = sfn.Fail(self, f"{self._prefix}-{self.team}-{self.pipeline}-fail", error="States.ALL")

        #JOB FAIL STATE
        job_fail_state = sfn.Fail(
            self, 
            f"{self._prefix}-{self.team}-{self.pipeline}-job-failed-sm-b",
            cause="Job failed, please check the logs",
            error="Job Failed"
        )

        # WAIT STATE
        wait_state = sfn.Wait(
            self, 
            f"{self._prefix}-{self.team}-{self.pipeline}-wait-state",
            time=sfn.WaitTime.duration(cdk.Duration.seconds(15))
        )

       # CREATE PARALLEL STATE DEFINITION
        parallel_state = sfn.Parallel(self, f"{self._prefix}-{self.team}-{self.pipeline}-ParallelSM-B")

        parallel_state.branch(process_task
                .next(wait_state)
                .next(check_job_task)
                .next(sfn.Choice(self, f"{self._prefix}-{self.team}-{self.pipeline}-is job-complete?")
                .when(sfn.Condition.string_equals("$.body.job.Payload.jobDetails.jobStatus", "SUCCEEDED"), crawler_task.next(postupdate_task))
                .when(sfn.Condition.string_equals("$.body.job.Payload.jobDetails.jobStatus", "FAILED"), job_fail_state)
                .otherwise(wait_state))  
                )
        
        parallel_state.next(success_state)

        parallel_state.add_catch(
                error_task,
                errors = ["States.ALL"],
                result_path = JsonPath.DISCARD
                )
        
        error_task.next(fail_state)

        self.build_state_machine(
            id=f"{self._prefix}-{self.team}-{self.pipeline}-state-machine-b",
            environment_id=environment_id,
            definition=(
                parallel_state               
            ),
            additional_role_policy_statements=[
                PolicyStatement(
                    effect=Effect.ALLOW,
                    actions=[
                        "lambda:InvokeFunction"
                    ],
                    resources=[f"arn:aws:lambda:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:function:{self._prefix}-{self.team}-{self.pipeline}-*"],
                ),
                PolicyStatement(
                    effect=Effect.ALLOW,
                    actions=[
                        "glue:StartCrawler"
                    ],
                    resources=[f"arn:aws:glue:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:crawler/{self._prefix}-{self.team}-*"],
                )
            ]
        )
        StringParameter(
            self,
            f"{self._prefix}-{self.team}-{self.pipeline}-state-machine-b-ssm",
            parameter_name=f"/SDLF/SM/{self.team}/{self.pipeline}StageBSM",
            string_value=self.state_machine.state_machine_arn,
        )

    def _create_crawler_task(self,team, pipeline) -> CallAwsService:
        crawler_task = CallAwsService(
            self, 
            f"{self._prefix}-{team}-{pipeline}-crawler-task",
            service="glue",
            action="startCrawler",
            parameters={
                    "Name": sfn.JsonPath.string_at("$.body.crawler_name")
            },
            result_path=JsonPath.DISCARD,
            iam_resources=[f"arn:aws:glue:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:crawler/{self._config.database_crawler_name}"]
        )

        return crawler_task


    def _create_process_lambda(self,team, pipeline) -> LambdaInvoke:
        process_lambda_role = Role(
            self,
            f"{self._prefix}-process-role-{team}-{pipeline}-b",
            role_name=f"{self._prefix}-process-role-{team}-{pipeline}-b",
            assumed_by=ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole")],
        )
        ManagedPolicy(
            self,
            f"{self._prefix}-process-policy-{team}-{pipeline}-b",
            managed_policy_name = f"{self._prefix}-process-policy-{team}-{pipeline}-b",
            roles=[process_lambda_role],
            document=PolicyDocument(
                statements=[
                    PolicyStatement(
                        effect=Effect.ALLOW,
                        actions=[
                            "glue:StartJobRun"
                        ],
                        resources=["*"],
                    ),
                    PolicyStatement(
                        effect=Effect.ALLOW,
                        actions=[
                            "kms:CreateGrant",
                            "kms:Decrypt",
                            "kms:DescribeKey",
                            "kms:Encrypt",
                            "kms:GenerateDataKey*",
                            "kms:ReEncrypt*"
                        ],
                        resources=["*"],
                        conditions={
                            "ForAnyValue:StringLike":{
                                "kms:ResourceAliases": [f"alias/{self._prefix}-octagon-*"]
                            }
                        }
                    ),
                    PolicyStatement(
                        effect=Effect.ALLOW,
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
                            "dynamodb:DescribeTable"
                        ],
                        resources=[f"arn:aws:dynamodb:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:table/octagon-*"],
                    ),
                    PolicyStatement(
                        effect=Effect.ALLOW,
                        actions=[
                            "ssm:GetParameter",
                            "ssm:GetParameters"
                        ],
                        resources=[f"arn:aws:ssm:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:parameter/*"],
                    )
                ]
            ),
        )
        self._config.stage_bucket_key.grant_encrypt_decrypt(process_lambda_role)
        self._config.stage_bucket.grant_read_write(process_lambda_role)
        
        process_lambda = LambdaFactory.function(
            self,
            f"{self._prefix}-{team}-{pipeline}-process-b",
            environment_id = self._environment_id,
            function_name=f"{self._prefix}-{team}-{pipeline}-process-b",
            code=Code.from_asset(os.path.join(f"{Path(__file__).parents[1]}", "src/lambdas/sdlf_heavy_transform/process-object")),
            handler="handler.lambda_handler",
            role = process_lambda_role,
            description="exeute heavy transform",
            timeout=cdk.Duration.minutes(15),
            memory_size=256,
            layers = [self._config.data_lake_lib, self._config.wrangler_layer],
            runtime = Runtime.PYTHON_3_9,
        )
        process_task = LambdaInvoke(
            self,
            f"{self._prefix}-{team}-{pipeline}-process-task-b",
            lambda_function=process_lambda,
            result_path="$.body.job"
        )
        return process_task


    def _create_postupdate_lambda(self,team, pipeline ) -> LambdaInvoke:
        postupdate_lambda_role = Role(
            self,
            f"{self._prefix}-postupdate-role-{team}-{pipeline}-b",
            role_name=f"{self._prefix}-postupdate-role-{team}-{pipeline}-b",
            assumed_by=ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole")],
        )
        ManagedPolicy(
            self,
            f"{self._prefix}-postupdate-policy-{team}-{pipeline}-b",
            managed_policy_name = f"{self._prefix}-postupdate-policy-{team}-{pipeline}-b",
            roles=[postupdate_lambda_role],
            document=PolicyDocument(
                statements=[
                    PolicyStatement(
                        effect=Effect.ALLOW,
                        actions=[
                            "dynamodb:GetItem"
                        ],
                        resources=[f"arn:aws:dynamodb:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:table/octagon-*"],
                    ),
                    PolicyStatement(
                        effect=Effect.ALLOW,
                        actions=[
                            "kms:CreateGrant",
                            "kms:Decrypt",
                            "kms:DescribeKey",
                            "kms:Encrypt",
                            "kms:GenerateDataKey*",
                            "kms:ReEncrypt*"
                        ],
                        resources=["*"],
                        conditions={
                            "ForAnyValue:StringLike":{
                                "kms:ResourceAliases": [f"alias/{self._prefix}-octagon-*"]
                            }
                        }
                    ),
                    PolicyStatement(
                        effect=Effect.ALLOW,
                        actions=[
                            "dynamodb:BatchGetItem",
                            "dynamodb:GetRecords",
                            "dynamodb:GetShardIterator",
                            "dynamodb:Query",
                            "dynamodb:Scan",
                            "dynamodb:BatchWriteItem",
                            "dynamodb:PutItem",
                            "dynamodb:UpdateItem",
                            "dynamodb:DeleteItem",
                            "dynamodb:DescribeTable"
                        ],
                        resources=[f"arn:aws:dynamodb:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:table/octagon-*"],
                    ),
                    PolicyStatement(
                        effect=Effect.ALLOW,
                        actions=[
                            "ssm:GetParameter",
                            "ssm:GetParameters"
                        ],
                        resources=[f"arn:aws:ssm:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:parameter/*"],
                    )
                ]
            ),
        )

        self._config.stage_bucket_key.grant_decrypt(postupdate_lambda_role)
        self._config.stage_bucket.grant_read(postupdate_lambda_role)

        postupdate_lambda = LambdaFactory.function(
            self,
            f"{self._prefix}-{team}-{pipeline}-postupdate-b",
            environment_id = self._environment_id,
            function_name=f"{self._prefix}-{team}-{pipeline}-postupdate-b",
            code=Code.from_asset(os.path.join(f"{Path(__file__).parents[1]}", "src/lambdas/sdlf_heavy_transform/postupdate-metadata")),
            handler="handler.lambda_handler",
            role = postupdate_lambda_role,
            description="post update metadata",
            timeout=cdk.Duration.minutes(10),
            memory_size=256,
            layers = [self._config.data_lake_lib],
            runtime = Runtime.PYTHON_3_9,
        )
        postupdate_task = LambdaInvoke(
            self,
            f"{self._prefix}-{team}-{pipeline}-postupdate-task-b",
            lambda_function=postupdate_lambda,
            result_path="$.statusCode"
        )
        return postupdate_task

    def _create_error_lambda(self,team, pipeline ) -> LambdaInvoke:
        error_lambda_role = Role(
            self,
            f"{self._prefix}-error-role-{team}-{pipeline}-b",
            role_name=f"{self._prefix}-error-role-{team}-{pipeline}-b",
            assumed_by=ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole")],
        )
        ManagedPolicy(
            self,
            f"{self._prefix}-error-policy-{team}-{pipeline}-b",
            managed_policy_name = f"{self._prefix}-error-policy-{team}-{pipeline}-b",
            roles=[error_lambda_role],
            document=PolicyDocument(
                statements=[
                    PolicyStatement(
                        effect=Effect.ALLOW,
                        actions=[
                            "sqs:SendMessage",
                            "sqs:GetQueueAttributes",
                            "sqs:ListQueues",
                            "sqs:GetQueueUrl",
                            "sqs:ListDeadLetterSourceQueues",
                            "sqs:ListQueueTags"
                        ],
                        resources=[f"arn:aws:sqs:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:{self._prefix}-{team}-*"],
                    ),
                    PolicyStatement(
                        effect=Effect.ALLOW,
                        actions=[
                            "kms:CreateGrant",
                            "kms:Decrypt",
                            "kms:DescribeKey",
                            "kms:Encrypt",
                            "kms:GenerateDataKey*",
                            "kms:ReEncrypt*"
                        ],
                        resources=["*"],
                        conditions={
                            "ForAnyValue:StringLike":{
                                "kms:ResourceAliases": [f"alias/{self._prefix}-{team}-*"]
                            }
                        }
                    ),
                    PolicyStatement(
                        effect=Effect.ALLOW,
                        actions=[
                            "ssm:GetParameter",
                            "ssm:GetParameters"
                        ],
                        resources=[f"arn:aws:ssm:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:parameter/*"],
                    )
                ]
            ),
        )

        error_lambda = LambdaFactory.function(
            self,
            f"{self._prefix}-{team}-{pipeline}-error-b",
            environment_id = self._environment_id,
            function_name=f"{self._prefix}-{team}-{pipeline}-error-b",
            code=Code.from_asset(os.path.join(f"{Path(__file__).parents[1]}", "src/lambdas/sdlf_heavy_transform/error")),
            handler="handler.lambda_handler",
            role = error_lambda_role,
            description="send errors to DLQ",
            timeout=cdk.Duration.minutes(10),
            memory_size=256,
            layers = [self._config.data_lake_lib],
            runtime = Runtime.PYTHON_3_9,
        )
        error_task = LambdaInvoke(
            self,
            f"{self._prefix}-{team}-{pipeline}-error-task-b",
            lambda_function=error_lambda,
            result_path=None
        )
        return error_task


    def _create_check_job_lambda(self,team, pipeline ) -> LambdaInvoke:
        check_job_lambda_role = Role(
            self,
            f"{self._prefix}-check-job-role-{team}-{pipeline}-b",
            role_name=f"{self._prefix}-check-job-role-{team}-{pipeline}-b",
            assumed_by=ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole")],
        )
        ManagedPolicy(
            self,
            f"{self._prefix}-check-job-policy-{team}-{pipeline}-b",
            managed_policy_name = f"{self._prefix}-check-job-policy-{team}-{pipeline}-b",
            roles=[check_job_lambda_role],
            document=PolicyDocument(
                statements=[
                    PolicyStatement(
                        effect=Effect.ALLOW,
                        actions=[
                            "dynamodb:UpdateItem"
                        ],
                        resources=[f"arn:aws:dynamodb:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:table/octagon-*"],
                    ),
                    PolicyStatement(
                        effect=Effect.ALLOW,
                        actions=[
                            "glue:GetJobRun"
                        ],
                        resources=["*"],
                    ),
                    PolicyStatement(
                        effect=Effect.ALLOW,
                        actions=[
                            "dynamodb:GetItem"
                        ],
                        resources=[f"arn:aws:dynamodb:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:table/octagon-*"],
                    ),
                    PolicyStatement(
                        effect=Effect.ALLOW,
                        actions=[
                            "kms:CreateGrant",
                            "kms:Decrypt",
                            "kms:DescribeKey",
                            "kms:Encrypt",
                            "kms:GenerateDataKey*",
                            "kms:ReEncrypt*"
                        ],
                        resources=["*"],
                        conditions={
                            "ForAnyValue:StringLike":{
                                "kms:ResourceAliases": [f"alias/{self._prefix}-octagon-*"]
                            }
                        }
                    ),
                    PolicyStatement(
                        effect=Effect.ALLOW,
                        actions=[
                            "ssm:GetParameter",
                            "ssm:GetParameters"
                        ],
                        resources=[f"arn:aws:ssm:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:parameter/*"],
                    )
                ]
            ),
        )

        check_job_lambda = LambdaFactory.function(
            self,
            f"{self._prefix}-{team}-{pipeline}-checkjob-b",
            environment_id = self._environment_id,
            role = check_job_lambda_role,
            function_name=f"{self._prefix}-{team}-{pipeline}-checkjob-b",
            code=Code.from_asset(os.path.join(f"{Path(__file__).parents[1]}", "src/lambdas/sdlf_heavy_transform/check-job")),
            handler="handler.lambda_handler",
            description="check if glue job still running",
            timeout=cdk.Duration.minutes(10),
            memory_size=256,
            layers = [self._config.data_lake_lib, self._config.wrangler_layer],
            runtime = Runtime.PYTHON_3_9,
        )
        check_job_task = LambdaInvoke(
            self,
            f"{self._prefix}-{team}-{pipeline}-check-job-task",
            lambda_function=check_job_lambda,
            result_path="$.body.job"
        )
        return check_job_task

    def _create_routing_lambda(self, team, pipeline ) -> IFunction:
        routing_lambda_role = Role(
            self,
            f"{self._prefix}-routing-role-{team}-{pipeline}-b",
            role_name=f"{self._prefix}-routing-role-{team}-{pipeline}-b",
            assumed_by=ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole")],
        )
        ManagedPolicy(
            self,
            f"{self._prefix}-routing-policy-{team}-{pipeline}-b",
            managed_policy_name = f"{self._prefix}-routing-policy-{team}-{pipeline}-b",
            roles=[routing_lambda_role],
            document=PolicyDocument(
                statements=[
                    PolicyStatement(
                        effect=Effect.ALLOW,
                        actions=[
                            "states:StartExecution"
                        ],
                        resources=[f"arn:aws:states:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:stateMachine:*"],
                    ),
                    PolicyStatement(
                        effect=Effect.ALLOW,
                        actions=[
                            "sqs:ChangeMessageVisibility",
                            "sqs:DeleteMessage",
                            "sqs:ReceiveMessage",
                            "sqs:GetQueueAttributes",
                            "sqs:GetQueueUrl"
                        ],
                        resources=[f"arn:aws:sqs:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:{self._prefix}-{team}-*"],
                    ),
                    PolicyStatement(
                        effect=Effect.ALLOW,
                        actions=[
                            "kms:CreateGrant",
                            "kms:Decrypt",
                            "kms:DescribeKey",
                            "kms:Encrypt",
                            "kms:GenerateDataKey*",
                            "kms:ReEncrypt*"
                        ],
                        resources=["*"],
                        conditions={
                            "ForAnyValue:StringLike":{
                                "kms:ResourceAliases": [f"alias/{self._prefix}-{team}-*"]
                            }
                        }
                    ),
                    PolicyStatement(
                        effect=Effect.ALLOW,
                        actions=[
                            "dynamodb:GetItem"
                        ],
                        resources=[f"arn:aws:dynamodb:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:table/octagon-*"],
                    ),
                    PolicyStatement(
                        effect=Effect.ALLOW,
                        actions=[
                            "kms:CreateGrant",
                            "kms:Decrypt",
                            "kms:DescribeKey",
                            "kms:Encrypt",
                            "kms:GenerateDataKey*",
                            "kms:ReEncrypt*"
                        ],
                        resources=["*"],
                        conditions={
                            "ForAnyValue:StringLike":{
                                "kms:ResourceAliases": [f"alias/{self._prefix}-octagon-*"]
                            }
                        }
                    ),
                    PolicyStatement(
                        effect=Effect.ALLOW,
                        actions=[
                            "ssm:GetParameter",
                            "ssm:GetParameters"
                        ],
                        resources=[f"arn:aws:ssm:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:parameter/*"],
                    )
                ]
            ),
        )

        routing_lambda = LambdaFactory.function(
            self,
            f"{self._prefix}-{team}-{pipeline}-routing-b",
            environment_id = self._environment_id,
            role = routing_lambda_role,
            function_name=f"{self._prefix}-{team}-{pipeline}-routing-b",
            code=Code.from_asset(os.path.join(f"{Path(__file__).parents[1]}", "src/lambdas/sdlf_heavy_transform/routing")),
            handler="handler.lambda_handler",
            description="Triggers Step Function stageB",
            timeout=cdk.Duration.minutes(10),
            memory_size=256,
            layers = [self._config.data_lake_lib],
            runtime = Runtime.PYTHON_3_9,
        )
        return routing_lambda

    def _create_redrive_lambda(self, team, pipeline) -> None:
        redrive_lambda_role = Role(
            self,
            f"{self._prefix}-redrive-role-{team}-{pipeline}-b",
            role_name=f"{self._prefix}-redrive-role-{team}-{pipeline}-b",
            assumed_by=ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole")],
        )
        ManagedPolicy(
            self,
            f"{self._prefix}-redrive-policy-{team}-{pipeline}-b",
            managed_policy_name = f"{self._prefix}-redrive-policy-{team}-{pipeline}-b",
            roles=[redrive_lambda_role],
            document=PolicyDocument(
                statements=[
                    PolicyStatement(
                        effect=Effect.ALLOW,
                        actions=[
                            "sqs:SendMessage",
                            "sqs:ReceiveMessage",
                            "sqs:DeleteMessage",
                            "sqs:GetQueueAttributes",
                            "sqs:ListQueues",
                            "sqs:GetQueueUrl",
                            "sqs:ListDeadLetterSourceQueues",
                            "sqs:ListQueueTags"
                        ],
                        resources=[f"arn:aws:sqs:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:{self._prefix}-{team}-*"],
                    ),
                    PolicyStatement(
                        effect=Effect.ALLOW,
                        actions=[
                            "states:StartExecution"
                        ],
                        resources=[f"arn:aws:states:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:stateMachine:*"],
                    ),
                    PolicyStatement(
                        effect=Effect.ALLOW,
                        actions=[
                            "ssm:GetParameter",
                            "ssm:GetParameters"
                        ],
                        resources=[f"arn:aws:ssm:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:parameter/*"],
                    )
                ]
            ),
        )

        self._redrive_lambda = LambdaFactory.function(
            self,
            f"{self._prefix}-{team}-{pipeline}-redrive-b",
            environment_id = self._environment_id,
            role = redrive_lambda_role,
            function_name=f"{self._prefix}-{team}-{pipeline}-redrive-b",
            code=Code.from_asset(os.path.join(f"{Path(__file__).parents[1]}", "src/lambdas/sdlf_heavy_transform/redrive")),
            handler="handler.lambda_handler",
            environment={
                "TEAM": self.team,
                "PIPELINE": self.pipeline,
                "STAGE": "StageB"
            },
            description="Redrive Step Function stageB",
            timeout=cdk.Duration.minutes(10),
            memory_size=256,
            layers = [self._config.data_lake_lib],
            runtime = Runtime.PYTHON_3_9,
        )
        return None
        

    def get_targets(self) -> Optional[List[IRuleTarget]]:
        return [LambdaFunction(self._routing_lambda), ]

    @property
    def routing_lambda(self):
        return self._routing_lambda
