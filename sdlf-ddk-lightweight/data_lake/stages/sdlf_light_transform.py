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
from aws_cdk.aws_stepfunctions_tasks import LambdaInvoke
from aws_cdk.aws_stepfunctions import JsonPath
from aws_ddk_core.pipelines import StateMachineStage
from aws_ddk_core.stages import SqsToLambdaStage
from aws_cdk.aws_kms import IKey
from aws_cdk.aws_events import EventPattern, IRuleTarget
from aws_cdk.aws_events_targets import LambdaFunction
from aws_cdk.aws_iam import Effect, PolicyStatement
from aws_cdk.aws_lambda import Code, IFunction, LayerVersion, Runtime, ILayerVersion
from aws_cdk import aws_stepfunctions as sfn
from aws_cdk.aws_iam import Effect, ManagedPolicy, PolicyDocument, PolicyStatement, Role, ServicePrincipal, IRole
from aws_cdk.aws_s3 import IBucket
from aws_cdk.aws_ssm import StringParameter
from aws_cdk.aws_sqs import DeadLetterQueue, QueueEncryption
import aws_cdk as cdk
from aws_ddk_core.resources import KMSFactory, SQSFactory, LambdaFactory
from aws_cdk.custom_resources import Provider


@dataclass
class SDLFLightTransformConfig:
    team: str
    pipeline: str
    raw_bucket: IBucket
    raw_bucket_key: IKey
    stage_bucket: IBucket
    stage_bucket_key: IKey
    routing_lambda: IFunction
    data_lake_lib: ILayerVersion
    register_provider :Provider
    wrangler_layer: ILayerVersion




class SDLFLightTransform(StateMachineStage):
    def __init__(
        self,
        scope,
        name: str,
        prefix:str,
        id: str,
        environment_id: str,
        config: SDLFLightTransformConfig,
        props: Dict[str, Any],
        **kwargs: Any,
    ) -> None:
        super().__init__(scope, id, name, **kwargs)

        self._config: SDLFLightTransformConfig = config
        self._environment_id: str = environment_id
        self._prefix = prefix

        self._props: Dict[str, Any] = props

        service_setup_properties = {"RegisterProperties": self._props}

        cdk.CustomResource(
            self,
            f"{self._props['id']}-{self._props['type']}-custom-resource",
            service_token=self._config.register_provider.service_token,
            properties=service_setup_properties
        )

        self.team = self._config.team
        self.pipeline = self._config.pipeline

        self._routing_queue, self._routing_dlq = self._create_routing_sqs_lambda(self.team, self.pipeline)

        preudate_task = self._create_preupdate_lambda(self.team, self.pipeline)

        process_task = self._create_process_lambda(self.team, self.pipeline)

        postupdate_task = self._create_postupdate_lambda(self.team, self.pipeline)

        error_task = self._create_error_lambda(self.team, self.pipeline)

        self._create_redrive_lambda(self.team, self.pipeline)

        # SUCCEED STATE
        success_state = sfn.Succeed(self, f"{self._prefix}-{self.team}-{self.pipeline}-success")

        # FAIL STATE
        fail_state = sfn.Fail(self, f"{self._prefix}-{self.team}-{self.pipeline}-fail", error="States.ALL")

       # CREATE PARALLEL STATE DEFINITION
        parallel_state = sfn.Parallel(self, f"{self._prefix}-{self.team}-{self.pipeline}-ParallelSM-A")

        parallel_state.branch(preudate_task
                .next(process_task)
                .next(postupdate_task)
                )
        
        parallel_state.next(success_state)

        parallel_state.add_catch(
                error_task,
                errors = ["States.ALL"],
                result_path = JsonPath.DISCARD
                )
        
        error_task.next(fail_state)

        self.build_state_machine(
            id=f"{self._prefix}-{self.team}-{self.pipeline}-state-machine-a",
            environment_id=environment_id,
            definition=(
                parallel_state               
            ),
            additional_role_policy_statements=[PolicyStatement(
                effect=Effect.ALLOW,
                actions=[
                    "lambda:InvokeFunction"
                ],
                resources=[f"arn:aws:lambda:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:function:{self._prefix}-{self.team}-{self.pipeline}-*"],
            )]
        )
        StringParameter(
            self,
            f"{self._prefix}-{self.team}-{self.pipeline}-state-machine-a-ssm",
            parameter_name=f"/SDLF/SM/{self.team}/{self.pipeline}StageASM",
            string_value=self.state_machine.state_machine_arn,
        )


    def _create_preupdate_lambda(self, team, pipeline) -> LambdaInvoke:
        preupdate_lambda_role = Role(
            self,
            f"{self._prefix}-preupdate-role-{team}-{pipeline}-a",
            role_name=f"{self._prefix}-preupdate-role-{team}-{pipeline}-a",
            assumed_by=ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole")],
        )
        ManagedPolicy(
            self,
            f"{self._prefix}-preupdate-policy-{team}-{pipeline}-a",
            managed_policy_name = f"{self._prefix}-preupdate-policy-{team}-{pipeline}-a",
            roles=[preupdate_lambda_role],
            document=PolicyDocument(
                statements=[
                    PolicyStatement(
                        effect=Effect.ALLOW,
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
        
        preupdate_lambda = LambdaFactory.function(
            self,
            f"{self._prefix}-{team}-{pipeline}-preupdate",
            environment_id = self._environment_id,
            function_name=f"{self._prefix}-{team}-{pipeline}-preupdate-a",
            code=Code.from_asset(os.path.join(f"{Path(__file__).parents[1]}", "src/lambdas/sdlf_light_transform/preupdate-metadata")),
            handler="handler.lambda_handler",
            role = preupdate_lambda_role,
            description="preupdate metadata",
            timeout=cdk.Duration.minutes(10),
            memory_size=256,
            runtime = Runtime.PYTHON_3_9,
            layers = [self._config.data_lake_lib]
        )

        preupdate_task = LambdaInvoke(
            self,
            f"{self._prefix}-{team}-{pipeline}-preupdate-task",
            lambda_function=preupdate_lambda,
            result_path=None
        )
        return preupdate_task

    def _create_process_lambda(self, team, pipeline) -> LambdaInvoke:
        process_lambda_role = Role(
            self,
            f"{self._prefix}-process-role-{team}-{pipeline}-a",
            role_name=f"{self._prefix}-process-role-{team}-{pipeline}-a",
            assumed_by=ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole")],
        )
        ManagedPolicy(
            self,
            f"{self._prefix}-process-policy-{team}-{pipeline}-a",
            managed_policy_name = f"{self._prefix}-process-policy-{team}-{pipeline}-a",
            roles=[process_lambda_role],
            document=PolicyDocument(
                statements=[
                    PolicyStatement(
                        effect=Effect.ALLOW,
                        actions=[
                            "s3:Get*",
                            "s3:List*",
                            "s3-object-lambda:Get*",
                            "s3-object-lambda:List*"
                        ],
                        resources=["*"],
                    ),
                    PolicyStatement(
                        effect=Effect.ALLOW,
                        actions=[
                            "kms:CreateGrant",
                            "kms:Decrypt",
                            "kms:DescribeKey",
                            "kms:ReEncryptTo",
                            "kms:ReEncryptFrom",
                            "kms:ListAliases"
                        ],
                        resources=["*"],
                        conditions={
                            "ForAnyValue:StringLike":{
                                "kms:ResourceAliases": ["alias/*"]
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
                            "dynamodb:GetItem",
                            "dynamodb:Scan",
                            "dynamodb:BatchWriteItem",
                            "dynamodb:PutItem",
                            "dynamodb:UpdateItem",
                            "dynamodb:DeleteItem",
                            "dynamodb:DescribeTable"
                        ],
                        resources=[
                            f"arn:aws:dynamodb:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:table/octagon-*"],
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
        self._config.raw_bucket_key.grant_decrypt(process_lambda_role)
        self._config.raw_bucket.grant_read(process_lambda_role)
        self._config.stage_bucket_key.grant_encrypt(process_lambda_role)
        self._config.stage_bucket.grant_write(process_lambda_role)

        process_lambda = LambdaFactory.function(
            self,
            f"{self._prefix}-{team}-{pipeline}-process",
            environment_id = self._environment_id,
            function_name=f"{self._prefix}-{team}-{pipeline}-process-a",
            code=Code.from_asset(os.path.join(f"{Path(__file__).parents[1]}", "src/lambdas/sdlf_light_transform/process-object")),
            handler="handler.lambda_handler",
            role = process_lambda_role,
            description="executes lights transform",
            timeout=cdk.Duration.minutes(15),
            memory_size=1536,
            runtime = Runtime.PYTHON_3_9,
            layers = [self._config.wrangler_layer, self._config.data_lake_lib]
        )

        process_task = LambdaInvoke(
            self,
            f"{self._prefix}-{team}-{pipeline}-process-task",
            lambda_function=process_lambda,
            result_path="$.Payload.body.processedKeys"
        )
        return process_task

    def _create_postupdate_lambda(self, team, pipeline) -> LambdaInvoke:
        postupdate_lambda_role = Role(
            self,
            f"{self._prefix}-postupdate-role-{team}-{pipeline}-a",
            role_name=f"{self._prefix}-postupdate-role-{team}-{pipeline}-a",
            assumed_by=ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole")],
        )
        ManagedPolicy(
            self,
            f"{self._prefix}-postupdate-policy-{team}-{pipeline}-a",
            managed_policy_name = f"{self._prefix}-postupdate-policy-{team}-{pipeline}-a",
            roles=[postupdate_lambda_role],
            document=PolicyDocument(
                statements=[
                    PolicyStatement(
                        effect=Effect.ALLOW,
                        actions=[
                            "s3:Get*",
                            "s3:List*",
                            "s3-object-lambda:Get*",
                            "s3-object-lambda:List*"
                        ],
                        resources=["*"],
                    ),
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

        postupdate_lambda = LambdaFactory.function(
            self,
            f"{self._prefix}-{self.team}-{team}-{pipeline}-post-update",
            environment_id = self._environment_id,
            function_name=f"{self._prefix}-{team}-{pipeline}-postupdate-a",
            code=Code.from_asset(os.path.join(f"{Path(__file__).parents[1]}", "src/lambdas/sdlf_light_transform/postupdate-metadata")),
            handler="handler.lambda_handler",
            environment={
                "stage_bucket": f"{self._prefix}-{self._environment_id}-{cdk.Aws.REGION}-{cdk.Aws.ACCOUNT_ID}-stage"
            },
            role = postupdate_lambda_role,
            description="post update metadata",
            timeout=cdk.Duration.minutes(10),
            memory_size=256,
            runtime = Runtime.PYTHON_3_9,
            layers = [self._config.data_lake_lib]
        )

        postupdate_task = LambdaInvoke(
            self,
            f"{self._prefix}-{team}-{pipeline}-postupdate-task",
            lambda_function=postupdate_lambda,
            result_path="$.statusCode"
        )
        return postupdate_task

    def _create_error_lambda(self, team, pipeline) -> LambdaInvoke:
        error_lambda_role = Role(
            self,
            f"{self._prefix}-error-role-{team}-{pipeline}-a",
            role_name=f"{self._prefix}-error-role-{team}-{pipeline}-a",
            assumed_by=ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole")],
        )
        ManagedPolicy(
            self,
            f"{self._prefix}-error-policy-{team}-{pipeline}-a",
            managed_policy_name = f"{self._prefix}-error-policy-{team}-{pipeline}-a",
            roles=[error_lambda_role],
            document=PolicyDocument(
                statements=[
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
        self._routing_dlq.grant_send_messages(error_lambda_role)

        error_lambda = LambdaFactory.function(
            self,
            f"{self._prefix}-{team}-{pipeline}-error-a",
            environment_id = self._environment_id,
            function_name=f"{self._prefix}-{team}-{pipeline}-error-a",
            code=Code.from_asset(os.path.join(f"{Path(__file__).parents[1]}", "src/lambdas/sdlf_light_transform/error")),
            handler="handler.lambda_handler",
            role = error_lambda_role,
            description="send errors to DLQ",
            timeout=cdk.Duration.minutes(10),
            memory_size=256,
            runtime = Runtime.PYTHON_3_9,
            layers = [self._config.data_lake_lib]
        )
 
        error_task = LambdaInvoke(
            self,
            f"{self._prefix}-{team}-{pipeline}-error-task",
            lambda_function=error_lambda,
            result_path=None
        )
        return error_task

    def _create_redrive_lambda(self, team, pipeline) -> None:
        redrive_lambda_role = Role(
            self,
            f"{self._prefix}-redrive-role-{team}-{pipeline}-a",
            role_name=f"{self._prefix}-redrive-role-{team}-{pipeline}-a",
            assumed_by=ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole")],
        )
        ManagedPolicy(
            self,
            f"{self._prefix}-redrive-policy-{team}-{pipeline}-a",
            managed_policy_name = f"{self._prefix}-redrive-policy-{team}-{pipeline}-a",
            roles=[redrive_lambda_role],
            document=PolicyDocument(
                statements=[
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
        self._routing_dlq.grant_consume_messages(redrive_lambda_role)
        self._routing_queue.grant_send_messages(redrive_lambda_role)

        LambdaFactory.function(
            self,
            f"{self._prefix}-{team}-{pipeline}-redrive-a",
            environment_id = self._environment_id,
            function_name=f"{self._prefix}-{team}-{pipeline}-redrive-a",
            code=Code.from_asset(os.path.join(f"{Path(__file__).parents[1]}", "src/lambdas/sdlf_light_transform/redrive")),
            handler="handler.lambda_handler",
            environment={
                "TEAM": self.team,
                "PIPELINE": self.pipeline,
                "STAGE": "StageA"
            },
            role = redrive_lambda_role,
            description="Redrive Step Function stageA",
            timeout=cdk.Duration.minutes(10),
            memory_size=256,
            runtime = Runtime.PYTHON_3_9,
            layers = [self._config.data_lake_lib]
        )

        return None

    def _create_routing_sqs_lambda(self, team, pipeline) -> None:
        #SQS and DLQ
        #sqs kms key resource
        sqs_key = KMSFactory.key(
            self,
            f"{self._prefix}-{team}-{pipeline}-sqs-key-a",
            environment_id = self._environment_id,
            description=f"{self._prefix} SQS Key Stage A",
            alias=f"{self._prefix}-{team}-{pipeline}-sqs-stage-a-key",
            enable_key_rotation=True,
            pending_window=cdk.Duration.days(30),
            removal_policy=cdk.RemovalPolicy.DESTROY,
        )

        routing_dead_letter_queue = SQSFactory.queue(
            self, 
            id=f'{self._prefix}-{team}-{pipeline}-dlq-a.fifo',
            environment_id= self._environment_id,
            queue_name=f'{self._prefix}-{team}-{pipeline}-dlq-a.fifo', 
            fifo=True,
            visibility_timeout=cdk.Duration.seconds(60),
            encryption=QueueEncryption.KMS,
            encryption_master_key=sqs_key
        )

        routing_dlq = DeadLetterQueue(
            max_receive_count=1, 
            queue=routing_dead_letter_queue
        )

        StringParameter(
            self,
            f'{self._prefix}-{team}-{pipeline}-dlq-a.fifo-ssm',
            parameter_name=f"/SDLF/SQS/{team}/{pipeline}StageADLQ",
            string_value=f'{self._prefix}-{team}-{pipeline}-dlq-a.fifo',
        )

        routing_queue = SQSFactory.queue(
            self, 
            id=f'{self._prefix}-{team}-{pipeline}-queue-a.fifo', 
            environment_id= self._environment_id,
            queue_name=f'{self._prefix}-{team}-{pipeline}-queue-a.fifo', 
            fifo=True,
            content_based_deduplication=True,
            visibility_timeout=cdk.Duration.seconds(60),
            encryption=QueueEncryption.KMS,
            encryption_master_key=sqs_key, 
            dead_letter_queue=routing_dlq)

        StringParameter(
            self,
            f'{self._prefix}-{team}-{pipeline}-queue-a.fifo-ssm',
            parameter_name=f"/SDLF/SQS/{team}/{pipeline}StageAQueue",
            string_value=f'{self._prefix}-{team}-{pipeline}-queue-a.fifo',
        )

        routing_lambda_role = Role(
            self,
            f"{self._prefix}-routing-role-{team}-{pipeline}-a",
            role_name=f"{self._prefix}-routing-role-{team}-{pipeline}-a",
            assumed_by=ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole")],
        )
        ManagedPolicy(
            self,
            f"{self._prefix}-routing-policy-{team}-{pipeline}-a",
            managed_policy_name = f"{self._prefix}-routing-policy-{team}-{pipeline}-a",
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
                            "ssm:GetParameter",
                            "ssm:GetParameters"
                        ],
                        resources=[f"arn:aws:ssm:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:parameter/SDLF/*"],
                    )
                ]
            ),
        )
        sqs_key.grant_encrypt_decrypt(routing_lambda_role)
        routing_queue.grant_consume_messages(routing_lambda_role)

        routing_lambda = LambdaFactory.function(
            self,
            f"{self._prefix}-{team}-{pipeline}-routing",
            environment_id = self._environment_id,
            function_name=f"{self._prefix}-{team}-{pipeline}-routing-a",
            code=Code.from_asset(os.path.join(f"{Path(__file__).parents[1]}", "src/lambdas/sdlf_light_transform/routing")),
            handler="handler.lambda_handler",
            environment={
                "STEPFUNCTION": f"arn:aws:states:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:stateMachine:sdlf-{team}-{pipeline}-sm-a"
            },
            role = routing_lambda_role,
            description="Triggers Step Function",
            timeout=cdk.Duration.minutes(1),
            memory_size=256,
            runtime = Runtime.PYTHON_3_9,
            layers = [self._config.data_lake_lib]
        )

        SqsToLambdaStage(
            self,
            id=f"{self._prefix}-routing-{team}-{pipeline}-sqs-lambda",
            environment_id=self._environment_id,
            lambda_function=routing_lambda,
            sqs_queue=routing_queue
        )

        return routing_queue, routing_dead_letter_queue


    def get_targets(self) -> Optional[List[IRuleTarget]]:
        return [LambdaFunction(self._config.routing_lambda), ]

