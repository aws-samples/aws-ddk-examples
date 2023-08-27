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

import copy
from typing import Any, Dict

import aws_cdk as cdk
import aws_cdk.aws_iam as iam
import aws_cdk.aws_lambda as lmbda
import aws_cdk.aws_ssm as ssm
from aws_ddk_core import BaseStack, DataPipeline,  S3EventStage, MWAATriggerDagsStage
from constructs import Construct

from ...foundations import FoundationsStack
from ..common_stages import (
    SDLFHeavyTransform,
    SDLFHeavyTransformConfig,
    SDLFLightTransform,
    SDLFLightTransformConfig,
)
from .standard_dataset_stack import StandardDatasetConfig, StandardDatasetStack


def get_ssm_value(scope, id: str, parameter_name: str) -> str:
    return ssm.StringParameter.from_string_parameter_name(
        scope,
        id=id,
        string_parameter_name=parameter_name,
    ).string_value


class StandardPipeline(BaseStack):
    PIPELINE_TYPE: str = "standard"

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        environment_id: str,
        resource_prefix: str,
        team: str,
        mwaa_env: Any,
        orchestration: str,
        foundations_stage: FoundationsStack,
        wrangler_layer: lmbda.ILayerVersion,
        app: str,
        org: str,
        runtime: lmbda.Runtime,
        **kwargs: Any,
    ) -> None:
        self._team = team
        self._orchestration = orchestration
        self._mwaa_env = mwaa_env
        self._environment_id = environment_id
        self._resource_prefix = resource_prefix
        super().__init__(
            scope,
            construct_id,
            environment_id=environment_id,
            stack_name=f"{self._resource_prefix}-StandardPipeline-{self._team}-{environment_id}",
            **kwargs,
        )
        self._pipeline_id = f"{self._resource_prefix}-{self._team}-{self.PIPELINE_TYPE}"
        self._wrangler_layer = wrangler_layer
        self._foundations_stage = foundations_stage
        self._data_lake_library_layer_arn = get_ssm_value(
            self,
            "data-lake-library-layer-arn-ssm",
            parameter_name="/SDLF/Layer/DataLakeLibrary",
        )

        self._data_lake_library_layer = lmbda.LayerVersion.from_layer_version_arn(
            self,
            "data-lake-library-layer",
            layer_version_arn=self._data_lake_library_layer_arn,
        )
        self._app = app
        self._org = org
        self._runtime = runtime

        self._create_sdlf_pipeline()

    def _create_sdlf_pipeline(self):
        # routing function
        routing_function = self._create_routing_lambda()

        # S3 Event Capture Stage
        self._s3_event_capture_stage = S3EventStage(
            self,
            id=f"{self._pipeline_id}-s3-event-capture",
            event_names=["Object Created"],
            bucket=self._foundations_stage.raw_bucket,
        )

        self._data_lake_light_transform = SDLFLightTransform(
            self,
            construct_id=f"{self._pipeline_id}-stage-a",
            name=f"{self._resource_prefix}-SDLFLightTransform-{self._team}-{self.PIPELINE_TYPE}-{self._environment_id}",
            prefix=self._resource_prefix,
            environment_id=self._environment_id,
            config=SDLFLightTransformConfig(
                team=self._team,
                orchestration=self._orchestration,
                pipeline=self.PIPELINE_TYPE,
                raw_bucket=self._foundations_stage.raw_bucket,
                raw_bucket_key=self._foundations_stage.raw_bucket_key,
                stage_bucket=self._foundations_stage.stage_bucket,
                stage_bucket_key=self._foundations_stage.stage_bucket_key,
                routing_lambda=routing_function,
                data_lake_lib=self._data_lake_library_layer,
                register_provider=self._foundations_stage.register_provider,
                wrangler_layer=self._wrangler_layer,
                runtime=self._runtime,
            ),
            props={
                "version": 1,
                "status": "ACTIVE",
                "name": f"{self._team}-{self.PIPELINE_TYPE}-stage-a",
                "type": "octagon_pipeline",
                "description": f"{self._resource_prefix} data lake light transform",
                "id": f"{self._team}-{self.PIPELINE_TYPE}-stage-a",
            },
            description=f"{self._resource_prefix} data lake light transform",
        )

        data_lake_heavy_transform = SDLFHeavyTransform(
            self,
            construct_id=f"{self._pipeline_id}-stage-b",
            name=f"{self._resource_prefix}-SDLFHeavyTransform-{self._team}-{self.PIPELINE_TYPE}-{self._environment_id}",
            prefix=self._resource_prefix,
            environment_id=self._environment_id,
            config=SDLFHeavyTransformConfig(
                team=self._team,
                orchestation=self._orchestration,
                pipeline=self.PIPELINE_TYPE,
                stage_bucket=self._foundations_stage.stage_bucket,
                stage_bucket_key=self._foundations_stage.stage_bucket_key,
                data_lake_lib=self._data_lake_library_layer,
                register_provider=self._foundations_stage.register_provider,
                wrangler_layer=self._wrangler_layer,
                runtime=self._runtime,
            ),
            props={
                "version": 1,
                "status": "ACTIVE",
                "name": f"{self._team}-{self.PIPELINE_TYPE}-stage-b",
                "type": "octagon_pipeline",
                "description": f"{self._resource_prefix} data lake heavy transform",
                "id": f"{self._team}-{self.PIPELINE_TYPE}-stage-b",
            },
            description=f"{self._resource_prefix} data lake heavy transform",
        )
        self.routing_b = data_lake_heavy_transform.routing_lambda

        self._data_lake_pipeline: DataPipeline = (
            DataPipeline(
                self,
                id=self._pipeline_id,
                name=f"{self._resource_prefix}-DataPipeline-{self._team}-{self.PIPELINE_TYPE}-{self._environment_id}",
                description=f"{self._resource_prefix} data lake pipeline",
            )
            .add_stage(stage=self._s3_event_capture_stage)  # type: ignore
            .add_stage(
                stage=self._data_lake_light_transform, skip_rule=True
            )  # configure rule on register_dataset() call
            .add_stage(stage=data_lake_heavy_transform, skip_rule=True)
        )

        if(self._orchestration == "mwaa"):
            common_mwaa_trigger_dags_stage = MWAATriggerDagsStage(
                self,
                id=f"{self._resource_prefix}-{self._team}-{self.PIPELINE_TYPE}-trigger-dag-state-machine",
                mwaa_environment_name=self._mwaa_env.name,
                state_machine_name=f"{self._resource_prefix}-{self._team}-{self.PIPELINE_TYPE}-trigger-dag-state-machine",
                dag_path="$.dag_ids",
            )

            self._state_machine = common_mwaa_trigger_dags_stage.state_machine

            ssm.StringParameter(
                self,
                f"{self._resource_prefix}-{self._team}-{self.PIPELINE_TYPE}-state-machine-a-ssm",
                parameter_name=f"/SDLF/SM/{self._team}/{self.PIPELINE_TYPE}StageASM",
                string_value=self._state_machine.state_machine_arn,
            )

            ssm.StringParameter(
                self,
                f"{self._resource_prefix}-{self._team}-{self.PIPELINE_TYPE}-state-machine-b-ssm",
                parameter_name=f"/SDLF/SM/{self._team}/{self.PIPELINE_TYPE}StageBSM",
                string_value=self._state_machine.state_machine_arn,
            )

            self._data_lake_pipeline.add_stage(stage=common_mwaa_trigger_dags_stage, skip_rule=True)


        return data_lake_heavy_transform

    def _create_routing_lambda(self) -> lmbda.IFunction:
        # Lambda
        routing_function: lmbda.IFunction = lmbda.Function(
            self,
            id=f"{self._resource_prefix}-{self._team}-{self.PIPELINE_TYPE}-pipeline-routing-function",
            function_name=f"{self._resource_prefix}-{self._team}-{self.PIPELINE_TYPE}-pipeline-routing",
            code=lmbda.Code.from_asset("data_lake/src/lambdas/routing"),
            handler="handler.lambda_handler",
            description="routes to the right team and pipeline",
            timeout=cdk.Duration.seconds(60),
            memory_size=256,
            runtime=self._runtime,
            environment={
                "ENV": self._environment_id,
                "APP": self._app,
                "ORG": self._org,
                "PREFIX": self._resource_prefix,
            },
        )
        self._foundations_stage.object_metadata.grant_read_write_data(routing_function)
        self._foundations_stage.datasets.grant_read_write_data(routing_function)
        routing_function.add_to_role_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "kms:CreateGrant",
                    "kms:Decrypt",
                    "kms:DescribeKey",
                    "kms:Encrypt",
                    "kms:GenerateDataKey",
                    "kms:GenerateDataKeyPair",
                    "kms:GenerateDataKeyPairWithoutPlaintext",
                    "kms:GenerateDataKeyWithoutPlaintext",
                    "kms:ReEncryptTo",
                    "kms:ReEncryptFrom",
                    "kms:ListAliases",
                    "kms:ListGrants",
                    "kms:ListKeys",
                    "kms:ListKeyPolicies",
                ],
                resources=["*"],
                conditions={
                    "ForAnyValue:StringLike": {"kms:ResourceAliases": "alias/*"}
                },
            )
        )
        routing_function.add_to_role_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "sqs:SendMessage",
                    "sqs:DeleteMessage",
                    "sqs:ReceiveMessage",
                    "sqs:GetQueueAttributes",
                    "sqs:ListQueues",
                    "sqs:GetQueueUrl",
                    "sqs:ListDeadLetterSourceQueues",
                    "sqs:ListQueueTags",
                ],
                resources=[
                    f"arn:aws:sqs:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:{self._resource_prefix}-*"
                ],
            )
        )
        routing_function.add_to_role_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["ssm:GetParameter", "ssm:GetParameters"],
                resources=[
                    f"arn:aws:ssm:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:parameter/SDLF/*"
                ],
            )
        )

        routing_function.add_permission(
            id="invoke-lambda-eventbridge",
            principal=iam.ServicePrincipal("events.amazonaws.com"),
            action="lambda:InvokeFunction",
        )

        return routing_function

    @property
    def routing_b(self) -> lmbda.IFunction:
        return self._routing_b

    @routing_b.setter
    def routing_b(self, routing_lambda: lmbda.IFunction):
        self._routing_b = routing_lambda

    def register_dataset(self, dataset: str, config: Dict[str, Any]):
        # Create dataset stack
        stage_a_transform = config.get("stage_a_transform", "sdlf_light_transform")
        stage_b_transform = config.get("stage_b_transform", "sdlf_heavy_transform")
        app = self._app
        org = self._org

        StandardDatasetStack(
            self,
            construct_id=f"{self._team}-{self.PIPELINE_TYPE}-{dataset}-dataset-stage",
            environment_id=self._environment_id,
            resource_prefix=self._resource_prefix,
            config=StandardDatasetConfig(
                team=self._team,
                dataset=dataset,
                pipeline=self.PIPELINE_TYPE,
                app=app,
                org=org,
                routing_b=self.routing_b,
                stage_a_transform=stage_a_transform,
                stage_b_transform=stage_b_transform,
                artifacts_bucket=self._foundations_stage.artifacts_bucket,
                artifacts_bucket_key=self._foundations_stage.artifacts_bucket_key,
                stage_bucket=self._foundations_stage.stage_bucket,
                stage_bucket_key=self._foundations_stage.stage_bucket_key,
                glue_role=self._foundations_stage.glue_role,
                register_provider=self._foundations_stage.register_provider,
            ),
        )

        # Add S3 object created event pattern
        base_event_pattern = copy.deepcopy(self._s3_event_capture_stage.event_pattern)
        base_event_pattern.detail["object"] = {  # type: ignore
            "key": [{"prefix": f"{self._team}/{dataset}/"}]
        }

        self._data_lake_pipeline.add_rule(
            id=f"{self._pipeline_id}-{dataset}-rule",
            event_pattern=base_event_pattern,
            event_targets=self._data_lake_light_transform.targets,
        )
