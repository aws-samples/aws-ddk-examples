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


from typing import Any
from aws_ddk_core.stages import S3EventStage 
import aws_cdk as cdk
from aws_ddk_core.pipelines import DataPipeline  
from aws_ddk_core.base import BaseStack
import aws_cdk.aws_events as events
import aws_cdk.aws_events_targets as targets
import aws_cdk.aws_lambda as lmbda

import json
import os
from pathlib import Path

from ..stages import (
    SDLFLightTransform,
    SDLFLightTransformConfig,
    SDLFHeavyTransform,
    SDLFHeavyTransformConfig,
    FoundationsStage,
    SDLFDatasetStage,
    SDLFDatasetConfig
)


class SDLFPipelineStack(BaseStack):
    def __init__(self, scope, construct_id: str, environment_id: str, resource_prefix: str, params: dict, **kwargs: Any) -> None:
        super().__init__(scope, construct_id, environment_id, **kwargs)
        self._environment_id: str = environment_id
        self._params: dict = params
        self._resource_prefix = resource_prefix
        self._app=self._params.get("app", "datalake")
        self._org=self._params.get("org", "aws")

        path  = os.path.join(f"{Path(__file__).parents[0]}", "parameters.json")
        with open(path) as f:
            customer_configs = json.load(f).get(self._environment_id)

        self._wrangler_layer = self._create_wrangler_layer()

        self._foundations_stage = FoundationsStage(
            self, 
            id="foundation-stage", 
            environment_id=self._environment_id, 
            resource_prefix=self._resource_prefix, 
            app=self._app, 
            org=self._org,
            runtime=lmbda.Runtime.PYTHON_3_9
        )

        dataset_names = []
        pipeline_names = []
        for customer_config in customer_configs:
            dataset = customer_config.get("dataset", "legislators")
            team = customer_config.get("team", "demoteam")
            pipeline = customer_config.get("pipeline", "gsp")

            #PIPELINE CREATION
            if f"{team}-{pipeline}" not in pipeline_names:
                pipeline_names.append(f"{team}-{pipeline}")
                if pipeline != "gsp":
                    #HANDLE NEW PIPELINE DEFINITION
                    self._create_custom_pipeline()
                else:
                    self._create_sdlf_pipeline(self._resource_prefix, team, pipeline, dataset)



            #DATASET CREATION
            if f"{team}-{dataset}" not in dataset_names:
                dataset_names.append(f"{team}-{dataset}")
                stage_a_transform = customer_config.get("stage_a_transform", "sdlf_light_transform")
                stage_b_transform = customer_config.get("stage_b_transform", "sdlf_heavy_transform")
                
                # Init Dataset Stage and Set Getters
                SDLFDatasetStage(
                    self, 
                    id=f"{team}-{dataset}-dataset-stage", 
                    environment_id=self._environment_id, 
                    resource_prefix=self._resource_prefix, 
                    config=SDLFDatasetConfig(
                        team = team,
                        dataset = dataset,
                        pipeline = pipeline,
                        stage_a_transform = stage_a_transform,
                        stage_b_transform = stage_b_transform,
                        artifacts_bucket = self._foundations_stage.artifacts_bucket,
                        artifacts_bucket_key = self._foundations_stage.artifacts_bucket_key,
                        stage_bucket=self._foundations_stage.stage_bucket,
                        stage_bucket_key=self._foundations_stage.stage_bucket_key,
                        glue_role = self._foundations_stage.glue_role,
                        register_provider = self._foundations_stage.register_provider
                    )
                )
                
            events.Rule(
                self,
                f"{resource_prefix}-{team}-{pipeline}-{dataset}-schedule-rule",
                schedule=events.Schedule.rate(cdk.Duration.minutes(5)),
                targets=[targets.LambdaFunction(
                    self.routing_b,
                    event=events.RuleTargetInput.from_object({
                        "team": team,
                        "pipeline": pipeline,
                        "pipeline_stage": "StageB",
                        "dataset": dataset,
                        "org": self._org,
                        "app": self._app,
                        "env": self._environment_id,
                        "crawler_name": f"{resource_prefix}-{team}-{dataset}-post-stage-crawler"
                    })
                    )
                ] 
            )


    def _create_sdlf_pipeline(self, resource_prefix, team, pipeline, dataset):
        # This is the Pipeline Stages ID Passed to S3 Event Capture, Light Transform and Heavy Transform Stage
        pipeline_id: str = f"{resource_prefix}-{team}-{pipeline}"

        # This is the Pipeline ID used to define a new Data Pipeline for Any New Team, Pipeline, or Dataset
        logical_data_pipeline_id = f"{resource_prefix}-{team}-{pipeline}-{dataset}"

        # S3 Event Capture Stage
        data_lake_s3_event_capture_stage = S3EventStage(
            self,
            id=f"{pipeline_id}-s3-event-capture",
            environment_id=self._environment_id,  
            event_names=[
                "Object Created"
            ],
            bucket_name = self._foundations_stage.raw_bucket.bucket_name,
            key_prefix = f"{team}/",    
        )

        data_lake_light_transform = SDLFLightTransform(
            self,
            id=f"{pipeline_id}-stage-a",
            name=f"{team}-{pipeline}-stage-a",
            prefix = resource_prefix,
            description=f"{resource_prefix} data lake light transform",
            props={
                "version": 1,
                "status": "ACTIVE",
                "name": f"{team}-{pipeline}-stage-a",
                "type": "octagon_pipeline",
                "description": f"{resource_prefix} data lake light transform",
                "id": f"{team}-{pipeline}-stage-a"
            },
            environment_id=self._environment_id,
            config=SDLFLightTransformConfig(
                team=team,
                pipeline=pipeline,
                raw_bucket = self._foundations_stage.raw_bucket,
                raw_bucket_key = self._foundations_stage.raw_bucket_key,
                stage_bucket = self._foundations_stage.stage_bucket, 
                stage_bucket_key = self._foundations_stage.stage_bucket_key,
                routing_lambda = self._foundations_stage.routing_function, 
                data_lake_lib = self._foundations_stage.data_lake_library,
                register_provider = self._foundations_stage.register_provider,
                wrangler_layer = self._wrangler_layer,
                runtime = lmbda.Runtime.PYTHON_3_9
            ),
        )
        
        data_lake_heavy_transform = SDLFHeavyTransform(
            self,
            id=f"{pipeline_id}-stage-b",
            name=f"{team}-{pipeline}-stage-b",
            prefix = resource_prefix,
            description=f"{resource_prefix} data lake heavy transform",
            props={
                "version": 1,
                "status": "ACTIVE",
                "name": f"{team}-{pipeline}-stage-b",
                "type": "octagon_pipeline",
                "description": f"{resource_prefix} data lake heavy transform",
                "id": f"{team}-{pipeline}-stage-b"
            },
            environment_id=self._environment_id,
            config=SDLFHeavyTransformConfig(
                team=team,
                pipeline=pipeline,
                stage_bucket = self._foundations_stage.stage_bucket, 
                stage_bucket_key = self._foundations_stage.stage_bucket_key,
                data_lake_lib = self._foundations_stage.data_lake_library,
                register_provider = self._foundations_stage.register_provider,
                wrangler_layer = self._wrangler_layer,
                database_crawler_name = f"{resource_prefix}-{team}-{dataset}-post-stage-crawler",
                runtime = lmbda.Runtime.PYTHON_3_9
            ),
        )
        self.routing_b = data_lake_heavy_transform.routing_lambda
    
        self._data_lake_pipeline: DataPipeline = (
            DataPipeline(
                self, 
                id=logical_data_pipeline_id,
                name=f"{logical_data_pipeline_id}-pipeline",
                description=f"{self._resource_prefix} data lake pipeline", 
            )
            .add_stage(data_lake_s3_event_capture_stage)
            .add_stage(data_lake_light_transform)
            .add_stage(data_lake_heavy_transform, skip_rule=True)
        )
        return data_lake_heavy_transform

    def _create_custom_pipeline(self):
        return

    def _create_wrangler_layer(self):
        wrangler_layer_version = lmbda.LayerVersion.from_layer_version_arn(
            self,
            "wrangler-layer",
            layer_version_arn=f"arn:aws:lambda:{self.region}:336392948345:layer:AWSDataWrangler-Python39:1",
        )

        return wrangler_layer_version

    @property
    def routing_b(self):
        return self._routing_b

    @routing_b.setter
    def routing_b(self, routing_lambda):
        self._routing_b = routing_lambda
