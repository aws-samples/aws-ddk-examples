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
import os
from pathlib import Path
from typing import Any, Dict, Protocol
import aws_cdk as cdk
import aws_cdk.aws_iam as iam
import aws_cdk.aws_lambda as lmbda
import aws_cdk.aws_ssm as ssm
from aws_ddk_core import BaseStack
from constructs import Construct

from ..foundations import FoundationsStack
from .custom_pipeline import CustomPipeline
from .standard_pipeline import StandardPipeline


class SDLFPipeline(Protocol):
    PIPELINE_TYPE: str

    def register_dataset(self, dataset: str, config: Dict[str, Any]):
        ...


class SDLFBaseStack(BaseStack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        environment_id: str,
        resource_prefix: str,
        params: Dict[str, Any],
        **kwargs: Any,
    ) -> None:
        self._resource_prefix = resource_prefix
        super().__init__(
            scope,
            construct_id,
            environment_id=environment_id,
            stack_name=f"{self._resource_prefix}-SDLFPipelineStack-{environment_id}",
            **kwargs,
        )
        self._params = params
        self._environment_id = environment_id
        self._app = self._params.get("app", "datalake")
        self._org = self._params.get("org", "aws")

        path = os.path.join(f"{Path(__file__).parents[0]}", "parameters.json")
        with open(path, encoding="utf-8") as params_file:
            customer_configs = json.load(params_file).get(self._environment_id)

        self._wrangler_layer = self._create_wrangler_layer()
        self._create_data_lake_library_layer()

        # creates DDB tables, routing lambda, data lake buckets,
        # pushes Glue scripts to S3, and creates data lake library lamdba layer
        self._foundations_stage = FoundationsStack(
            self,
            construct_id="foundation-stage",
            environment_id=self._environment_id,
            resource_prefix=self._resource_prefix,
            app=self._app,
            org=self._org,
            runtime=lmbda.Runtime.PYTHON_3_9,
        )

        dataset_names: set[str] = set()
        pipelines: Dict[str, SDLFPipeline] = {}
        
        # loop through values in parameters.json and create the necessary resources for each pipeline
        for customer_config in customer_configs:
            dataset = customer_config["dataset"]
            team = customer_config["team"]
            pipeline_type = customer_config.get(
                "pipeline", StandardPipeline.PIPELINE_TYPE
            )
            orchestration = customer_config.get(
                "orchestration", "sfn"
            ).lower()

            # PIPELINE CREATION
            pipeline: SDLFPipeline
            pipeline_name = f"{team}-{pipeline_type}"
            if pipeline_name not in pipelines:
                if pipeline_type == StandardPipeline.PIPELINE_TYPE:
                    pipeline = StandardPipeline(
                        self,
                        construct_id=f"{team}-{pipeline_type}",
                        environment_id=self._environment_id,
                        resource_prefix=self._resource_prefix,
                        team=team,
                        orchestration=orchestration,
                        foundations_stage=self._foundations_stage,
                        wrangler_layer=self._wrangler_layer,
                        app=self._app,
                        org=self._org,
                        runtime=lmbda.Runtime.PYTHON_3_9,
                    )
                elif pipeline_type == CustomPipeline.PIPELINE_TYPE:
                    pipeline = CustomPipeline(
                        self,
                        construct_id=f"{team}-{pipeline_type}",
                        environment_id=self._environment_id,
                        resource_prefix=self._resource_prefix,
                        team=team,
                        foundations_stage=self._foundations_stage,
                        wrangler_layer=self._wrangler_layer,
                        app=self._app,
                        org=self._org,
                        runtime=lmbda.Runtime.PYTHON_3_9,
                    )
                else:
                    raise NotImplementedError(
                        f"Could not find a valid implementation for pipeline type: {pipeline_type}"
                    )
                pipelines[pipeline_name] = pipeline
            else:
                pipeline = pipelines[pipeline_name]

            # Register dataset to pipeline with concrete implementations
            dataset_name = f"{team}-{dataset}"
            if dataset_name not in dataset_names:
                dataset_names.add(dataset_name)
                pipeline.register_dataset(
                    dataset, config=customer_config.get("config", {})
                )
        
    def _create_wrangler_layer(self):
        wrangler_layer_version = lmbda.LayerVersion.from_layer_version_arn(
            self,
            "wrangler-layer",
            layer_version_arn=f"arn:aws:lambda:{self.region}:336392948345:layer:AWSDataWrangler-Python39:1",
        )

        return wrangler_layer_version

    def _create_data_lake_library_layer(self) -> lmbda.LayerVersion:
        data_lake_library_layer = lmbda.LayerVersion(
            self,
            "data-lake-library-layer",
            layer_version_name="data-lake-library",
            code=lmbda.Code.from_asset(
                os.path.join(
                    f"{Path(__file__).parents[1]}", "src/layers/data_lake_library"
                )
            ),
            compatible_runtimes=[lmbda.Runtime.PYTHON_3_9],
            description=f"{self._resource_prefix} Data Lake Library",
            license="Apache-2.0",
        )

        ssm.StringParameter(
            self,
            f"data-lake-library-layer-ssm",
            parameter_name=f"/SDLF/Layer/DataLakeLibrary",
            string_value=data_lake_library_layer.layer_version_arn,
        )
