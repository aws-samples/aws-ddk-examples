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


from typing import Any, Dict

import aws_cdk as cdk
from aws_ddk_core.base import BaseStack
from aws_ddk_core.cicd import CICDPipelineStack
from aws_ddk_core.config import Config
from constructs import Construct

from data_lake.pipelines import SDLFBaseStack


class DataLakeFrameworkCICD(cdk.Stage):  # For CICD Deployments
    def __init__(
        self,
        scope: Construct,
        pipeline_params: Dict[str, Any],
        environment_id: str,
        **kwargs: Any,
    ) -> None:
        self._environment_id = environment_id
        self._resource_prefix = pipeline_params.get("resource_prefix", "ddk")
        super().__init__(scope, f"SDLF-DDK-{environment_id}", **kwargs)

        self._sdlf_params = pipeline_params.get("data_pipeline_parameters", {})

        SDLFBaseStack(
            self,
            f"{self._resource_prefix}-data-lake-pipeline",
            environment_id=environment_id,
            resource_prefix=self._resource_prefix,
            params=self._sdlf_params,
        )


class DataLakeFramework(BaseStack):  # For NO CICD deployments
    def __init__(
        self,
        scope: Construct,
        id: str,
        pipeline_params: Dict[str, Any],
        environment_id: str,
        **kwargs: Any,
    ) -> None:
        self._environment_id = environment_id
        self._resource_prefix = pipeline_params.get("resource_prefix", "ddk")
        super().__init__(scope, id, environment_id, **kwargs)

        self._sdlf_params = pipeline_params.get("data_pipeline_parameters", {})

        SDLFBaseStack(
            self,
            f"{self._resource_prefix}-data-lake-pipeline",
            environment_id=environment_id,
            resource_prefix=self._resource_prefix,
            params=self._sdlf_params,
        )


satellite_app = cdk.App()
config = Config()
PIPELINE_NAME = "sdlf-ddk-pipeline"
cicd_repository_name = config.get_env_config("cicd").get(
    "repository", "sdlf-ddk-example"
)

cicd_enabled = config.get_env_config("cicd").get("cicd_enabled", False)

if cicd_enabled:
    pipeline = CICDPipelineStack(
        satellite_app,
        id=PIPELINE_NAME,
        environment_id="cicd",
        pipeline_name=PIPELINE_NAME,
        pipeline_args={"publish_assets_in_parallel": True},
    )
    pipeline.add_source_action(repository_name=cicd_repository_name)
    pipeline.add_synth_action()
    pipeline.build()  # type:ignore
    pipeline.add_checks()
    pipeline.add_stage(
        "dev",
        DataLakeFrameworkCICD(
            satellite_app,
            environment_id="dev",
            pipeline_params=config.get_env_config("dev"),
            env=config.get_env("dev"),
        ),
    )
    pipeline.synth()
else:
    DataLakeFramework(
        satellite_app,
        id=f"sdlf-ddk-dev",
        environment_id="dev",
        pipeline_params=config.get_env_config("dev"),
        env=config.get_env("dev"),
    )


satellite_app.synth()
