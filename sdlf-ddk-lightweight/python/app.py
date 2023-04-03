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
from aws_ddk_core import BaseStack, CICDPipelineStack, Configurator
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
        super().__init__(scope, f"sdlf-ddk-{environment_id}", **kwargs)

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
        super().__init__(scope, id, environment_id=environment_id, **kwargs)

        self._sdlf_params = pipeline_params.get("data_pipeline_parameters", {})

        SDLFBaseStack(
            self,
            f"{self._resource_prefix}-data-lake-pipeline",
            environment_id=environment_id,
            resource_prefix=self._resource_prefix,
            params=self._sdlf_params,
        )


satellite_app = cdk.App()
PIPELINE_NAME = "sdlf-ddk-pipeline"
dev_config = Configurator.get_env_config(config_path="./ddk.json", environment_id="dev")
cicd_config = Configurator.get_env_config(
    config_path="./ddk.json", environment_id="cicd"
)
cicd_repository_name = cicd_config.get("repository", "sdlf-ddk-example")

cicd_enabled = cicd_config.get("cicd_enabled", False)

if cicd_enabled:
    pipeline = CICDPipelineStack(
        satellite_app,
        id=PIPELINE_NAME,
        environment_id="cicd",
        pipeline_name=PIPELINE_NAME,
        env=cdk.Environment(
            account=cicd_config.get("account"), region=cicd_config.get("region")
        ),
    )
    pipeline.add_source_action(repository_name=cicd_repository_name)
    pipeline.add_synth_action()
    pipeline.build_pipeline(publish_assets_in_parallel=True)  # type:ignore
    pipeline.add_checks()
    pipeline.add_stage(
        stage_id="dev",
        stage=DataLakeFrameworkCICD(
            satellite_app,
            environment_id="dev",
            pipeline_params=dev_config,
            env=cdk.Environment(
                account=dev_config.get("account"), region=dev_config.get("region")
            ),
        ),
    )
    pipeline.synth()
else:
    DataLakeFramework(
        satellite_app,
        id=f"sdlf-ddk-dev",
        environment_id="dev",
        pipeline_params=dev_config,
        env=cdk.Environment(
            account=dev_config.get("account"), region=dev_config.get("region")
        ),
    )


satellite_app.synth()
