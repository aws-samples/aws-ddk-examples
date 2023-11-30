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
import aws_cdk.aws_iam as iam
from aws_ddk_core import BaseStack, CICDPipelineStack, Configurator
from constructs import Construct

from data_lake.pipelines import SDLFBaseStack


class DataLakeFrameworkCICD(cdk.Stage):  # For CICD Deployments
    def __init__(
        self,
        scope: Construct,
        pipeline_params: Configurator,
        environment_id: str,
        **kwargs: Any,
    ) -> None:
        self._environment_id = environment_id
        self._resource_prefix = (
            pipeline_params.get_config_attribute("resource_prefix")
            if pipeline_params.get_config_attribute("resource_prefix")
            else "ddk"
        )
        super().__init__(scope, f"sdlf-ddk-{environment_id}", **kwargs)

        self._sdlf_params = (
            pipeline_params.get_config_attribute("data_pipeline_parameters")
            if pipeline_params.get_config_attribute("data_pipeline_parameters")
            else {}
        )

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
        pipeline_params: Configurator,
        environment_id: str,
        **kwargs: Any,
    ) -> None:
        self._environment_id = environment_id
        self._resource_prefix = (
            pipeline_params.get_config_attribute("resource_prefix")
            if pipeline_params.get_config_attribute("resource_prefix")
            else "ddk"
        )
        super().__init__(scope, id, environment_id=environment_id, **kwargs)

        self._sdlf_params = (
            pipeline_params.get_config_attribute("data_pipeline_parameters")
            if pipeline_params.get_config_attribute("data_pipeline_parameters")
            else {}
        )

        SDLFBaseStack(
            self,
            f"{self._resource_prefix}-data-lake-pipeline",
            environment_id=environment_id,
            resource_prefix=self._resource_prefix,
            params=self._sdlf_params,
        )


satellite_app = cdk.App()
PIPELINE_NAME = "sdlf-ddk-pipeline"
dev_config = Configurator(satellite_app, "./ddk.json", "dev")
cicd_config = Configurator(satellite_app, "./ddk.json", "cicd")
cicd_repository_name = (
    cicd_config.get_config_attribute("repository")
    if cicd_config.get_config_attribute("repository")
    else "sdlf-ddk-example"
)

cicd_enabled = (
    cicd_config.get_config_attribute("cicd_enabled")
    if cicd_config.get_config_attribute("cicd_enabled")
    else False
)

if cicd_enabled:
    pipeline = CICDPipelineStack(
        satellite_app,
        id=PIPELINE_NAME,
        environment_id="cicd",
        pipeline_name=PIPELINE_NAME,
        cdk_language="python",
        env=cdk.Environment(
            account=cicd_config.get_config_attribute("account"),
            region=cicd_config.get_config_attribute("region"),
        ),
    )
    pipeline.add_source_action(repository_name=cicd_repository_name)    
    pipeline.add_synth_action(
            role_policy_statements=[
                iam.PolicyStatement.from_json(
                    {"Action": "ec2:DescribeAvailabilityZones", "Resource": "*"}
                )
            ]
        )
    pipeline.build_pipeline(publish_assets_in_parallel=True)  # type:ignore
    pipeline.add_security_lint_stage()
    pipeline.add_stage(
        stage_id="dev",
        stage=DataLakeFrameworkCICD(
            satellite_app,
            environment_id="dev",
            pipeline_params=dev_config,
            env=cdk.Environment(
                account=dev_config.get_config_attribute("account"),
                region=dev_config.get_config_attribute("region"),
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
            account=dev_config.get_config_attribute("account"),
            region=dev_config.get_config_attribute("region"),
        ),
    )


satellite_app.synth()
