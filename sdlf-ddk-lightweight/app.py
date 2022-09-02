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
import wrapt
import aws_cdk as cdk
from aws_ddk_core.cicd import CICDPipelineStack
from data_lake.pipelines import SDLFPipelineStack
from aws_ddk_core.config import Config
from aws_cdk.pipelines import CodePipeline


@wrapt.patch_function_wrapper(CICDPipelineStack, 'build')
def build(wrapped, self, args, kwargs) -> "CICDPipelineStack":
    """
    This is a temporary workaround to expose the publish_assets_in_parallel parameter
    from CDK into DDK so we can reduce the number of file asset jobs created.

    Returns
    -------
    pipeline : CICDPipelineStack
        CICDPipelineStack
    """

    publish_assets_in_parallel = kwargs['publish_assets_in_parallel'] if 'publish_assets_in_parallel' in kwargs else True
    # Create self-mutating CDK Pipeline
    self._pipeline = CodePipeline(
        self,
        id="code-pipeline",
        cross_account_keys=True,
        synth=self._synth_action,
        cli_version=self._config.get_cdk_version(),
        publish_assets_in_parallel=publish_assets_in_parallel
    )
    return self



class DataLakeFramework(cdk.Stage):
    def __init__(
        self,
        scope,
        pipeline_params: Dict,
        environment_id: str,
        **kwargs: Any,
    ) -> None:
        super().__init__(scope, f"SDLF-DDK-{environment_id}", **kwargs)

        self._environment_id = environment_id
        self._resource_prefix = pipeline_params.get("resource_prefix", "ddk")
        self._sdlf_params = pipeline_params.get("data_pipeline_parameters", {})
       
        SDLFPipelineStack(self, f"{self._resource_prefix}-data-lake-pipeline", environment_id=environment_id, resource_prefix=self._resource_prefix, params=self._sdlf_params)

satellite_app = cdk.App()
config = Config()

cicd_repository_name = config.get_env_config("cicd").get("repository", "sdlf-ddk-example")

pipeline_name = "sdlf-ddk-pipeline"
pipeline = CICDPipelineStack(satellite_app, id=pipeline_name, environment_id="cicd",  pipeline_name=pipeline_name)

pipeline.add_source_action(repository_name=cicd_repository_name)
pipeline.add_synth_action()
pipeline.build(publish_assets_in_parallel=False)
pipeline.add_checks()
pipeline.add_stage("dev", DataLakeFramework(satellite_app, environment_id="dev", pipeline_params=config.get_env_config("dev"), env=config.get_env("dev")))
pipeline.synth()
pipeline.add_notifications()


satellite_app.synth()