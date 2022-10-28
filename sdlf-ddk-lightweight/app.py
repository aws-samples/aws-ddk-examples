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


from typing import Any, Tuple

import aws_cdk as cdk
import wrapt
from aws_cdk.pipelines import CodePipeline
from aws_ddk_core.cicd import CICDPipelineStack
from aws_ddk_core.config import Config
from constructs import Construct

from data_lake.pipelines import SDLFBaseStack


@wrapt.patch_function_wrapper(CICDPipelineStack, 'build')  # type: ignore
def build(
    wrapped: Any,  # pylint: disable=unused-argument
    self: CICDPipelineStack,
    args: Tuple[Any, ...],  # pylint: disable=unused-argument
    kwargs: dict[str, Any]
) -> "CICDPipelineStack":
    """
    This is a temporary workaround to expose the publish_assets_in_parallel parameter
    from CDK into DDK so we can reduce the number of file asset jobs created.
    Returns
    -------
    pipeline : CICDPipelineStack
        CICDPipelineStack
    """

    publish_assets_in_parallel: bool = \
        kwargs['publish_assets_in_parallel'] \
        if 'publish_assets_in_parallel' in kwargs \
        else True

    # Create self-mutating CDK Pipeline
    self._pipeline = CodePipeline(  # type: ignore pylint: disable=protected-access
        self,
        id="code-pipeline",
        cross_account_keys=True,
        synth=self._synth_action,  # type: ignore pylint: disable=protected-access
        cli_version=self._config.get_cdk_version(),  # type: ignore pylint: disable=protected-access
        publish_assets_in_parallel=publish_assets_in_parallel
    )
    return self


class DataLakeFramework(cdk.Stage):
    def __init__(
        self,
        scope: Construct,
        pipeline_params: dict[str, Any],
        environment_id: str,
        **kwargs: Any,
    ) -> None:
        self._environment_id = environment_id
        self._resource_prefix = pipeline_params.get("resource_prefix", "ddk")
        super().__init__(
            scope,
            f"SDLF-DDK-{environment_id}",
            **kwargs
        )

        self._sdlf_params = pipeline_params.get("data_pipeline_parameters", {})

        SDLFBaseStack(
            self,
            f"{self._resource_prefix}-data-lake-pipeline",
            environment_id=environment_id,
            resource_prefix=self._resource_prefix,
            params=self._sdlf_params
        )


if __name__ == "__main__":
    satellite_app = cdk.App()
    config = Config()
    PIPELINE_NAME = "sdlf-ddk-pipeline"
    cicd_repository_name = config.get_env_config("cicd").get("repository", "sdlf-ddk-example")

    pipeline = CICDPipelineStack(satellite_app, id=PIPELINE_NAME, environment_id="cicd", pipeline_name=PIPELINE_NAME)
    pipeline.add_source_action(repository_name=cicd_repository_name)
    pipeline.add_synth_action()
    pipeline.build(publish_assets_in_parallel=False)  # type:ignore
    pipeline.add_checks()
    pipeline.add_stage(
            "dev",
            DataLakeFramework(
                satellite_app,
                environment_id="dev",
                pipeline_params=config.get_env_config("dev"),
                env=config.get_env("dev")
            )
        )
    pipeline.synth()

    satellite_app.synth()
