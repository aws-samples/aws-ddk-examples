#!/usr/bin/env python3
# Copyright (c) 2021 Amazon.com, Inc. or its affiliates
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT


from typing import Any

import aws_cdk as cdk
from aws_ddk_core import CICDPipelineStack, Configurator

from data_processing_pipeline.compute import DataProcessingPipeline
from data_processing_pipeline.storage import DataStorage


class DataStorageStage(cdk.Stage):
    def __init__(
        self,
        scope,
        environment_id: str,
        mode: str,
        compute_params: dict,
        **kwargs: Any,
    ) -> None:
        super().__init__(scope, f"{environment_id}", **kwargs)

        DataStorage(
            self,
            "ddk-storage-stage",
            environment_id=environment_id,
            mode=mode,
            compute_params=compute_params,
        )


class DataComputeStage(cdk.Stage):
    def __init__(
        self,
        scope,
        environment_id: str,
        mode: str,
        storage_params: dict,
        **kwargs: Any,
    ) -> None:
        super().__init__(scope, f"{environment_id}", **kwargs)

        DataProcessingPipeline(
            self,
            "ddk-compute-stage",
            environment_id=environment_id,
            mode=mode,
            storage_params=storage_params,
        )


# This is the code that creates the pipeline.

app = cdk.App()

stor_config = Configurator(app, "./ddk.json", "stor")
comp_config = Configurator(app, "./ddk.json", "comp")
cicd_config = Configurator(app, "./ddk.json", "cicd")

mode = "same_account_region"
if stor_config.get_config_attribute("account") != comp_config.get_config_attribute(
    "account"
):
    mode = "cross_account"
elif stor_config.get_config_attribute("region") != comp_config.get_config_attribute(
    "region"
):
    mode = "cross_region"

########CICD###########
pipeline_name = f"cross-account-region-pipeline"
repository_name = "cross-account-region-repo"
pipeline = CICDPipelineStack(
    app,
    id=pipeline_name,
    environment_id="cicd",
    pipeline_name=pipeline_name,
    cdk_language="python",
    env=cdk.Environment(
        account=cicd_config.get_config_attribute("account"),
        region=cicd_config.get_config_attribute("region"),
    ),
)

pipeline.add_source_action(repository_name=repository_name)
pipeline.add_synth_action()
pipeline.build_pipeline()
pipeline.add_stage(
    stage_id="dev-storage",
    stage=DataStorageStage(
        app,
        environment_id="stor",
        mode=mode,
        compute_params=Configurator.get_env_config(
            config_path="./ddk.json", environment_id="comp"
        ),
        env=cdk.Environment(
            account=stor_config.get_config_attribute("account"),
            region=stor_config.get_config_attribute("region"),
        ),
    ),
)
pipeline.add_stage(
    stage_id="dev-compute",
    stage=DataComputeStage(
        app,
        environment_id="comp",
        mode=mode,
        storage_params=Configurator.get_env_config(
            config_path="./ddk.json", environment_id="stor"
        ),
        env=cdk.Environment(
            account=comp_config.get_config_attribute("account"),
            region=comp_config.get_config_attribute("region"),
        ),
    ),
)
#######################

app.synth()
