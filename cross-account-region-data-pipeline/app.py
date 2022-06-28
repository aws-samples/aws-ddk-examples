#!/usr/bin/env python3
# Copyright (c) 2021 Amazon.com, Inc. or its affiliates
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT


import aws_cdk as cdk
from typing import Any

from aws_ddk_core.config import Config
from aws_ddk_core.cicd import CICDPipelineStack
from utils.application_config import GetApplicationParameters
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

        # storage_params = GetApplicationParameters(environment_id=environment_id)
        # compute_params = GetApplicationParameters(environment_id="comp")
        # self._mode = storage_params.get_mode(compute_params)

        DataStorage(self, "ddk-storage-stage", environment_id=environment_id, mode = mode, compute_params = compute_params)

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

        # compute_params = GetApplicationParameters(environment_id=environment_id)
        # storage_params = GetApplicationParameters(environment_id="stor")
        # self._mode = compute_params.get_mode(storage_params)

        DataProcessingPipeline(self, "ddk-compute-stage", environment_id=environment_id, mode = mode, storage_params = storage_params)
        
# This is the code that creates the pipeline.

app = cdk.App()
config = Config()

storage_params = config.get_env_config("stor")
compute_params = config.get_env_config("comp")

mode = "same_account_region"
if storage_params.get("account") != compute_params.get("account"):
    mode = "cross_account"
elif storage_params.get("region") != compute_params.get("region"):
    mode = "cross_region"

########CICD###########
pipeline_name = f"cross-account-region-pipeline"
repository_name="cross-account-region-repo" 
pipeline = CICDPipelineStack(app, id=pipeline_name, environment_id="cicd",  pipeline_name=pipeline_name)

pipeline.add_source_action(repository_name=repository_name)
pipeline.add_synth_action()
pipeline.build()
pipeline.add_stage("dev-storage", DataStorageStage(app, environment_id="stor", mode=mode, compute_params=compute_params, env=config.get_env("stor")))
pipeline.add_stage("dev-compute", DataComputeStage(app, environment_id="comp", mode=mode, storage_params=storage_params, env=config.get_env("comp")))
#######################

app.synth()
