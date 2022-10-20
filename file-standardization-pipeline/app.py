#!/usr/bin/env python3

import aws_cdk as cdk
from aws_ddk_core.config import Config
from file_standardization_pipeline.file_standardization_pipeline import FileStandardizationPipelineStack

app = cdk.App()
config = Config()
FileStandardizationPipelineStack(app, id="FileStandardizationPipelineStack",environment_id="dev", env=config.get_env("dev"))

app.synth()
