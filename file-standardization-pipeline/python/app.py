#!/usr/bin/env python3

import aws_cdk as cdk
from file_standardization_pipeline.file_standardization_pipeline import FileStandardizationPipelineStack

app = cdk.App()
FileStandardizationPipelineStack(app, "FileStandardizationPipelineStack", "dev")

app.synth()
