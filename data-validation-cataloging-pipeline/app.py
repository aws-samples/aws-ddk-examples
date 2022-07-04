#!/usr/bin/env python3

import aws_cdk as cdk
from aws_ddk_core.config import Config
from data_validation_cataloging_pipeline.data_validation_cataloging import DataValidationCatalogingStack

app = cdk.App()
config = Config()

DataValidationCatalogingStack(app, id="DataValidationCatalogingPipeline", environment_id="dev", env=config.get_env("dev"))

app.synth()
