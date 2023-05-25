#!/usr/bin/env python3

import aws_cdk as cdk
from data_validation_cataloging_pipeline.data_validation_cataloging import DataValidationCatalogingStack

app = cdk.App()

DataValidationCatalogingStack(app, id="DataValidationCatalogingPipeline", environment_id="dev")

app.synth()
