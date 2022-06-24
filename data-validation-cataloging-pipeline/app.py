#!/usr/bin/env python3

import aws_cdk as cdk
import json
from data_validation_cataloging_pipeline.data_validation_cataloging import DataValidationCatalogingStack

app = cdk.App()

env = "dev"

with open("./ddk.json") as f:
    config_file = json.load(f)

config = config_file.get("environments", {}).get(env, {})
account_id = config.get("account")
region = config.get("region") 

DataValidationCatalogingStack(app, id="DataValidationCatalogingPipeline", environment_id="dev", env= cdk.Environment(account=account_id, region=region))

app.synth()
