#!/usr/bin/env python3

import aws_cdk as cdk

from ddk_app.databrew_athena_stack import DataBrewAthenaStack

app = cdk.App()
DataBrewAthenaStack(app, "DataBrewAthenaStack", "dev")

app.synth()
