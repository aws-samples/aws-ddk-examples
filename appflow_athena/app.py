#!/usr/bin/env python3

import aws_cdk as cdk
from ddk_app.ddk_app_stack import DdkApplicationStack

app = cdk.App()
DdkApplicationStack(app, "DdkApplicationStack", "dev")

app.synth()
