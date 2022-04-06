#!/usr/bin/env python3

import aws_cdk as cdk
from ddk_app.ddk_app_stack import DdkApplicationStack
from ddk_app.flow_stack import FlowStack

app = cdk.App()
FlowStack(app, "FlowStack", "dev")
DdkApplicationStack(app, "DdkApplicationStack", "dev")

app.synth()
