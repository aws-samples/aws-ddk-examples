#!/usr/bin/env python3

import aws_cdk as cdk

from athena_views_pipeline.athena_views_pipeline import AthenaViewsPipeline

app = cdk.App()
AthenaViewsPipeline(app, "AthenaViewsStack", "dev")

app.synth()
