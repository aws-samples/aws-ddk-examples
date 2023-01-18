#!/usr/bin/env python3

import aws_cdk as cdk

from athena_query_execution.athena_query_execution_pipeline import \
    AthenaQueryExecutionPipeline

app = cdk.App()
AthenaQueryExecutionPipeline(app, "AthenaQueryExecutionStack", "dev")

app.synth()
