from typing import Any

import aws_cdk.aws_s3 as s3
from aws_cdk.aws_lambda import Code, LayerVersion, Runtime
from aws_ddk_core import BaseStack, DataPipeline, FirehoseToS3Stage, SqsToLambdaStage
from constructs import Construct


class DdkApplicationStack(BaseStack):
    def __init__(
        self, scope: Construct, id: str, environment_id: str, **kwargs: Any
    ) -> None:
        super().__init__(scope, id, environment_id=environment_id, **kwargs)

        # The code that defines your stack goes here. For example:
        ddk_bucket = s3.Bucket(
            self,
            "ddk-bucket",
            event_bridge_enabled=True,
        )

        firehose_s3_stage = FirehoseToS3Stage(
            self,
            "ddk-firehose-s3",
            s3_bucket=ddk_bucket,
            data_output_prefix="raw/",
        )
        sqs_lambda_stage = SqsToLambdaStage(
            scope=self,
            id="ddk-sqs-lambda",
            lambda_function_props={
                "code": Code.from_asset("./lambda"),
                "handler": "index.lambda_handler",
                "layers": [
                    LayerVersion.from_layer_version_arn(
                        self,
                        "ddk-lambda-layer-wrangler",
                        f"arn:aws:lambda:{self.region}:336392948345:layer:AWSDataWrangler-Python39:2",
                    )
                ],
                "runtime": Runtime.PYTHON_3_9,
            },
        )
        ddk_bucket.grant_read_write(sqs_lambda_stage.function)

        (
            DataPipeline(self, id="ddk-pipeline")
            .add_stage(stage = firehose_s3_stage)
            .add_stage(stage = sqs_lambda_stage)
        )
