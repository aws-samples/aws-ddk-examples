import os
from pathlib import Path
from typing import Any, Dict

from aws_cdk.aws_events import CfnEventBusPolicy
from aws_cdk.aws_lambda import Code, Runtime
from aws_cdk.aws_s3 import Bucket, IBucket
from aws_ddk_core import (
    BaseStack,
    Configurator,
    DataPipeline,
    S3EventStage,
    SqsToLambdaStage,
)
from constructs import Construct


class DataProcessingPipeline(BaseStack):
    def __init__(
        self,
        scope: Construct,
        id: str,
        environment_id: str,
        mode: str,
        storage_params: Configurator,
        **kwargs: Any,
    ) -> None:
        super().__init__(scope, id, environment_id=environment_id, **kwargs)
        self._environment_id = environment_id
        self._mode = mode
        self._s3_bucket_name = storage_params.get_config_attribute("s3BucketName")
        s3_bucket: IBucket = Bucket.from_bucket_name(
            self, "bucket-name", bucket_name=self._s3_bucket_name
        )
        self._storage_account = storage_params.get_config_attribute("account")

        data_lake_s3_event_capture_stage = S3EventStage(
            self,
            id=f"s3-event-capture",
            event_names=["Object Created", "Object Deleted"],
            bucket=s3_bucket,
        )

        if self._mode == "cross_account":
            CfnEventBusPolicy(
                self,
                "StorageAcctEventBusPolicy",
                statement_id="AllowStorageAccountPutEvents",
                action="events:PutEvents",
                principal=self._storage_account,
            )

        sqs_lambda = SqsToLambdaStage(
            self,
            "sqs-lambda-stage",
            lambda_function_props={
                "code": Code.from_asset(
                    os.path.join(
                        f"{Path(__file__).parents[0]}", "lambdas/processing_lambda"
                    )
                ),
                "handler": "handler.lambda_handler",
                "runtime": Runtime.PYTHON_3_9,
            },
        )

        self._pipeline: DataPipeline = (
            DataPipeline(
                self,
                id=f"{self._mode.replace('_','-')}-pipeline-id",
                name=f"{self._mode.replace('_','-')}-pipeline",
                description="Data processing pipeline",
            )
            .add_stage(stage=data_lake_s3_event_capture_stage)
            .add_stage(stage=sqs_lambda)
        )
