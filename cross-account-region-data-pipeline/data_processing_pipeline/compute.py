from typing import Any
from aws_ddk_core.base import BaseStack
from aws_ddk_core.stages import S3EventStage, SqsToLambdaStage
from aws_ddk_core.pipelines.pipeline import DataPipeline 
from constructs import Construct
from aws_cdk.aws_events import CfnEventBusPolicy
from aws_cdk.aws_lambda import Code
import os
from pathlib import Path
from typing import Any, Dict


class DataProcessingPipeline(BaseStack):

    def __init__(self, scope: Construct, id: str, environment_id: str, mode: str, storage_params: Dict, **kwargs: Any) -> None:
        super().__init__(scope, id, environment_id, **kwargs)
        self._environment_id = environment_id
        self._mode = mode
        self._s3_bucket_name = storage_params.get('s3BucketName')
        self._storage_account = storage_params.get("account")
        self._storage_region = storage_params.get("region")


        data_lake_s3_event_capture_stage = S3EventStage(
            self,
            id=f"s3-event-capture",
            environment_id=self._environment_id,  
            event_names=[
                "Object Created",
                "Object Deleted"
            ],
            bucket_name = self._s3_bucket_name   
        )

        if self._mode == "cross_account":
            CfnEventBusPolicy(
                self, 
                "StorageAcctEventBusPolicy",
                statement_id="AllowStorageAccountPutEvents",
                action="events:PutEvents",
                principal=self._storage_account
            )

        sqs_lambda = SqsToLambdaStage(
            self,
            "sqs-lambda-stage",
            environment_id = self._environment_id,
            code=Code.from_asset(os.path.join(f"{Path(__file__).parents[0]}", "lambdas/processing_lambda")),
            handler="handler.lambda_handler"
        )

        self._pipeline: DataPipeline = (
            DataPipeline(
                self, 
                id=f"{self._mode.replace('_','-')}-pipeline-id", 
                name=f"{self._mode.replace('_','-')}-pipeline", 
                description="Data processing pipeline", 
            )
            .add_stage(data_lake_s3_event_capture_stage)
            .add_stage(sqs_lambda)
        )
