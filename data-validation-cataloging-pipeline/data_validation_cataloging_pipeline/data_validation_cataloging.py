from typing import Any, Dict
import os
from pathlib import Path
from aws_cdk.aws_iam import Effect, PolicyStatement, ServicePrincipal
from aws_cdk.aws_s3 import Bucket, BucketAccessControl
from aws_cdk.aws_lambda import Code
from aws_ddk_core.base import BaseStack
from aws_ddk_core.pipelines import DataPipeline
from aws_ddk_core.resources import S3Factory, KMSFactory
import aws_cdk.aws_dynamodb as DDB
import aws_cdk as cdk
from aws_cdk.aws_ssm import StringParameter
from aws_ddk_core.stages import (
    S3EventStage, SqsToLambdaStage
)
from data_validation_cataloging_pipeline.stages import (DataValidationCatalogingStage)
from constructs import Construct




class DataValidationCatalogingStack(BaseStack):

    def __init__(self, scope: Construct, id: str, environment_id: str, **kwargs: Any) -> None:
        super().__init__(scope, id, environment_id, **kwargs)

        self._environment_id: str = environment_id

        self._didc_table, self._didc_table_key = self._create_ddb_table(
            prefix="di",
            name="didc",
            ddb_props={"partition_key": DDB.Attribute(name="TableName", type=DDB.AttributeType.STRING),
                        "sort_key": DDB.Attribute(name="Source-SchemaName", type=DDB.AttributeType.STRING)},
        )

        self._raw_bucket = self._create_s3_bucket(
            name=f"di-{self._environment_id}-{self.region}-{self.account}-raw",
            environment_id=environment_id,
            eb_enabled=True
        )
               
        s3_event_capture_stage = S3EventStage(
            self,
            id="s3-event-capture",
            environment_id=self._environment_id,  
            event_names=[
                "Object Created"
            ],
            bucket_name = self._raw_bucket.bucket_name,
            key_prefix = "manifests/",    
        )

        sqs_lambda = SqsToLambdaStage(
            self,
            "sqs-lambda-stage",
            environment_id = self._environment_id,
            code=Code.from_asset(os.path.join(f"{Path(__file__).parents[0]}", "src/processing_lambda")),
            handler="handler.lambda_handler"
        )

        step_functions_stage = DataValidationCatalogingStage(
            self,
            id="state-machine-stage",
            environment_id=self._environment_id, 
            raw_bucket_arn = self._raw_bucket.bucket_arn,
            didc_table_arn = self._didc_table.table_arn,
            didc_table_key_arn = self._didc_table_key.key_arn
        )

        sqs_lambda.function.add_environment("STEPFUNCTION", step_functions_stage.state_machine_arn)
        sqs_lambda.function.add_to_role_policy(
            PolicyStatement(
                effect=Effect.ALLOW,
                actions=[
                    "states:*",
                    "s3:*"
                ],
                resources=["*"],
            )
        )

        (
            DataPipeline(
                self, 
                id="ingestion-pipeline", 
                name="ingestion-pipeline", 
                description="ingestion pipeline using aws-ddk", 
            )
            .add_stage(s3_event_capture_stage)
            .add_stage(sqs_lambda)
            .add_stage(step_functions_stage, skip_rule=True)
        )

    def _create_s3_bucket(self, name: str, environment_id: str, eb_enabled: bool) -> Bucket:
        bucket = S3Factory.bucket(
            self,
            id=name,
            bucket_name=name,
            environment_id=environment_id,
            access_control=BucketAccessControl.BUCKET_OWNER_FULL_CONTROL,
            event_bridge_enabled=eb_enabled,
        )
        bucket.add_to_resource_policy(
            PolicyStatement(
                sid="AllowLambdaActions",
                effect=Effect.ALLOW,
                principals=[ServicePrincipal(service="lambda.amazonaws.com")],
                actions=[
                    "s3:Put*",
                    "s3:Get*",
                    "s3:AbortMultipartUpload",
                    "s3:ListMultipartUploadParts",
                    "s3:ListBucketMultipartUploads",
                ],
                resources=[
                    bucket.bucket_arn,
                    f"{bucket.bucket_arn}/*",
                ],
                conditions={
                    "StringEquals": {
                        "aws:SourceAccount": self.account,
                    }
                },
            )
        )
        return bucket

    def _create_ddb_table(self, prefix: str, name: str, ddb_props: Dict[str, Any]) -> DDB.Table:
        table_key = KMSFactory.key(
            self,
            environment_id=self._environment_id,
            id=f"{prefix}-{name}-table-key",
            description=f"{prefix.upper()} {name.title()} Table Key",
            alias=f"{prefix}-{name}-ddb-table-key",
            enable_key_rotation=True,
            pending_window=cdk.Duration.days(30),
            removal_policy=cdk.RemovalPolicy.DESTROY
        )
        StringParameter(
            self,
            f"{prefix}-{name}-table-key-arn-ssm",
            parameter_name=f"/DIF/KMS/{name.title()}DDBTableKeyArn",
            string_value=table_key.key_arn,
        )
        table: DDB.Table = DDB.Table(
            self,
            id=f"{prefix}-{name}-table-{self._environment_id}",
            table_name=f"{prefix}-{name}-{self._environment_id}",
            encryption=DDB.TableEncryption.CUSTOMER_MANAGED,
            encryption_key=table_key,
            removal_policy= cdk.RemovalPolicy.DESTROY,
            **ddb_props,
        )

        return table, table_key
