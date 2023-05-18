from typing import Any

import aws_cdk as cdk
from aws_cdk import Duration
from aws_cdk.aws_appflow import CfnFlow
from aws_cdk.aws_events import EventPattern, Rule, Schedule
from aws_cdk.aws_glue_alpha import Database
from aws_cdk.aws_iam import Effect, PolicyStatement, ServicePrincipal
from aws_cdk.aws_lambda import Code, LayerVersion, Runtime
from aws_cdk.aws_s3 import Bucket, BucketAccessControl, Location
from aws_ddk_core import (
    AppFlowIngestionStage,
    AthenaSQLStage,
    BaseStack,
    Configurator,
    DataPipeline,
    SqsToLambdaStage,
)
from constructs import Construct


class DdkApplicationStack(BaseStack):
    def __init__(
        self, scope: Construct, id: str, environment_id: str, **kwargs: Any
    ) -> None:
        super().__init__(scope, id, environment_id=environment_id, **kwargs)

        Configurator(self, "./ddk.json", environment_id)

        # Create S3 bucket
        bucket = self._create_s3_bucket()

        # Create Database
        database = self._create_database(database_name="appflow_data")

        # Create Google Analytics AppFlow flow
        flow = self._create_ga_s3_ingest_flow(
            connector_profile_name="ga-connection",
            flow_name="ga-flow",
            google_analytics_object="264236892",
            bucket_name=bucket.bucket_name,
            bucket_prefix="ga-data",
        )

        # Create stages
        appflow_stage = AppFlowIngestionStage(
            self,
            id="appflow-stage",
            flow_name=flow.flow_name,
        )
        sqs_lambda_stage = SqsToLambdaStage(
            self,
            id="lambda-stage",
            lambda_function_props={
                "code": Code.from_asset("./ddk_app/lambda_handlers"),
                "handler": "handler.lambda_handler",
                "layers": [
                    LayerVersion.from_layer_version_arn(
                        self,
                        id="layer",
                        layer_version_arn=f"arn:aws:lambda:{self.region}:336392948345:layer:AWSDataWrangler-Python39:1",
                    )
                ],
                "runtime": Runtime.PYTHON_3_9,
            },
        )
        # Grant lambda function S3 read & write permissions
        bucket.grant_read_write(sqs_lambda_stage.function)
        # Grant Glue database & table permissions
        sqs_lambda_stage.function.add_to_role_policy(
            self._get_glue_db_iam_policy(database_name=database.database_name)
        )
        athena_stage = AthenaSQLStage(
            self,
            id="athena-sql",
            query_string=[
                (
                    "SELECT year, month, day, device, count(user_count) as cnt "
                    f"FROM {database.database_name}.ga_sample "
                    "GROUP BY year, month, day, device "
                    "ORDER BY cnt DESC "
                    "LIMIT 10; "
                )
            ],
            output_location=Location(
                bucket_name=bucket.bucket_name, object_key="query-results/"
            ),
            additional_role_policy_statements=[
                self._get_glue_db_iam_policy(database_name=database.database_name)
            ],
        )

        # Create data pipeline
        (
            DataPipeline(self, id="ingestion-pipeline")
            .add_stage(
                stage=appflow_stage,
                override_rule=Rule(
                    self,
                    "schedule-rule",
                    schedule=Schedule.rate(Duration.hours(1)),
                    targets=appflow_stage.targets,
                ),
            )
            .add_stage(
                stage=sqs_lambda_stage,
                # By default, AppFlowIngestionStage stage emits an event after the flow run finishes successfully
                # Override rule below changes that behavior to call the the stage when data lands in the bucket instead
                override_rule=Rule(
                    self,
                    "s3-object-created-rule",
                    event_pattern=EventPattern(
                        source=["aws.s3"],
                        detail={
                            "bucket": {"name": [bucket.bucket_name]},
                            "object": {"key": [{"prefix": "ga-data"}]},
                        },
                        detail_type=["Object Created"],
                    ),
                    targets=sqs_lambda_stage.targets,
                ),
            )
            .add_stage(stage=athena_stage)
        )

    def _create_s3_bucket(self) -> Bucket:
        bucket = Bucket(
            self,
            id="bucket",
            access_control=BucketAccessControl.BUCKET_OWNER_FULL_CONTROL,
            event_bridge_enabled=True,
            versioned=True,
            removal_policy=cdk.RemovalPolicy.DESTROY,
        )
        bucket.add_to_resource_policy(
            PolicyStatement(
                sid="AllowAppFlowDestinationActions",
                effect=Effect.ALLOW,
                principals=[ServicePrincipal(service="appflow.amazonaws.com")],
                actions=[
                    "s3:PutObject",
                    "s3:AbortMultipartUpload",
                    "s3:ListMultipartUploadParts",
                    "s3:ListBucketMultipartUploads",
                    "s3:GetBucketAcl",
                    "s3:PutObjectAcl",
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

    def _create_database(self, database_name: str) -> Database:
        return Database(
            self,
            id=database_name,
            database_name=database_name,
        )

    def _get_glue_db_iam_policy(self, database_name: str) -> PolicyStatement:
        return PolicyStatement(
            effect=Effect.ALLOW,
            actions=[
                "glue:CreateTable",
                "glue:getDatabase",
                "glue:getTable",
            ],
            resources=[
                f"arn:aws:glue:{self.region}:{self.account}:catalog",
                f"arn:aws:glue:{self.region}:{self.account}:database/{database_name}",
                f"arn:aws:glue:{self.region}:{self.account}:table/{database_name}/*",
            ],
        )

    def _create_ga_s3_ingest_flow(
        self,
        connector_profile_name: str,
        flow_name: str,
        google_analytics_object: str,
        bucket_name: str,
        bucket_prefix: str,
    ) -> CfnFlow:
        return CfnFlow(
            self,
            id=flow_name,
            flow_name=flow_name,
            source_flow_config=CfnFlow.SourceFlowConfigProperty(
                connector_type="Googleanalytics",
                connector_profile_name=connector_profile_name,
                source_connector_properties=CfnFlow.SourceConnectorPropertiesProperty(
                    google_analytics=CfnFlow.GoogleAnalyticsSourcePropertiesProperty(
                        object=google_analytics_object,
                    )
                ),
            ),
            destination_flow_config_list=[
                CfnFlow.DestinationFlowConfigProperty(
                    connector_type="S3",
                    destination_connector_properties=CfnFlow.DestinationConnectorPropertiesProperty(
                        s3=CfnFlow.S3DestinationPropertiesProperty(
                            bucket_name=bucket_name,
                            bucket_prefix=bucket_prefix,
                        ),
                    ),
                ),
            ],
            tasks=[
                CfnFlow.TaskProperty(
                    task_type="Filter",
                    source_fields=[
                        "ga:dateHour|DIMENSION",
                        "ga:deviceCategory|DIMENSION",
                        "ga:users|METRIC",
                    ],
                    connector_operator=CfnFlow.ConnectorOperatorProperty(
                        google_analytics="PROJECTION"
                    ),
                )
            ],
            trigger_config=CfnFlow.TriggerConfigProperty(
                trigger_type="OnDemand",
            ),
        )
