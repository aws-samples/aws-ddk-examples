from typing import Any

from aws_cdk import Duration
from aws_cdk.aws_appflow import CfnFlow
from aws_cdk.aws_events import Rule, Schedule
from aws_cdk.aws_iam import Effect, PolicyStatement, ServicePrincipal
from aws_cdk.aws_lambda import Code, LayerVersion
from aws_cdk.aws_s3 import Bucket, BucketAccessControl
from aws_ddk_core.base import BaseStack
from aws_ddk_core.pipelines import DataPipeline
from aws_ddk_core.resources import S3Factory
from aws_ddk_core.stages import (
    AppFlowIngestionStage,
    AthenaSQLStage,
    S3EventStage,
    SqsToLambdaStage,
)
from constructs import Construct


class DdkApplicationStack(BaseStack):
    def __init__(
        self, scope: Construct, id: str, environment_id: str, **kwargs: Any
    ) -> None:
        super().__init__(scope, id, environment_id, **kwargs)

        # Create S3 bucket
        bucket = self._create_s3_bucket(environment_id=environment_id)

        # Create Google Analytics AppFlow flow
        flow_name = "ga-flow"
        flow = self._create_ga_s3_ingest_flow(
            connector_profile_name="ga-connection",
            flow_name=flow_name,
            google_analytics_object="264236892",
            bucket_name=bucket.bucket_name,
            bucket_prefix="ga-data",
        )

        # Create stages
        appflow_stage = AppFlowIngestionStage(
            self,
            id="appflow-stage",
            environment_id=environment_id,
            flow_name=flow_name,
        )
        s3_event_stage = S3EventStage(
            self,
            id="s3-event-stage",
            environment_id=environment_id,
            event_names=["CopyObject", "PutObject", "CompleteMultipartUpload"],
            bucket_name=bucket.bucket_name,
            key_prefix="ga-data",
        )
        sqs_lambda_stage = SqsToLambdaStage(
            self,
            id="lambda-stage",
            environment_id=environment_id,
            code=Code.from_asset("./ddk_app/lambda_handlers"),
            handler="handler.lambda_handler",
            layers=[
                LayerVersion.from_layer_version_arn(
                    self,
                    id="layer",
                    layer_version_arn=f"arn:aws:lambda:{self.region}:336392948345:layer:AWSDataWrangler-Python39:1",
                )
            ],
        )
        bucket.grant_read_write(sqs_lambda_stage.function)
        athena_stage = AthenaSQLStage(
            self,
            id="athena-sql",
            environment_id=environment_id,
            query_string=(
                "SELECT year, month, day, device, count(user_count) as cnt "
                "FROM appflow_data.ga_sample "
                "GROUP BY year, month, day, device "
                "ORDER BY cnt DESC "
                "LIMIT 10; "
            ),
            output_bucket_name=bucket.bucket_name,
            output_object_key="query-results/",
            additional_role_policy_statements=[
                PolicyStatement(
                    effect=Effect.ALLOW,
                    actions=[
                        "glue:getDatabase",
                        "glue:getTable",
                    ],
                    resources=[
                        f"arn:aws:glue:{self.region}:{self.account}:database/appflow_data",
                        f"arn:aws:glue:{self.region}:{self.account}:table/appflow_data/ga_sample",
                    ],
                )
            ],
        )

        # Create AppFlow ingestion pipeline
        (
            DataPipeline(self, id="ingestion-pipeline").add_stage(
                appflow_stage,
                override_rule=Rule(
                    self,
                    "schedule-rule",
                    enabled=True,
                    schedule=Schedule.rate(Duration.minutes(2)),
                    targets=appflow_stage.get_targets(),
                ),
            )
        )

        # Create data transformation pipeline
        (
            DataPipeline(self, id="transformation-pipeline")
            .add_stage(s3_event_stage)
            .add_stage(sqs_lambda_stage)
            .add_stage(athena_stage)
        )

    def _create_s3_bucket(self, environment_id: str) -> Bucket:
        bucket = S3Factory.bucket(
            self,
            id="bucket",
            environment_id=environment_id,
            access_control=BucketAccessControl.BUCKET_OWNER_FULL_CONTROL,
            event_bridge_enabled=True,
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
