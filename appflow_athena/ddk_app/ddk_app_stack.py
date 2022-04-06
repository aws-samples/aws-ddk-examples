from typing import Any

from aws_ddk_core.base import BaseStack
from aws_cdk.aws_lambda import Code
from aws_ddk_core.resources import S3Factory
from aws_ddk_core.pipelines import DataPipeline
from aws_ddk_core.stages import AppFlowIngestionStage, AthenaSQLStage, SqsToLambdaStage, S3EventStage
from aws_cdk import Duration
from aws_cdk.aws_events import Rule, Schedule
from aws_cdk.aws_iam import Effect, PolicyStatement
from aws_cdk.aws_lambda import LayerVersion
from constructs import Construct


class DdkApplicationStack(BaseStack):

    def __init__(self, scope: Construct, id: str, environment_id: str, **kwargs: Any) -> None:
        super().__init__(scope, id, environment_id, **kwargs)

        # Create S3 bucket
        bucket = S3Factory.bucket(
            self,
            id="bucket",
            environment_id=environment_id,
        )

        # Create stages
        appflow_stage = AppFlowIngestionStage(
            self,
            id="appflow-stage",
            environment_id=environment_id,
            flow_name="dummy-flow-1",
        )
        s3_event_stage = S3EventStage(
            self,
            id="s3-event-stage",
            environment_id=environment_id,
            event_names=["CopyObject", "PutObject"],
            bucket_name=bucket.bucket_name,
            key_prefix="appflow/out",
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
                    layer_version_arn=f"arn:aws:lambda:{self.region}:336392948345:layer:AWSDataWrangler-Python39:1"
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
                        f"arn:aws:glue:{self.region}:{self.account}:table/appflow_data/ga_sample"
                    ],
                )
            ],
        )

        # Create AppFlow ingestion pipeline
        (
            DataPipeline(self, id="ingestion-pipeline")
                .add_stage(
                    appflow_stage,
                    override_rule=Rule(
                        self,
                        "schedule-rule",
                        enabled=True,
                        schedule=Schedule.rate(Duration.minutes(5)),
                        targets=appflow_stage.get_targets(),
                    ),
                )
        )

        # Create transformation pipeline
        (
            DataPipeline(self, id="transformation-pipeline")
                .add_stage(s3_event_stage)
                .add_stage(sqs_lambda_stage)
                .add_stage(athena_stage)
        )
