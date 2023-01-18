import json
import os
from pathlib import Path
from typing import Any
import aws_cdk as cdk
from aws_cdk import Duration
from aws_cdk.aws_events import Rule, RuleTargetInput, Schedule
from aws_cdk.aws_events_targets import SfnStateMachine
from aws_cdk.aws_glue import CfnCrawler
from aws_cdk.aws_glue_alpha import (Code, Database, GlueVersion, JobExecutable,
                                    JobLanguage, JobType)
from aws_cdk.aws_iam import (Effect, ManagedPolicy, PolicyStatement, Role,
                             ServicePrincipal)
from aws_cdk.aws_lambda import Code as lambda_code
from aws_cdk.aws_lambda import LayerVersion
from aws_cdk.aws_s3 import Bucket, BucketAccessControl
from aws_cdk.aws_stepfunctions import CfnStateMachine, JsonPath
from aws_ddk_core.base import BaseStack
from aws_ddk_core.pipelines import DataPipeline
from aws_ddk_core.resources import S3Factory, pandas_sdk_layer
from aws_ddk_core.stages import (AthenaSQLStage, GlueTransformStage,
                                 S3EventStage, SqsToLambdaStage)
from constructs import Construct


class AthenaQueryExecutionPipeline(BaseStack):
    def __init__(
        self, scope: Construct, id: str, environment_id: str, **kwargs: Any
    ) -> None:
        super().__init__(scope, id, environment_id, **kwargs)
    
        bucket = self._create_s3_bucket(environment_id=environment_id)
        database = self._create_database(database_name="athena_data")
        analytics_database = self._create_database(database_name="athena_analytics")

        path = os.path.join(f"{Path(__file__).parents[0]}", "configs.json")
        with open(path, encoding="utf-8") as config_file:
            configs = json.load(config_file).get(environment_id, [])

        # S3 Event Stage
        # ------------------------------------#
        s3_event_capture_stage = S3EventStage(
            self,
            id="s3-event-capture-stage",
            environment_id=environment_id,
            event_names=["Object Created"],
            bucket_name=bucket.bucket_name,
            key_prefix="data/",
        )

        # Sqs Lambda Stage
        # ------------------------------------#
        sqs_lambda_stage = SqsToLambdaStage(
            self,
            id="sqs-lambda-stage",
            environment_id=environment_id,
            code=lambda_code.from_asset("./athena_query_execution/lambda_handlers"),
            handler="handler.lambda_handler",
            layers=[pandas_sdk_layer(self)],
        )
        

        bucket.grant_read_write(sqs_lambda_stage.function)
        sqs_lambda_stage.function.add_environment("DB", database.database_name)
        sqs_lambda_stage.function.add_to_role_policy(
            self._get_glue_db_iam_policy(database_name=database.database_name)
        )

        # Athena SQL Stage
        # ------------------------------------#
        athena_stage = AthenaSQLStage(
            self,
            id="athena-sql-stage",
            query_string_path="$.queryString",
            environment_id=environment_id,
            output_bucket_name=bucket.bucket_name,
            output_object_key="query_output",
            additional_role_policy_statements=[
                self._get_glue_db_iam_policy(database_name=database.database_name)
            ],
        )

        bucket.grant_read_write(athena_stage.state_machine)

        # Glue Transform Stage
        # ------------------------------------#
        glue_crawler_role = Role(
            self,
            "glue-crawler-role",
            assumed_by=ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSGlueServiceRole"
                )
            ],
        )

        bucket.grant_read(glue_crawler_role)

        glue_transform_stage = GlueTransformStage(
            self,
            id="glue-transform-stage",
            environment_id=environment_id,
            executable=JobExecutable.of(
                glue_version=GlueVersion.V2_0,
                language=JobLanguage.PYTHON,
                script=Code.from_asset("./athena_query_execution/src/job.py"),
                type=JobType.ETL,
            ),
            job_args={
                "--additional-python-modules": "pyarrow==3,awswrangler",
                "--SFN_QUERY_INPUT": JsonPath.string_at("$.detail.input"),
                "--SFN_QUERY_OUTPUT": JsonPath.string_at("$.detail.output"),
            },
            glue_job_args={"max_concurrent_runs": 100},
            glue_crawler_args={
                "configuration": json.dumps(
                    {"Version": 1.0, "Grouping": {"TableLevelConfiguration": 3}}
                )
            },
            database_name=analytics_database.database_name,
            targets=CfnCrawler.TargetsProperty(
                s3_targets=[
                    CfnCrawler.S3TargetProperty(
                        path=f"s3://{bucket.bucket_name}/analytics/"
                    )
                ]
            ),
            crawler_role=glue_crawler_role,
        )

        glue_transform_stage.state_machine.add_to_role_policy(
            self._get_glue_crawler_iam_policy(crawler=glue_transform_stage.crawler.ref)
        )

        bucket.grant_read_write(glue_transform_stage.job)

        # Create data pipeline
        # ------------------------------------#
        athena_pipeline = (
            DataPipeline(self, id="athena-query-execution-pipeline")
            .add_stage(s3_event_capture_stage)
            .add_stage(sqs_lambda_stage)
            .add_stage(athena_stage, skip_rule=True)
            .add_stage(glue_transform_stage)
        ).add_notifications()

        # Create all rules through config file
        for config in configs:
            athena_pipeline.add_rule(
                id=f"schedule-rule-{config['queryId']}-rule",
                override_rule=Rule(
                    self,
                    f"schedule-rule-{config['queryId']}",
                    schedule=Schedule.expression(config["cronExpression"]),
                    targets=[
                        SfnStateMachine(
                            athena_stage.state_machine,
                            input=RuleTargetInput.from_object(config),
                        )
                    ],
                ),
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
                sid="AllowLambdaActions",
                effect=Effect.ALLOW,
                principals=[ServicePrincipal(service="lambda.amazonaws.com")],
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

    def _get_glue_crawler_iam_policy(self, crawler: str) -> PolicyStatement:
        return PolicyStatement(
            effect=Effect.ALLOW,
            actions=[
                "glue:StartCrawler",
            ],
            resources=[f"arn:aws:glue:{self.region}:{self.account}:crawler/{crawler}"],
        )
