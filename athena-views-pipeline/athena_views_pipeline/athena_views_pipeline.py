from typing import Any

from aws_cdk.aws_events import EventPattern, Rule, RuleTargetInput, Schedule
from aws_cdk.aws_events_targets import SfnStateMachine
from aws_cdk.aws_iam import Effect, PolicyStatement, ServicePrincipal
from aws_cdk.aws_lambda import Code as lambda_code
from aws_cdk.aws_s3 import Bucket, BucketAccessControl
from aws_cdk.aws_dynamodb import Table, AttributeType, Attribute
from aws_cdk.aws_stepfunctions import JsonPath
from aws_ddk_core.base import BaseStack
from aws_ddk_core.pipelines import DataPipeline
from aws_ddk_core.resources import S3Factory, pandas_sdk_layer
from aws_ddk_core.stages import AthenaSQLStage, SqsToLambdaStage
from constructs import Construct
import aws_cdk as cdk
from athena_views_pipeline.utils import utils


class AthenaViewsPipeline(BaseStack):
    def __init__(
        self, scope: Construct, id: str, environment_id: str, **kwargs: Any
    ) -> None:
        super().__init__(scope, id, environment_id, **kwargs)

        bucket = self._create_s3_bucket(environment_id=environment_id)
        events_list = utils.get_events_json()

        self._athena_stage = AthenaSQLStage(
            self,
            id="athena-sql-stage",
            query_string=JsonPath.format(
                'CREATE OR REPLACE VIEW "{}"."{}" AS {}',
                JsonPath.string_at("$.db"),
                JsonPath.string_at("$.view"),
                JsonPath.string_at("$.query"),
            ),
            environment_id=environment_id,
            output_bucket_name=bucket.bucket_name,
            output_object_key="query_output",
            additional_role_policy_statements=[self._get_glue_db_iam_policy()],
        )

        bucket.grant_read_write(self._athena_stage.state_machine)

        self._sqs_lambda_stage = SqsToLambdaStage(
            self,
            id="sqs-lambda-stage",
            environment_id=environment_id,
            code=lambda_code.from_asset("./athena_views_pipeline/lambda_handlers"),
            handler="handler.lambda_handler",
        )

        self._ddb_table = Table(
            self,
            id=f"ddb-failure-capture-table",
            partition_key=Attribute(name="view_name", type=AttributeType.STRING),
            sort_key=Attribute(name="db", type=AttributeType.STRING),
            removal_policy=cdk.RemovalPolicy.DESTROY
        )

        self._ddb_table.grant_read_write_data(self._sqs_lambda_stage.function)
        self._sqs_lambda_stage.function.add_environment(
            key="DDB_TABLE",
            value=self._ddb_table.table_name
        )

        self._athena_views_pipeline = (
            DataPipeline(self, id="athena-views-execution-pipeline")
            .add_stage(self._athena_stage)
            .add_stage(
                self._sqs_lambda_stage, override_rule=self._get_failure_override_rule()
            )
        )

        for config in events_list:
            self._add_rule(config)

    def _get_failure_override_rule(self):
        return Rule(
            self,
            f"failure-rule",
            event_pattern=EventPattern(
                source=["aws.states"],
                detail_type=["Step Functions Execution Status Change"],
                detail={
                    "status": ["FAILED"],
                    "stateMachineArn": [
                        self._athena_stage.state_machine.state_machine_arn
                    ],
                },
            ),
            targets=self._sqs_lambda_stage.get_targets(),
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

    def _get_glue_db_iam_policy(self) -> PolicyStatement:
        return PolicyStatement(
            effect=Effect.ALLOW,
            actions=[
                "glue:CreateTable",
                "glue:getDatabase",
                "glue:getTable",
                "glue:updateTable"
            ],
            resources=[
                f"arn:aws:glue:{self.region}:{self.account}:catalog",
                f"arn:aws:glue:{self.region}:{self.account}:database/*",
                f"arn:aws:glue:{self.region}:{self.account}:table/*/*",
            ],
        )

    def _add_rule(self, config: dict):
        self._athena_views_pipeline.add_rule(
            id=f"schedule-rule-{config['db']}-{config['view']}-rule",
            override_rule=Rule(
                self,
                f"schedule-rule-{config['db']}-{config['view']}",
                schedule=Schedule.expression(config["schedule"]),
                targets=[
                    SfnStateMachine(
                        self._athena_stage.state_machine,
                        input=RuleTargetInput.from_object(config),
                    )
                ],
            ),
        )
