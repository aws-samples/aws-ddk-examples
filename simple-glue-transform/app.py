#!/usr/bin/env python3
from typing import Any

import aws_cdk as cdk
import aws_cdk.aws_iam as iam
from aws_cdk.aws_glue import CfnCrawler
from aws_cdk.aws_glue_alpha import (
    Code,
    GlueVersion,
    JobExecutable,
    JobLanguage,
    JobType,
)
from aws_ddk_core.base import BaseStack
from aws_ddk_core.pipelines import DataPipeline
from aws_ddk_core.resources import S3Factory
from aws_ddk_core.stages import GlueTransformStage, S3EventStage
from constructs import Construct

app = cdk.App()


class DdkApplicationStack(BaseStack):
    def __init__(
        self, scope: Construct, id: str, environment_id: str, **kwargs: Any
    ) -> None:
        super().__init__(scope, id, environment_id, **kwargs)

        bucket = S3Factory.bucket(
            self,
            id="bucket",
            environment_id=environment_id,
            event_bridge_enabled=True,
        )

        s3_stage = S3EventStage(
            self,
            id="s3-event-capture",
            environment_id=self._environment_id,
            event_names=["Object Created"],
            bucket_name=bucket.bucket_name,
            key_prefix="source",
        )

        glue_crawler_role = iam.Role(
            self,
            "glue-crawler-role",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSGlueServiceRole"
                )
            ],
        )
        bucket.grant_read(
            glue_crawler_role,
            objects_key_pattern="target/",  # optionally specify exactly which bucket prefix you need access to
        )

        glue_transform_stage = GlueTransformStage(
            self,
            id="glue-transform-simple",
            environment_id=environment_id,
            executable=JobExecutable.of(
                glue_version=GlueVersion.V2_0,
                language=JobLanguage.PYTHON,
                script=Code.from_asset("src/sample-job.py"),
                type=JobType.ETL,
            ),
            database_name="glue-database",
            targets=CfnCrawler.TargetsProperty(
                s3_targets=[
                    CfnCrawler.S3TargetProperty(
                        path=f"s3://{bucket.bucket_name}/target/"
                    )
                ]
            ),
            crawler_role=glue_crawler_role,
            job_args={
                "--S3_SOURCE_PATH": bucket.arn_for_objects("source/"),
                "--S3_TARGET_PATH": bucket.arn_for_objects("target/"),
            },
        )

        bucket.grant_read_write(glue_transform_stage.job)
        glue_transform_stage.state_machine.role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "glue:StartCrawler",
                ],
                resources=[
                    f"arn:aws:glue:{self.region}:{self.account}:crawler/{glue_transform_stage.crawler.ref}"
                ],
            ),
        )

        (
            DataPipeline(scope=self, id="ddk-pipeline")
            .add_stage(s3_stage)
            .add_stage(glue_transform_stage)
        )


DdkApplicationStack(app, id="SamplePipeline", environment_id="dev")

app.synth()
