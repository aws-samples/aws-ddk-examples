from typing import Any, Dict

import aws_cdk as cdk
import aws_cdk.aws_events as events
from aws_cdk.aws_events import Rule
from aws_cdk.aws_events_targets import EventBus
from aws_cdk.aws_iam import (
    AccountPrincipal,
    Effect,
    ManagedPolicy,
    PolicyStatement,
    Role,
    ServicePrincipal,
)
from aws_cdk.aws_kms import Key
from aws_cdk.aws_s3 import Bucket, BucketAccessControl, BucketEncryption
from aws_cdk.aws_ssm import StringParameter
from aws_ddk_core import BaseStack, Configurator, S3EventStage
from constructs import Construct


class DataStorage(BaseStack):
    def __init__(
        self,
        scope: Construct,
        id: str,
        environment_id: str,
        mode: str,
        compute_params: Configurator,
        **kwargs: Any,
    ) -> None:
        super().__init__(scope, id, environment_id=environment_id, **kwargs)
        self._environment_id = environment_id
        self._S3_NAME = compute_params.get_config_attribute("s3BucketName")
        self._mode = mode
        self._compute_account = compute_params.get_config_attribute("account")
        self._compute_region = compute_params.get_config_attribute("region")

        self._raw_bucket = self._create_bucket(name=self._S3_NAME)

        if self._mode != "same_account_region":
            self._create_eb_rule()

    def _create_bucket(self, name: str) -> Bucket:
        bucket_key: Key = Key(
            self,
            id=f"{name}-bucket-key",
            description=f"{name.title()} Bucket Key",
            alias=f"{name}-bucket-key",
            enable_key_rotation=True,
            pending_window=cdk.Duration.days(30),
            removal_policy=cdk.RemovalPolicy.DESTROY,
        )
        bucket_key.add_to_resource_policy(
            statement=PolicyStatement(
                effect=Effect.ALLOW,
                principals=[AccountPrincipal(self._compute_account)],
                actions=[
                    "kms:CreateGrant",
                    "kms:Decrypt",
                    "kms:DescribeKey",
                    "kms:Encrypt",
                    "kms:GenerateDataKey*",
                    "kms:ReEncrypt*",
                ],
                resources=["*"],
            )
        )

        bucket: Bucket = Bucket(
            self,
            id=f"{name}-bucket",
            bucket_name=name,
            encryption=BucketEncryption.KMS,
            encryption_key=bucket_key,
            access_control=BucketAccessControl.BUCKET_OWNER_FULL_CONTROL,
            removal_policy=cdk.RemovalPolicy.RETAIN,
            event_bridge_enabled=True,
        )
        bucket.add_to_resource_policy(
            permission=PolicyStatement(
                effect=Effect.ALLOW,
                principals=[AccountPrincipal(self._compute_account)],
                actions=["s3:Get*"],
                resources=[f"{bucket.bucket_arn}/*"],
            )
        )

        StringParameter(
            self,
            f"{name}-bucket-key-arn-ssm",
            parameter_name=f"/DDK/KMS/{name.title()}BucketKeyArn",
            string_value=bucket_key.key_arn,
        )

        StringParameter(
            self,
            f"{name}-bucket-arn-ssm",
            parameter_name=f"/DDK/S3/{name.title()}BucketArn",
            string_value=bucket.bucket_arn,
        )

        return bucket

    def _create_eb_rule(self):
        data_lake_s3_event_capture_stage = S3EventStage(
            self,
            id=f"s3-event-capture",
            event_names=["Object Created", "Object Deleted"],
            bucket=self._raw_bucket,
        )

        eb_rule = Rule(
            self,
            "s3-event-capture-rule",
            event_pattern=data_lake_s3_event_capture_stage.event_pattern,
        )

        eb_iam_policy = ManagedPolicy(
            self,
            "s3-event-capture-policy",
            statements=[
                PolicyStatement(
                    effect=Effect.ALLOW,
                    actions=["events:PutEvents"],
                    resources=[
                        f"arn:aws:events:{self._compute_region}:{self._compute_account}:event-bus/default",
                        f"arn:aws:events:{self._compute_region}:{self._compute_account}:rule/default/*",
                    ],
                )
            ],
        )
        eb_role: Role = Role(
            self,
            "s3-event-capture-role",
            assumed_by=ServicePrincipal("events.amazonaws.com"),
            role_name="s3-event-capture-role",
            managed_policies=[eb_iam_policy],
        )

        eb_rule.add_target(
            EventBus(
                event_bus=events.EventBus.from_event_bus_arn(
                    self,
                    "External",
                    f"arn:aws:events:{self._compute_region}:{self._compute_account}:event-bus/default",
                ),
                role=eb_role,
            )
        )
