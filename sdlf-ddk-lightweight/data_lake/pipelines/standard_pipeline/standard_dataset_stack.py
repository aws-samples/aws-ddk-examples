# Copyright 2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License").
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from dataclasses import dataclass
from typing import Any, Optional
import json

import aws_cdk as cdk
import aws_cdk.aws_glue as glue
import aws_cdk.aws_sqs as sqs
import aws_cdk.aws_iam as iam
import aws_cdk.aws_kms as kms
import aws_cdk.aws_lakeformation as lf
import aws_cdk.aws_s3 as s3
import aws_cdk.aws_ssm as ssm
from aws_cdk.custom_resources import Provider
from aws_ddk_core.base import BaseStack
from aws_ddk_core.resources import KMSFactory, SQSFactory
from constructs import Construct


@dataclass
class StandardDatasetConfig:
    team: str
    dataset: str
    pipeline: str
    stage_a_transform: str
    stage_b_transform: str
    artifacts_bucket: s3.IBucket
    artifacts_bucket_key: kms.IKey
    stage_bucket: s3.IBucket
    stage_bucket_key: kms.IKey
    glue_role: iam.IRole
    register_provider: Provider


class StandardDatasetStack(BaseStack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        environment_id: str,
        resource_prefix: str,
        config: StandardDatasetConfig,
        **kwargs: Any
    ) -> None:
        self._dataset_config: StandardDatasetConfig = config
        self._resource_prefix = resource_prefix
        self._environment_id: str = environment_id
        self._team = self._dataset_config.team
        self._pipeline = self._dataset_config.pipeline
        self._dataset = self._dataset_config.dataset
        super().__init__(
            scope,
            construct_id,
            environment_id,
            stack_name=f"{self._resource_prefix}-StandardDataset-{self._team}"
                       + f"-{self._dataset}-{self._environment_id}",
            **kwargs
        )

        self._stage_a_transform = self._dataset_config.stage_a_transform
        self._stage_b_transform = self._dataset_config.stage_b_transform

        glue_path = f"data_lake/src/glue/pyshell_scripts/sdlf_heavy_transform/{self._team}/{self._dataset}/main.py"

        self._create_stage_b_glue_job(self._team, self._dataset, glue_path)

        self._register_octagon_configs(
            self._team,
            self._pipeline,
            self._dataset,
            self._stage_a_transform,
            self._stage_b_transform
        )

        self._create_glue_database(self._team, self._dataset)

        self._create_routing_queue()

    @property
    def database(self) -> glue.CfnDatabase:
        return self._database

    def _create_stage_b_glue_job(
        self,
        team: str,
        dataset_name: str,
        code_path: str
    ):
        glue.CfnJob(
            self,
            f"{self._resource_prefix}-heavy-transform-{team}-{dataset_name}-job",
            name=f"{self._resource_prefix}-{team}-{dataset_name}-glue-job",
            glue_version="2.0",
            allocated_capacity=2,
            execution_property=glue.CfnJob.ExecutionPropertyProperty(max_concurrent_runs=4),
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                script_location=f"s3://{self._dataset_config.artifacts_bucket.bucket_name}/{code_path}",
            ),
            default_arguments={
                "--job-bookmark-option": "job-bookmark-enable",
                "--enable-metrics": "",
                "--additional-python-modules": "awswrangler==2.4.0",
                "--enable-glue-datacatalog": ""
            },
            role=self._dataset_config.glue_role.role_arn,
        )

    def _register_octagon_configs(
        self,
        team: str,
        pipeline: str,
        dataset_name: str,
        stage_a_transform: Optional[str] = None,
        stage_b_transform: Optional[str] = None
    ):
        self.stage_a_transform: str = stage_a_transform if stage_a_transform else "light_transform_blueprint"
        self.stage_b_transform: str = stage_b_transform if stage_b_transform else "heavy_transform_blueprint"

        self._props = {
            "id": f"{team}-{dataset_name}",
            "description": f"{dataset_name.title()} dataset",
            "name": f"{team}-{dataset_name}",
            "type": "octagon_dataset",
            "pipeline": pipeline,
            "max_items_process": {
                "stage_b": 100,
                "stage_c": 100
            },
            "min_items_process": {
                "stage_b": 1,
                "stage_c": 1
            },
            "version": 1,
            "transforms": {
                "stage_a_transform": self.stage_a_transform,
                "stage_b_transform": self.stage_b_transform,
            }
        }

        service_setup_properties = {"RegisterProperties": json.dumps(self._props)}

        cdk.CustomResource(
            self,
            f"{self._props['id']}-{self._props['type']}-custom-resource",
            service_token=self._dataset_config.register_provider.service_token,
            properties=service_setup_properties
        )

    def _create_glue_database(
        self,
        team: str,
        dataset_name: str
    ) -> None:
        lf.CfnDataLakeSettings(
            self,
            f"{self._resource_prefix}-{team}-{dataset_name}-DataLakeSettings",
            admins=[lf.CfnDataLakeSettings.DataLakePrincipalProperty(
                data_lake_principal_identifier=self._dataset_config.glue_role.role_arn
            )]
        )

        self._database: glue.CfnDatabase = glue.CfnDatabase(
            self,
            f"{self._resource_prefix}-{team}-{dataset_name}-database",
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name=f"aws_datalake_{self._environment_id}_{team}_{dataset_name}_db",
                location_uri=f"s3://{self._dataset_config.stage_bucket.bucket_name}/post-stage/{team}/{dataset_name}"
            ),
            catalog_id=cdk.Aws.ACCOUNT_ID
        )

        lf.CfnPermissions(
            self,
            f"{self._resource_prefix}-{team}-{dataset_name}-glue-job-database-lakeformation-permissions",
            data_lake_principal=lf.CfnPermissions.DataLakePrincipalProperty(
                data_lake_principal_identifier=self._dataset_config.glue_role.role_arn
            ),
            resource=lf.CfnPermissions.ResourceProperty(
                database_resource=lf.CfnPermissions.DatabaseResourceProperty(name=self._database.ref)
            ),
            permissions=["CREATE_TABLE", "ALTER", "DROP"],
        )

    def _create_routing_queue(self):
        sqs_key = KMSFactory.key(
            self,
            id=f"{self._resource_prefix}-{self._team}-{self._dataset}-sqs-key-b",
            environment_id=self._environment_id,
            description=f"{self._resource_prefix} SQS Key Stage B",
            alias=f"{self._resource_prefix}-{self._team}-{self._dataset}-sqs-stage-b-key",
            enable_key_rotation=True,
            pending_window=cdk.Duration.days(30),
            removal_policy=cdk.RemovalPolicy.DESTROY,
        )

        routing_dlq = sqs.DeadLetterQueue(
            max_receive_count=1,
            queue=SQSFactory.queue(
                self,
                id=f'{self._resource_prefix}-{self._team}-{self._dataset}-dlq-b.fifo',
                environment_id=self._environment_id,
                queue_name=f'{self._resource_prefix}-{self._team}-{self._dataset}-dlq-b.fifo',
                fifo=True,
                visibility_timeout=cdk.Duration.seconds(60),
                encryption=sqs.QueueEncryption.KMS,
                encryption_master_key=sqs_key
            )
        )

        ssm.StringParameter(
            self,
            f'{self._resource_prefix}-{self._team}-{self._dataset}-dlq-b.fifo-ssm',
            parameter_name=f"/SDLF/SQS/{self._team}/{self._dataset}StageBDLQ",
            string_value=f'{self._resource_prefix}-{self._team}-{self._dataset}-dlq-b.fifo',
        )

        SQSFactory.queue(
            self,
            id=f'{self._resource_prefix}-{self._team}-{self._dataset}-queue-b.fifo',
            environment_id=self._environment_id,
            queue_name=f'{self._resource_prefix}-{self._team}-{self._dataset}-queue-b.fifo',
            fifo=True,
            visibility_timeout=cdk.Duration.seconds(60),
            encryption=sqs.QueueEncryption.KMS,
            encryption_master_key=sqs_key,
            dead_letter_queue=routing_dlq
        )

        ssm.StringParameter(
            self,
            f'{self._resource_prefix}-{self._team}-{self._dataset}-queue-b.fifo-ssm',
            parameter_name=f"/SDLF/SQS/{self._team}/{self._dataset}StageBQueue",
            string_value=f'{self._resource_prefix}-{self._team}-{self._dataset}-queue-b.fifo',
        )
