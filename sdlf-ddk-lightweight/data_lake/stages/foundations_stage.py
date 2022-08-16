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


import os
from pathlib import Path
from typing import Any, Dict

import aws_cdk as cdk
from aws_ddk_core.pipelines.stage import DataStage
from aws_ddk_core.base import BaseStack
from aws_ddk_core.resources import S3Factory, KMSFactory, LambdaFactory
import aws_cdk.aws_dynamodb as ddb
import aws_cdk.aws_iam as iam
import aws_cdk.aws_kms as kms
import aws_cdk.aws_lakeformation as lf
import aws_cdk.aws_lambda as lmbda
import aws_cdk.aws_s3  as s3
import aws_cdk.aws_ssm as ssm
from aws_cdk.custom_resources import Provider
from aws_cdk.aws_s3_deployment import BucketDeployment, ServerSideEncryption, Source


class FoundationsStage(BaseStack):
    def __init__(
        self,
        scope,
        id,
        environment_id: str,
        resource_prefix: str,
        app: str,
        org:str,
        runtime: lmbda.Runtime,
        **kwargs: Any,
    ) -> None:
        super().__init__(scope, id, environment_id, **kwargs)

        self._environment_id: str = environment_id
        self._app = app
        self._org = org
        self._resource_prefix = resource_prefix

        self._object_metadata = self._create_octagon_ddb_table(
            name=f"octagon-ObjectMetadata-{self._environment_id}",
            ddb_props={"partition_key": ddb.Attribute(name="id", type=ddb.AttributeType.STRING)},
        )
    
        self._datasets = self._create_octagon_ddb_table(
            name=f"octagon-Datasets-{self._environment_id}",
            ddb_props={"partition_key": ddb.Attribute(name="name", type=ddb.AttributeType.STRING)},
        )

        self._pipelines = self._create_octagon_ddb_table(
            name=f"octagon-Pipelines-{self._environment_id}",
            ddb_props={"partition_key": ddb.Attribute(name="name", type=ddb.AttributeType.STRING)},
        )
        self._peh = self._create_octagon_ddb_table(
            name=f"octagon-PipelineExecutionHistory-{self._environment_id}",
            ddb_props={"partition_key": ddb.Attribute(name="id", type=ddb.AttributeType.STRING)},
        )

        
        self._create_register(runtime)
        self._routing_function = self._create_routing_lambda(runtime)
        self._lakeformation_bucket_registration_role = self._create_lakeformation_bucket_registration_role()
        self._raw_bucket, self._raw_bucket_key = self._create_bucket(name="raw")
        self._stage_bucket, self._stage_bucket_key = self._create_bucket(name="stage")
        self._analytics_bucket, self._analytics_bucket_key = self._create_bucket(name="analytics")
        self._artifacts_bucket, self._artifacts_bucket_key = self._create_bucket(name="artifacts")
        self._athena_bucket, self._athena_bucket_key = self._create_bucket(name="athena")
        
        self._glue_role = self._create_sdlf_glue_artifacts()
        self._data_lake_library = self._create_data_lake_library_layer()
        

    def _create_routing_lambda(self, runtime: lmbda.Runtime) -> None:

        #Lambda
        routing_function: lmbda.Function = LambdaFactory.function(
            self,
            id=f"{self._resource_prefix}-data-lake-routing-function",
            environment_id = self._environment_id,
            function_name=f"{self._resource_prefix}-data-lake-routing",
            code=lmbda.Code.from_asset(os.path.join(f"{Path(__file__).parents[1]}", "src/lambdas/routing")),
            handler="handler.lambda_handler",
            description="routes to the right team and pipeline",
            timeout=cdk.Duration.seconds(60),
            memory_size=256,
            runtime = runtime,
            environment={
                "ENV": self._environment_id,
                "APP": self._app,
                "ORG": self._org,
                "PREFIX": self._resource_prefix
            },
        )
        self._object_metadata.grant_read_write_data(routing_function)
        self._datasets.grant_read_write_data(routing_function)
        routing_function.add_to_role_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "kms:CreateGrant",
                    "kms:Decrypt",
                    "kms:DescribeKey",
                    "kms:Encrypt",
                    "kms:GenerateDataKey",
                    "kms:GenerateDataKeyPair",
                    "kms:GenerateDataKeyPairWithoutPlaintext",
                    "kms:GenerateDataKeyWithoutPlaintext",
                    "kms:ReEncryptTo",
                    "kms:ReEncryptFrom",
                    "kms:ListAliases",
                    "kms:ListGrants",
                    "kms:ListKeys",
                    "kms:ListKeyPolicies"
                ],
                resources=["*"],
                conditions={
                    "ForAnyValue:StringLike":{
                        "kms:ResourceAliases": f"alias/*"
                    }
                }
            )
        )
        routing_function.add_to_role_policy(
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "sqs:SendMessage",
                        "sqs:DeleteMessage",
                        "sqs:ReceiveMessage",
                        "sqs:GetQueueAttributes",
                        "sqs:ListQueues",
                        "sqs:GetQueueUrl",
                        "sqs:ListDeadLetterSourceQueues",
                        "sqs:ListQueueTags"
                    ],
                    resources=[f"arn:aws:sqs:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:{self._resource_prefix}-*"],
                )
        )
        routing_function.add_to_role_policy(
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "ssm:GetParameter",
                        "ssm:GetParameters"
                    ],
                    resources=[f"arn:aws:ssm:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:parameter/SDLF/*"],
                )
        )

        routing_function.add_permission(
            id= "invoke-lambda-eventbridge",
            principal= iam.ServicePrincipal("events.amazonaws.com"),
            action= "lambda:InvokeFunction"
        )

        return routing_function

    
    def _create_octagon_ddb_table(self, name: str, ddb_props: Dict[str, Any]) -> ddb.Table:
        
        tbleName = name.split("-")[1]

        #ddb kms key resource
        table_key: kms.Key = KMSFactory.key(
            self,
            id=f"{name}-table-key",
            environment_id = self._environment_id,
            description=f"{self._resource_prefix} {name.title()} Table Key",
            alias=f"{self._resource_prefix}-{name}-ddb-table-key",
            enable_key_rotation=True,
            pending_window=cdk.Duration.days(30),
            removal_policy=cdk.RemovalPolicy.DESTROY,
        )

        #ddb resource
        table: ddb.Table = ddb.Table(
            self,
            f"{name}-table",
            table_name=name,
            encryption=ddb.TableEncryption.CUSTOMER_MANAGED,
            encryption_key=table_key,
            billing_mode=ddb.BillingMode.PAY_PER_REQUEST,
            removal_policy= cdk.RemovalPolicy.DESTROY,
            point_in_time_recovery=True,
            **ddb_props,
        )

        #SSM for ddb table name
        ssm.StringParameter(
            self,
            f"{name}-table-name-ssm",
            parameter_name=f"/SDLF/DynamoDB/{tbleName}",
            string_value=name,
        )
        return table

    def _create_register(self, runtime: lmbda.Runtime) -> None:

        self._register_function: lmbda.Function = LambdaFactory.function(
            self,
            id="register-function",
            environment_id = self._environment_id,
            code=lmbda.Code.from_asset(os.path.join(f"{Path(__file__).parents[1]}", "src/lambdas/register")),
            handler="handler.on_event",
            memory_size=256,
            description="Registers Datasets, Pipelines and Stages into their respective DynamoDB tables",
            timeout=cdk.Duration.seconds(15 * 60),
            runtime = runtime,
            environment={
                "OCTAGON_DATASET_TABLE_NAME": self._datasets.table_name,
                "OCTAGON_PIPELINE_TABLE_NAME": self._pipelines.table_name
            }
        )
        self._datasets.grant_read_write_data(self._register_function)
        self._pipelines.grant_read_write_data(self._register_function)

        self._register_provider = Provider(
            self,
            "register-provider",
            on_event_handler=self._register_function,
        )

    def _create_lakeformation_bucket_registration_role(self) -> None:
        lakeformation_bucket_registration_role: iam.Role = iam.Role(
            self,
            "lakeformation-bucket-registration-role",
            assumed_by=iam.ServicePrincipal("lakeformation.amazonaws.com"),
            inline_policies={
                "LakeFormationDataAccessPolicyForS3": iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=["s3:ListAllMyBuckets"],
                            resources=[ "arn:aws:s3:::*"],
                        ),
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=["s3:ListBucket"],
                            resources=[f"arn:aws:s3:::{self._resource_prefix}-{self._environment_id}-{cdk.Aws.REGION}-{cdk.Aws.ACCOUNT_ID}-*"],
                        ),
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=[
                                "s3:GetObject",
                                "s3:GetObjectAttributes",
                                "s3:GetObjectTagging",
                                "s3:GetObjectVersion",
                                "s3:GetObjectVersionAttributes",
                                "s3:GetObjectVersionTagging",
                                "s3:PutObjectTagging",
                                "s3:PutObjectVersionTagging",
                                "s3:PutObject"
                            ],
                            resources=[f"arn:aws:s3:::{self._resource_prefix}-{self._environment_id}-{cdk.Aws.REGION}-{cdk.Aws.ACCOUNT_ID}-*"],
                        ),
                    ]
                )
            },
        )

        return lakeformation_bucket_registration_role

    def _create_bucket(self, name: str) -> s3.Bucket:
        bucket_key: kms.Key = KMSFactory.key(
            self,
            id=f"{self._resource_prefix}-{name}-bucket-key",
            environment_id=self._environment_id,
            description=f"{self._resource_prefix} {name.title()} Bucket Key",
            alias=f"{self._resource_prefix}-{name}-bucket-key",
            enable_key_rotation=True,
            pending_window=cdk.Duration.days(30),
            removal_policy=cdk.RemovalPolicy.DESTROY
        )

        ssm.StringParameter(
            self,
            f"{self._resource_prefix}-{name}-bucket-key-arn-ssm",
            parameter_name=f"/SDLF/KMS/{name.title()}BucketKeyArn",
            string_value=bucket_key.key_arn,
        )

        bucket: s3.Bucket = S3Factory.bucket(
            self,
            id=f"{self._resource_prefix}-{name}-bucket",
            environment_id = self._environment_id,
            bucket_name=f"{self._resource_prefix}-{self._environment_id}-{cdk.Aws.REGION}-{cdk.Aws.ACCOUNT_ID}-{name}",
            encryption=s3.BucketEncryption.KMS,
            encryption_key=bucket_key,
            access_control=s3.BucketAccessControl.BUCKET_OWNER_FULL_CONTROL,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=cdk.RemovalPolicy.RETAIN,
            event_bridge_enabled = True
        )

        ssm.StringParameter(
            self,
            f"{self._resource_prefix}-{name}-bucket-name-ssm",
            parameter_name=f"/SDLF/S3/{name.title()}Bucket",
            string_value=f"{self._resource_prefix}-{self._environment_id}-{cdk.Aws.REGION}-{cdk.Aws.ACCOUNT_ID}-{name}",
        )

        lf.CfnResource(
            self,
            f"{self._resource_prefix}-{name}-bucket-lakeformation-registration",
            resource_arn=bucket.bucket_arn,
            use_service_linked_role=False,
            role_arn=self._lakeformation_bucket_registration_role.role_arn,
        )
        
        bucket_key.add_to_resource_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "kms:CreateGrant",
                    "kms:Decrypt",
                    "kms:DescribeKey",
                    "kms:Encrypt",
                    "kms:GenerateDataKey*",
                    "kms:ReEncrypt*",
                ],
                resources=["*"],
                principals=[self._lakeformation_bucket_registration_role],
            )
        )
        return bucket, bucket_key
    

    def _create_data_lake_library_layer(self) -> None:
        data_lake_library_layer = lmbda.LayerVersion(
            self,
            "data-lake-library-layer",
            layer_version_name=f"data-lake-library",
            code=lmbda.Code.from_asset(os.path.join(f"{Path(__file__).parents[1]}", "src/layers/data_lake_library")),
            compatible_runtimes=[lmbda.Runtime.PYTHON_3_9],
            description=f"{self._resource_prefix} Data Lake Library",
            license="Apache-2.0",
        )
        return data_lake_library_layer


    def _create_sdlf_glue_artifacts(self) -> None:

        bucket_deployment_role: iam.Role = iam.Role(
            self,
            f"{self._resource_prefix}-glue-script-s3-deployment-role",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole")],
        )
        
        self._artifacts_bucket_key.grant_encrypt_decrypt(bucket_deployment_role)
        
        glue_path = "data_lake/src/glue/"
        
        BucketDeployment(
            self,
            f"{self._resource_prefix}-glue-script-s3-deployment",
            sources=[Source.asset(f"{glue_path}")],
            destination_bucket=self._artifacts_bucket,
            destination_key_prefix=f"{glue_path}",
            server_side_encryption_aws_kms_key_id=self._artifacts_bucket_key.key_id,
            server_side_encryption=ServerSideEncryption.AWS_KMS,
            role=bucket_deployment_role,
        )
        
        glue_role: iam.Role = iam.Role(
            self,
            f"{self._resource_prefix}-glue-stageb-job-role",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole")],
        )
        
        iam.ManagedPolicy(
            self,
            f"{self._resource_prefix}-glue-job-policy",
            roles=[glue_role],
            document=iam.PolicyDocument(
                statements=[
                    iam.PolicyStatement(
                        effect=iam.Effect.ALLOW,
                        actions=[
                            "kms:CreateGrant",
                            "kms:Decrypt",
                            "kms:DescribeKey",
                            "kms:Encrypt",
                            "kms:GenerateDataKey",
                            "kms:GenerateDataKeyPair",
                            "kms:GenerateDataKeyPairWithoutPlaintext",
                            "kms:GenerateDataKeyWithoutPlaintext",
                            "kms:ReEncryptTo",
                            "kms:ReEncryptFrom"
                        ],
                        resources=[f"arn:aws:kms:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:key/*"],
                        conditions={"ForAnyValue:StringLike": {"kms:ResourceAliases": f"alias/{self._resource_prefix}-*-key"}},
                    ),
                    iam.PolicyStatement(
                        effect=iam.Effect.ALLOW,
                        actions=["s3:ListBucket"],
                        resources=[f"arn:aws:s3:::{self._resource_prefix}-{self._environment_id}-{cdk.Aws.REGION}-{cdk.Aws.ACCOUNT_ID}-*"],
                    ),
                    iam.PolicyStatement(
                        effect=iam.Effect.ALLOW,
                        actions=[
                            "s3:GetObject",
                            "s3:PutObject",
                            "s3:DeleteObject",
                        ],
                        resources=[f"arn:aws:s3:::{self._resource_prefix}-{self._environment_id}-{cdk.Aws.REGION}-{cdk.Aws.ACCOUNT_ID}-*"],
                    ),
                    iam.PolicyStatement(
                        effect=iam.Effect.ALLOW,
                        actions=[
                            "lakeformation:DeregisterResource",
                            "lakeformation:GetDataAccess",
                            "lakeformation:GrantPermissions",
                            "lakeformation:PutDataLakeSettings",
                            "lakeformation:GetDataLakeSettings",
                            "lakeformation:RegisterResource",
                            "lakeformation:RevokePermissions",
                            "lakeformation:UpdateResource",
                            "glue:CreateDatabase",
                            "glue:CreateJob",
                            "glue:CreateSecurityConfiguration",
                            "glue:DeleteDatabase",
                            "glue:DeleteJob",
                            "glue:DeleteSecurityConfiguration",
                            "glue:GetDatabase",
                            "glue:GetDatabases",
                            "glue:GetMapping",
                            "glue:GetPartition",
                            "glue:GetPartitions",
                            "glue:GetPartitionIndexes",
                            "glue:GetSchema",
                            "glue:GetSchemaByDefinition",
                            "glue:GetSchemaVersion",
                            "glue:GetSchemaVersionsDiff",
                            "glue:GetTable",
                            "glue:GetTables",
                            "glue:GetTableVersion",
                            "glue:GetTableVersions",
                            "glue:GetTags",
                            "glue:PutDataCatalogEncryptionSettings",
                            "glue:SearchTables",
                            "glue:TagResource",
                            "glue:UntagResource",
                            "glue:UpdateDatabase",
                            "glue:UpdateJob",
                            "glue:ListSchemas",
                            "glue:ListSchemaVersions"
                        ],
                        resources=["*"],
                    ),
                    iam.PolicyStatement(
                        effect=iam.Effect.ALLOW,
                        actions=["dynamodb:GetItem"],
                        resources=[
                            f"arn:aws:dynamodb:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:table/{self._resource_prefix}-{self._environment_id}-*",
                            f"arn:aws:dynamodb:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:table/octagon-*"
                        ],
                    ),
                ]
            ),
        )
        return glue_role


    @property
    def raw_bucket(self) -> s3.IBucket:
        return self._raw_bucket
    
    @property
    def raw_bucket_key(self) -> kms.IKey:
        return self._raw_bucket_key

    @property
    def stage_bucket(self) -> s3.IBucket:
        return self._stage_bucket

    @property
    def stage_bucket_key(self) -> kms.IKey:
        return self._stage_bucket_key

    @property
    def artifacts_bucket(self) -> s3.IBucket:
        return self._artifacts_bucket

    @property
    def artifacts_bucket_key(self) -> kms.IKey:
        return self._artifacts_bucket_key

    @property
    def glue_role(self) -> s3.IBucket:
        return self._glue_role

    @property
    def routing_function(self) -> lmbda.IFunction:
        return self._routing_function

    @property
    def register_provider(self) -> Provider:
        return self._register_provider

    @property
    def data_lake_library(self) -> lmbda.ILayerVersion:
        return self._data_lake_library
