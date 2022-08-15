from pathlib import Path
from typing import Any, Dict, List, Optional

from aws_cdk.aws_iam import Effect, PolicyStatement
from aws_cdk.aws_lambda import Code,  LayerVersion, Runtime, Function
from aws_cdk.aws_stepfunctions_tasks import LambdaInvoke
import aws_cdk.aws_stepfunctions as sfn
from aws_ddk_core.pipelines import StateMachineStage
from aws_ddk_core.resources import LambdaFactory
from constructs import Construct
import aws_cdk as cdk
import os
from aws_cdk.aws_kms import IKey, Key
from aws_cdk.aws_stepfunctions import JsonPath
from aws_cdk.aws_s3 import Bucket, IBucket
from aws_cdk.aws_ssm import StringParameter
from aws_cdk.aws_glue import CfnDatabase
from aws_cdk.aws_lakeformation import CfnPermissions

class DataValidationCatalogingStage(StateMachineStage):
    def __init__(
        self,
        scope: Construct,
        id: str,
        environment_id: str,
        raw_bucket_arn: str,
        didc_table_arn: str,
        didc_table_key_arn: str,
        state_machine_failed_executions_alarm_threshold: Optional[int] = 1,
        state_machine_failed_executions_alarm_evaluation_periods: Optional[int] = 1,
    ) -> None:

        super().__init__(scope, id)

        self._environment_id = environment_id
        self._raw_bucket_arn = raw_bucket_arn
        self._didc_table_arn = didc_table_arn
        self._didc_table_key_arn = didc_table_key_arn
        self._pipeline_id = "validation-cataloging"

        self._raw_bucket: IBucket = Bucket.from_bucket_arn(
            self,
            f"di-raw-bucket",
            bucket_arn= self._raw_bucket_arn
        )

        self._wrangler_layer = LayerVersion.from_layer_version_arn(
                    self,
                    id="layer",
                    layer_version_arn=f"arn:aws:lambda:{cdk.Aws.REGION}:336392948345:layer:AWSDataWrangler-Python39:1")
        
        self._didc_table_key: IKey = Key.from_key_arn(
            self,
            f"di-didc-table-key",
            key_arn=self._didc_table_key_arn)

        db_name = f"di-validation-cataloging-data-catalog-{self._environment_id}"
        self._database = self._create_database(database_name=db_name)

        state_machine_role_policy_statements = [
            PolicyStatement(
                        effect=Effect.ALLOW,
                        actions=[
                            "lakeformation:GetDataAccess",
                            "lakeformation:GrantPermissions",
                            "glue:*Database*",
                            "glue:*Table*",
                            "glue:*Partition*",
                            "glue:SearchTables",
                            "states:StartExecution",
                            "glue:Get*",
                            "glue:Update*",
                            "glue:Start*",

                        ],
                        resources=["*"],
                    ),
                    PolicyStatement(
                        effect=Effect.ALLOW,
                        actions=[
                            "kms:Decrypt",
                            "kms:Encrypt",
                            "kms:ReEncrypt*",
                            "kms:GenerateDataKey*"
                        ],
                        resources=["*"],
                    ),
                    PolicyStatement(
                        effect=Effect.ALLOW,
                        actions=[
                            "s3:DeleteObject*",
                            "s3:Put*",
                            "s3:Abort*",
                            "s3:GetObject*",
                            "s3:GetBucket*",
                            "s3:List*"
                        ],
                        resources=[
                            "*"
                        ],
                    ),
                    PolicyStatement(
                        effect=Effect.ALLOW,
                        actions=[
                            "kms:Encrypt",
                            "kms:ReEncrypt*",
                            "kms:GenerateDataKey*"
                        ],
                        resources=["*"],
                    )

        ]

        # Schema Validation
        schema_validation = self._create_schema_validation(prefix="di")

        # Schema Registration
        schema_registration = self._create_schema_registration(prefix="di")

        # Object Tagging
        object_tagging = self._create_object_tagging(prefix="di")

        # Catalog Tagging
        catalog_tagging = self._create_catalog_tagging(prefix="di")

        # COPY S3 DATA
        s3_map = sfn.Map(
            self,
            f"di-{self._pipeline_id}-Copy",
            input_path = "$.body",
            items_path = "$.key_copy_source",
            max_concurrency = 0,
            parameters = {
                "target_bucket.$": "$.target_bucket",
                "target_prefix.$": "$.target_prefix",
                "Bucket.$": "$.bucket",
                "s3_object.$": "$$.Map.Item.Value"
            },
            result_path = JsonPath.DISCARD
        )

        copy_s3_state_json = {
            "Type": "Task",
            "Resource": "arn:aws:states:::aws-sdk:s3:copyObject",
            "Parameters": {
                "Bucket.$": "$.target_bucket",
                "CopySource.$": "States.Format('{}/{}', $.Bucket, $.s3_object.full_path)",
                "Key.$": "States.Format('{}/{}', $.target_prefix, $.s3_object.filename)"
            },
            "End": True
        }
        
        copy_s3_iterator = sfn.CustomState(
            self, 
            f"di-{self._pipeline_id}-DataS3Copy-Iterator",
            state_json=copy_s3_state_json
        )

        s3_map.iterator(copy_s3_iterator)

        # SUCCEED STATE
        success_state = sfn.Succeed(self, "Done")

        # FAIL STATE
        fail_state = sfn.Fail(self, "Fail", error="States.ALL")

        # CREATE PARALLEL STATE DEFINITION
        parallel_state = sfn.Parallel(self, "ParallelSM")

        parallel_state.branch(schema_validation
                .next(schema_registration)
                .next(object_tagging)
                .next(catalog_tagging)
                .next(s3_map)
                )
        
        parallel_state.next(success_state)

        parallel_state.add_catch(
                fail_state,
                errors = ["States.ALL"],
                result_path = "$.error"
                )

        
        # CREATE STATE MACHINE
        self.build_state_machine(
            id=f"{id}-state-machine",
            environment_id=environment_id,
            definition=(
                parallel_state
            ),
            additional_role_policy_statements=state_machine_role_policy_statements,
            state_machine_failed_executions_alarm_threshold=state_machine_failed_executions_alarm_threshold,
            state_machine_failed_executions_alarm_evaluation_periods=state_machine_failed_executions_alarm_evaluation_periods,  # noqa
        )

        self._create_table_lakeformation_perms( 
            prefix = "di", 
            name = "catalog", 
            lambda_role_arn = self.catalog_tagging_role_arn,
            database_name = self._database.ref,
            env=self._environment_id)

            
        self._create_db_lakeformation_perms(
            prefix = "di", 
            name = "schema-registration", 
            lambda_role_arn = self.schema_registration_role_arn,
            database_name = self._database.ref,
            env = self._environment_id)
    
    @property
    def state_machine_arn(self):
        return self.state_machine.state_machine_arn
        
    @property
    def catalog_tagging_role_arn(self):
        return self._catalog_tagging_lambda.role.role_arn

    @property
    def schema_registration_role_arn(self):
        return self._schema_registration.role.role_arn

    def _create_database(self, database_name: str):
        return CfnDatabase(
                self,
                database_name,
                database_input=CfnDatabase.DatabaseInputProperty(
                name=database_name,
                location_uri=f"s3://{self._raw_bucket.bucket_name}/",
                 ),
                catalog_id=cdk.Aws.ACCOUNT_ID
            )

    def _create_schema_registration(self, prefix: str) -> LambdaInvoke: 
        self._schema_registration: Function = LambdaFactory.function(
            self,
            environment_id=self._environment_id,
            id=f"{prefix}-{self._pipeline_id}-SchemaRegistration",
            function_name=f"{prefix}-{self._pipeline_id}-SchemaRegistration",
            code=Code.from_asset(os.path.join(f"{Path(__file__).parents[1]}", "src/schema_registration")),
            handler="handler.lambda_handler",
            runtime = Runtime.PYTHON_3_9,
            environment={
                "ENV": self._environment_id,
                "PREFIX": "di"
                },
            description="validates incoming file schema with expected enterprise schema",
            timeout=cdk.Duration.minutes(15),
            layers=[self._wrangler_layer],
        )
        self._schema_registration.add_to_role_policy(
            PolicyStatement(
                effect=Effect.ALLOW,
                actions=["dynamodb:*"],
                resources=[f"arn:aws:dynamodb:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:table/*"],
            )
        )

        self._schema_registration.add_to_role_policy(
            PolicyStatement(
                effect=Effect.ALLOW,
                actions=["glue:*"],
                resources=["*"],
            )
        )
        self._didc_table_key.grant_decrypt(self._schema_registration)
        self._raw_bucket.grant_read(self._schema_registration)

        schema_registration = LambdaInvoke(
            self,
            f"{prefix}-{self._pipeline_id}-schemaRegistration-step",
            lambda_function=self._schema_registration,
            comment = "Schema Registration",
            result_path = JsonPath.DISCARD
        )

        schema_registration.add_retry(
            errors = ["States.ALL"],
            max_attempts = 2
        )

        return schema_registration

    def _create_schema_validation(self, prefix: str) -> LambdaInvoke: 
        self._schema_validation: Function = LambdaFactory.function(
            self,
            environment_id=self._environment_id,
            id=f"{prefix}-{self._pipeline_id}-SchemaValidation",
            function_name=f"{prefix}-{self._pipeline_id}-SchemaValidation",
            code=Code.from_asset(os.path.join(f"{Path(__file__).parents[1]}", "src/schema_validation")),
            handler="handler.lambda_handler",
            runtime = Runtime.PYTHON_3_9,
            environment={
                "DIDC": self._didc_table_arn
            },
            description="validates incoming file schema with expected enterprise schema",
            timeout=cdk.Duration.minutes(15),
            layers=[self._wrangler_layer],
        )
        self._schema_validation.add_to_role_policy(
            PolicyStatement(
                effect=Effect.ALLOW,
                actions=["dynamodb:*"],
                resources=[f"arn:aws:dynamodb:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:table/*"],
            )
        )
        self._didc_table_key.grant_decrypt(self._schema_validation)
        self._raw_bucket.grant_read(self._schema_validation)

        schema_validation = LambdaInvoke(
            self,
            f"{prefix}-{self._pipeline_id}-schemaValidation-step",
            lambda_function=self._schema_validation,
            comment = "Schema Validation",
            result_path = JsonPath.DISCARD
        )
        schema_validation.add_retry(
            errors = ["SCHEMA_MISMATCH_ERROR", "COUNT_NOT_MATCHED_ERROR"],
            max_attempts = 0
        )
        schema_validation.add_retry(
            errors = ["States.ALL"],
            max_attempts = 2
        )

        return schema_validation
        
    def _create_object_tagging(self, prefix: str) -> LambdaInvoke:
        self._object_tagging_lambda: Function = LambdaFactory.function(
            self,
            environment_id=self._environment_id,
            id=f"{prefix}-{self._pipeline_id}-ObjectTag",
            function_name=f"{prefix}-{self._pipeline_id}-ObjectTag",
            code=Code.from_asset(os.path.join(f"{Path(__file__).parents[1]}", "src/object_tagging")),
            handler="handler.lambda_handler",
            runtime = Runtime.PYTHON_3_9,
            environment={
                "DIDC": self._didc_table_arn,
            },
            description="Tags object with metadata in s3",
            timeout=cdk.Duration.minutes(10),
        )

        self._object_tagging_lambda.add_to_role_policy(
            PolicyStatement(
                effect=Effect.ALLOW,
                actions=["dynamodb:*"],
                resources=[f"arn:aws:dynamodb:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:table/*"],
            )
        )
        self._didc_table_key.grant_decrypt(self._object_tagging_lambda)
        self._raw_bucket.grant_read_write(self._object_tagging_lambda)

        object_tagging = LambdaInvoke(
            self,
            f"{prefix}-{self._pipeline_id}-objectTagging-step",
            lambda_function=self._object_tagging_lambda,
            comment = "Data Object Tagging",
            result_path= JsonPath.DISCARD
        )

        object_tagging.add_retry(
            errors = ["States.ALL"],
            max_attempts = 2
        )

        return object_tagging

    def _create_catalog_tagging(self, prefix: str) -> LambdaInvoke:
        self._catalog_tagging_lambda: Function = LambdaFactory.function(
            self,
            environment_id=self._environment_id,
            id=f"{prefix}-{self._pipeline_id}-CatalogTag",
            function_name=f"{prefix}-{self._pipeline_id}-CatalogTag",
            code=Code.from_asset(os.path.join(f"{Path(__file__).parents[1]}", "src/catalog_tagging")),
            handler="handler.lambda_handler",
            runtime = Runtime.PYTHON_3_9,
            environment={
                "ENV": self._environment_id, 
                "DIDC": self._didc_table_arn, 
                "PREFIX": prefix, 
            },
            description="Tags objects in glue catalog",
            timeout=cdk.Duration.minutes(10),
        )
        self._catalog_tagging_lambda.add_to_role_policy(
            PolicyStatement(
                effect=Effect.ALLOW,
                actions=["glue:*Database*", "glue:*Table*", "glue:*Partition*"],
                resources=["*"],
            )
        )
        self._catalog_tagging_lambda.add_to_role_policy(
            PolicyStatement(
                effect=Effect.ALLOW,
                actions=["dynamodb:*"],
                resources=[f"arn:aws:dynamodb:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:table/*"],
            )
        )
        self._didc_table_key.grant_decrypt(self._catalog_tagging_lambda)

        StringParameter(
            self,
            f"{prefix}-{self._pipeline_id}-catalog-lambda-role-{self._environment_id}",
            parameter_name=f"/DIF/{self._pipeline_id}/Lambda/CatalogLambdaRoleArn",
            string_value=self._catalog_tagging_lambda.role.role_arn
        )

        catalog_tagging = LambdaInvoke(
            self,
            f"{prefix}-{self._pipeline_id}-catalogTagging-step",
            lambda_function=self._catalog_tagging_lambda,
            comment = "Data Catalog Tagging",
            result_path = JsonPath.DISCARD
        )

        catalog_tagging.add_retry(
            errors = ["States.ALL"],
            max_attempts = 2
        )

        return catalog_tagging

    def _create_table_lakeformation_perms(self, prefix, name, lambda_role_arn, database_name, env):
        return CfnPermissions(
                        self,
                        f"{prefix}-{name}-lambda-database-lakeformation-permissions-{env}",
                        data_lake_principal=CfnPermissions.DataLakePrincipalProperty(
                            data_lake_principal_identifier=lambda_role_arn
                        ),
                        resource=CfnPermissions.ResourceProperty(
                            table_resource=CfnPermissions.TableResourceProperty(
                                database_name=database_name, table_wildcard={}
                            ),
                        ),
                        permissions=["SELECT", "ALTER", "INSERT", "DESCRIBE"],
                    )

        

    def _create_db_lakeformation_perms(self, prefix, name, lambda_role_arn, database_name, env):
        return CfnPermissions(
                self,
                f"{prefix}-{name}-database-lakeformation-permissions-{env}",
                data_lake_principal=CfnPermissions.DataLakePrincipalProperty(
                    data_lake_principal_identifier=lambda_role_arn
                ),
                resource=CfnPermissions.ResourceProperty(
                    database_resource=CfnPermissions.DatabaseResourceProperty(name=database_name)
                ),
                permissions=["CREATE_TABLE", "ALTER", "DROP"],
            )


   