from typing import Any

from aws_cdk import CustomResource, Duration
from aws_cdk import aws_databrew as databrew
from aws_cdk import aws_events as events
from aws_cdk import aws_lambda as _lambda
from aws_cdk import aws_s3 as s3
from aws_cdk import custom_resources as cr
from aws_cdk.aws_glue_alpha import Database
from aws_cdk.aws_iam import (
    Effect,
    ManagedPolicy,
    PolicyDocument,
    PolicyStatement,
    Role,
    ServicePrincipal,
)
from aws_cdk.aws_s3 import Bucket, BucketAccessControl
from aws_cdk.aws_s3_deployment import BucketDeployment, Source
from aws_ddk_core import AthenaSQLStage, BaseStack, DataBrewTransformStage, DataPipeline
from constructs import Construct


class DataBrewAthenaStack(BaseStack):
    def __init__(
        self, scope: Construct, id: str, environment_id: str, **kwargs: Any
    ) -> None:
        super().__init__(scope, id, environment_id=environment_id, **kwargs)

        input_bucket = self._create_s3_bucket(
            bucket_id="databrew-pipeline-input-bucket"
        )

        marketing_bucket_deployment = self._upload_data_to_buckets(input_bucket)

        output_bucket = self._create_s3_bucket(
            bucket_id="databrew-pipeline-output-bucket"
        )

        marketing_job = self._create_databrew_environment(input_bucket, output_bucket)

        marketing_job.node.add_dependency(marketing_bucket_deployment)
        marketing_job.node.add_dependency(output_bucket)

        marketing_database = self._create_database(database_name="marketing_data")

        self._create_pipeline(
            marketing_job, output_bucket, marketing_database
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

    def _get_athena_results_iam_policy(self, bucket_name: str) -> PolicyStatement:
        return PolicyStatement(
            effect=Effect.ALLOW,
            actions=[
                "s3:ListBucket",
                "s3:GetObject",
                "s3:GetObjectLocation",
                "s3:ListBucketMultipartUploads",
                "s3:AbortMultipartUpload",
                "s3:PutObject",
                "s3:ListMultipartUploadParts",
            ],
            resources=[f"arn:aws:s3:::{bucket_name}", f"arn:aws:s3:::{bucket_name}/*"],
        )

    def _create_database(self, database_name: str) -> Database:
        return Database(
            self,
            id=database_name,
            database_name=database_name,
        )

    def _create_s3_bucket(self, bucket_id: str) -> Bucket:
        s3_bucket = s3.Bucket(
            self,
            id=bucket_id,
            access_control=BucketAccessControl.BUCKET_OWNER_FULL_CONTROL,
            event_bridge_enabled=True,
        )

        s3_bucket.add_to_resource_policy(
            PolicyStatement(
                sid="AllowDataBrewJobActions",
                effect=Effect.ALLOW,
                principals=[ServicePrincipal(service="databrew.amazonaws.com")],
                actions=[
                    "s3:PutObject",
                    "s3:AbortMultipartUpload",
                    "s3:ListMultipartUploadParts",
                    "s3:ListBucketMultipartUploads",
                    "s3:GetBucketAcl",
                    "s3:PutObjectAcl",
                ],
                resources=[
                    s3_bucket.bucket_arn,
                    f"{s3_bucket.bucket_arn}/*",
                ],
                conditions={
                    "StringEquals": {
                        "aws:SourceAccount": self.account,
                    }
                },
            )
        )

        return s3_bucket

    def _upload_data_to_buckets(self, input_bucket: Bucket) -> BucketDeployment:
        marketing_file_deployment: BucketDeployment = BucketDeployment(
            self,
            "marketing-data-deployment",
            sources=[Source.asset("./data/marketing")],
            destination_bucket=input_bucket,
            destination_key_prefix="marketing",
        )
        return marketing_file_deployment

    def _create_databrew_environment(
        self, input_bucket: Bucket, output_bucket: Bucket
    ) -> databrew.CfnJob:
        input_bucket_arn: str = input_bucket.bucket_arn
        output_bucket_arn: str = output_bucket.bucket_arn

        partition_column_names = ["year", "month", "day"]

        # defining the Dataset properties for the Databrew Job
        marketing_dataset_prop = databrew.CfnDataset.InputProperty(
            s3_input_definition=databrew.CfnDataset.S3LocationProperty(
                bucket=input_bucket.bucket_name,
                key="marketing/marketing_data.csv",
            )
        )

        # creating the GlueDatabrew marketing input dataset
        marketing_dataset = databrew.CfnDataset(
            self,
            "marketing-dataset",
            input=marketing_dataset_prop,
            name="marketing-dataset",
            format="CSV",
        )

        # defining the set of transformations withiin the Glue databrew job
        databrew_actions = [
            databrew.CfnRecipe.RecipeStepProperty(
                action=databrew.CfnRecipe.ActionProperty(
                    operation="YEAR",
                    parameters={"sourceColumn": "Date", "targetColumn": "year"},
                ),
                condition_expressions=None,
            ),
            databrew.CfnRecipe.RecipeStepProperty(
                action=databrew.CfnRecipe.ActionProperty(
                    operation="MONTH",
                    parameters={"sourceColumn": "Date", "targetColumn": "month"},
                ),
                condition_expressions=None,
            ),
            databrew.CfnRecipe.RecipeStepProperty(
                action=databrew.CfnRecipe.ActionProperty(
                    operation="DAY",
                    parameters={"sourceColumn": "Date", "targetColumn": "day"},
                ),
                condition_expressions=None,
            ),
        ]

        # creating the GlueDatabrew Recipe
        databrew_job_recipe: databrew.CfnRecipe = databrew.CfnRecipe(
            self,
            "databrew-recipe",
            name="databrew-job-recipe",
            steps=databrew_actions,
        )

        # creating the lambda custom resource for publishing recipe
        recipe_publisher_function: _lambda.IFunction = _lambda.Function(
            self,
            "recipe-publisher-function",
            code=_lambda.Code.from_asset("ddk_app/lambda_handlers"),
            handler="handler.handler",
            runtime=_lambda.Runtime.PYTHON_3_9,
        )

        if recipe_publisher_function.role:
            recipe_publisher_function.role.add_to_policy(
                PolicyStatement(
                    actions=["databrew:*Recipe", "databrew:*RecipeVersion"],
                    resources=["*"],
                )
            )

        custom_resource_provider = cr.Provider(
            self,
            "custom-resource-provider",
            on_event_handler=recipe_publisher_function,
        )

        # Define the custom resource and associate it with the Lambda functions
        custom_resource = CustomResource(
            self,
            "custom-resource",
            service_token=custom_resource_provider.service_token,
            properties={"RecipeName": databrew_job_recipe.name},
        )

        # retriving the version number from the lambda response
        databrew_job_recipe_version_number = custom_resource.get_att_string(
            "RecipeVersion"
        )

        # creating a servcie role for the databrew job
        databrew_job_role: Role = Role(
            self,
            "gdatabrew-job-role",
            assumed_by=ServicePrincipal("databrew.amazonaws.com"),
            managed_policies=[
                ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSGlueDataBrewServiceRole"
                ),
            ],
        )

        # adding the required IAM policies to the servcie role
        ManagedPolicy(
            self,
            "databrew-env-policy",
            roles=[databrew_job_role],
            document=PolicyDocument(
                statements=[
                    PolicyStatement(
                        effect=Effect.ALLOW,
                        actions=[
                            "s3:GetObject",
                            "s3:PutObject",
                            "s3:ListBucket",
                            "s3:DeleteObject",
                        ],
                        resources=[
                            input_bucket_arn,
                            f"{input_bucket_arn}/*",
                            output_bucket_arn,
                            f"{output_bucket_arn}/*",
                        ],
                    ),
                    PolicyStatement(
                        effect=Effect.ALLOW,
                        actions=["s3:PutObjectAcl"],
                        resources=[f"{input_bucket_arn}/*", f"{output_bucket_arn}/*"],
                    ),
                ]
            ),
        )

        # creating the output configurations for the GlueDatabrew Jobs
        marketing_job_output_props = databrew.CfnJob.OutputProperty(
            location=databrew.CfnJob.S3LocationProperty(
                bucket=output_bucket.bucket_name, key="marketing"
            ),
            compression_format="SNAPPY",
            format="PARQUET",
            format_options=None,
            overwrite=True,
            partition_columns=partition_column_names,
        )

        # creating the Databrew job using the DataBrewFactory
        marketing_job = databrew.CfnJob(
            self,
            "marketing-job",
            name="marketing-job",
            type="RECIPE",
            role_arn=databrew_job_role.role_arn,
            dataset_name=marketing_dataset.name,
            recipe=databrew.CfnJob.RecipeProperty(
                name=databrew_job_recipe.name,
                version=databrew_job_recipe_version_number,
            ),
            outputs=[marketing_job_output_props],
        )
        marketing_job.add_depends_on(databrew_job_recipe)

        return marketing_job

    def _create_pipeline(
        self,
        marketing_job: databrew.CfnJob,
        output_bucket: Bucket,
        marketing_database: Database,
    ):
        # Creating DataBrew Stage
        databrew_stage = DataBrewTransformStage(
            self,
            id="databrew-stage",
            job_name=marketing_job.name,
        )

        # Athena Drop Stage
        athena_drop_stage = AthenaSQLStage(
            self,
            id="athena-drop-sql",
            query_string=("DROP TABLE IF EXISTS marketing_data_output ;"),
            database_name=marketing_database.database_name,
            output_location=s3.Location(
                bucket_name=output_bucket.bucket_name,
                object_key="query-results/",
            ),
            additional_role_policy_statements=[
                self._get_glue_db_iam_policy(
                    database_name=marketing_database.database_name
                ),
                self._get_athena_results_iam_policy(
                    bucket_name=output_bucket.bucket_name
                ),
            ],
        )

        athena_ddl_sql = f"""CREATE EXTERNAL TABLE `marketing_data_output`
                    (`date` string, `new_visitors_seo` int, `new_visitors_cpc` int, 
                    `new_visitors_social_media` int, `return_visitors` int, 
                    `twitter_mentions` int,   `twitter_follower_adds` int, 
                    `twitter_followers_cumulative` int, `mailing_list_adds_` int,
                    `mailing_list_cumulative` int, `website_pageviews` int, 
                    `website_visits` int, `website_unique_visits` int,   
                    `mobile_uniques` int, `tablet_uniques` int, 
                    `desktop_uniques` int, `free_sign_up` int, 
                    `paid_conversion` int, `events` string) 
                    PARTITIONED BY (`year` string, `month` string, `day` string) 
                    ROW FORMAT SERDE   
                        'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
                    STORED AS INPUTFORMAT   
                        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
                    OUTPUTFORMAT
                        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat' 
                    LOCATION  's3://{output_bucket.bucket_name}/marketing/' 
                    TBLPROPERTIES ('classification'='parquet', 'compressionType'='none', 
                            'typeOfData'='file'); """

        # Athena Create SQL Stage
        athena_create_stage = AthenaSQLStage(
            self,
            id="athena-create-sql",
            query_string=(athena_ddl_sql),
            work_group="primary",
            output_location=s3.Location(
                bucket_name=output_bucket.bucket_name,
                object_key="query-results/",
            ),
            database_name=marketing_database.database_name,
            additional_role_policy_statements=[
                self._get_glue_db_iam_policy(
                    database_name=marketing_database.database_name
                ),
                self._get_athena_results_iam_policy(
                    bucket_name=output_bucket.bucket_name
                ),
            ],
        )

        # Load Partitions SQL Stage
        athena_parttion_stage = AthenaSQLStage(
            self,
            id="athena-partition-sql",
            query_string=("MSCK REPAIR TABLE marketing_data_output ;"),
            database_name=marketing_database.database_name,
            output_location=s3.Location(
                bucket_name=output_bucket.bucket_name,
                object_key="query-results/",
            ),
            additional_role_policy_statements=[
                self._get_glue_db_iam_policy(
                    database_name=marketing_database.database_name
                ),
                self._get_athena_results_iam_policy(
                    bucket_name=output_bucket.bucket_name
                ),
            ],
        )

        # Create data pipeline
        (
            DataPipeline(self, id="marketing-data-pipeline")
            .add_stage(
                stage=databrew_stage,
                override_rule=events.Rule(
                    self,
                    "schedule-rule",
                    schedule=events.Schedule.rate(Duration.hours(1)),
                    targets=databrew_stage.targets,
                ),
            )
            .add_stage(stage=athena_drop_stage)
            .add_stage(stage=athena_create_stage)
            .add_stage(stage=athena_parttion_stage)
        )
