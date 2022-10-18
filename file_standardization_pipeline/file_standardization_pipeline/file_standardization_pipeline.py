from typing import Any
from aws_ddk_core.base import BaseStack
from aws_ddk_core.resources import S3Factory, GlueFactory
from aws_ddk_core.stages import S3EventStage, SqsToLambdaStage, GlueTransformStage
from aws_ddk_core.pipelines import DataPipeline
from aws_cdk.aws_lambda import Code as lambda_code
from aws_cdk.aws_s3_deployment import BucketDeployment, Source
from aws_cdk.aws_glue_alpha import JobExecutable, GlueVersion, PythonVersion, Job
from aws_cdk.aws_glue_alpha import Code as glue_code
from aws_cdk.aws_iam import Role, ServicePrincipal, ManagedPolicy, PolicyStatement, Effect
from aws_cdk.aws_glue import CfnCrawler, CfnDatabase
from constructs import Construct

class FileStandardizationPipelineStack(BaseStack):

    def __init__(self, scope: Construct, id: str, environment_id: str, **kwargs: Any) -> None:
        super().__init__(scope, id, environment_id, **kwargs)

        self._environment_id = environment_id

        self._ddk_bucket = self._create_bucket(
            environment_id=environment_id,
            bucket_name=f"ddk-{self._environment_id}-filestandardization-bucket-{self.account}",
            event_bridge_status=True
        )


        self._s3_event_stage = S3EventStage(
            self,
            id="s3_event_stage",
            environment_id=self._environment_id,
            event_names=['Object Created'],
            bucket_name=self._ddk_bucket.bucket_name,
            key_prefix='input_files/'
        )


        self._glue_transform_stage = self._create_glue_transform_stage(
            database_name="ddk_pattern_database",
            crawler_role_name="ddk_pattern_glue_crawler_role"
        )


        self._sqs_to_lambda_stage = self._create_sqs_to_lambda_stage()


        self._ddk_data_pipeline =  (
            DataPipeline(
                self, 
                id="file-standardization-pipeline", 
                name="file-standardization-pipeline", 
                description="file standardization pipeline using aws-ddk", 
            )
            .add_stage(self._s3_event_stage)
            .add_stage(self._sqs_to_lambda_stage)
            .add_stage(self._glue_transform_stage, skip_rule=True)
        )

    
    def _create_bucket(self, environment_id, bucket_name, event_bridge_status):
        
        s3_bucket = S3Factory.bucket(
            self,
            id="s3_bucket",
            environment_id=environment_id,
            bucket_name=bucket_name,
            event_bridge_enabled=event_bridge_status
        )

        s3_bucket.add_to_resource_policy(
            PolicyStatement(
                sid="AllowGlueActions",
                effect=Effect.ALLOW,
                principals=[ServicePrincipal(service="glue.amazonaws.com")],
                actions=[
                    "s3:Put*",
                    "s3:Get*",
                    "s3:AbortMultipartUpload",
                    "s3:ListMultipartUploadParts",
                    "s3:ListBucketMultipartUploads",
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


    def _create_glue_transform_stage(self, database_name, crawler_role_name):

        upload_glue_script = BucketDeployment(
            self,
            id="upload_glue_script",
            destination_bucket=self._ddk_bucket,
            destination_key_prefix="glue",
            sources=[Source.asset("./file_standardization_pipeline/src/file_standardization")]
        )

        glue_job_details = JobExecutable.python_etl(
            glue_version=GlueVersion.V3_0,
            python_version=PythonVersion.THREE,
            script=glue_code.from_bucket(self._ddk_bucket, "glue/glue_script.py")
        )

        glue_job = GlueFactory.job(
            self,
            id="glue_job",
            environment_id=self._environment_id,
            job_name="ddk_pattern_glue_job",
            executable=glue_job_details,
            max_concurrent_runs=100
        )

        glue_database = CfnDatabase(
            self,
            id="glue_database",
            catalog_id=self.account,
            database_input=CfnDatabase.DatabaseInputProperty(
                description="glue database created by ddk",
                name=database_name
            )
        )

        glue_crawler_role = Role(
            self,
            id="glue_crawler_role",
            role_name=crawler_role_name,
            assumed_by=ServicePrincipal('glue.amazonaws.com'),
            managed_policies=[
                ManagedPolicy.from_aws_managed_policy_name('service-role/AWSGlueServiceRole'),
                ManagedPolicy.from_aws_managed_policy_name('AmazonS3FullAccess')
                ]
        )

        glue_transform_stage = GlueTransformStage(
            self,
            id="glue_transform_stage",
            environment_id=self._environment_id,
            job_name=glue_job.job_name,
            job_args= {
                "--additional-python-modules": "pyarrow==3,awswrangler",
                "--input_s3_path.$": "$.input_s3_path",
                "--target_s3_path.$": "$.target_s3_path"
                },
            database_name=database_name,
            targets=CfnCrawler.TargetsProperty(
                s3_targets=[CfnCrawler.S3TargetProperty(path=f"s3://{self._ddk_bucket.bucket_name}/output")]
            ),
            crawler_role=glue_crawler_role
        )

        glue_transform_stage.state_machine.add_to_role_policy(
            PolicyStatement(
                effect=Effect.ALLOW,
                actions=[
                    "glue:StartCrawler"
                ],
                resources=[f"arn:aws:glue:*:*:crawler/{glue_transform_stage.crawler.ref}"],
            )
        )

        self._ddk_bucket.grant_read_write(glue_job)

        return glue_transform_stage


    def _create_sqs_to_lambda_stage(self):

        sqs_to_lambda_stage = SqsToLambdaStage(
            self,
            id="lambda_to_sqs_stage",
            environment_id=self._environment_id,
            code=lambda_code.from_asset("./file_standardization_pipeline/src/invoke_step_function"), 
            handler="handler.lambda_handler"
        )

        sqs_to_lambda_stage.function.add_environment("STEPFUNCTION", self._glue_transform_stage.state_machine.state_machine_arn)

        sqs_to_lambda_stage.function.add_to_role_policy(
            PolicyStatement(
                effect=Effect.ALLOW,
                actions=[
                    "states:*"
                ],
                resources=[self._glue_transform_stage.state_machine.state_machine_arn],
            )
        )

        return sqs_to_lambda_stage
