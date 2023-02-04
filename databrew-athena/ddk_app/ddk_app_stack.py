from typing import Any

from aws_ddk_core.base import BaseStack
from aws_ddk_core.resources import S3Factory, DataBrewFactory
from constructs import Construct
from aws_cdk.aws_s3 import Bucket, BucketAccessControl
from aws_cdk.aws_s3_deployment import BucketDeployment, Source
from aws_cdk.aws_iam import Effect, PolicyStatement, ServicePrincipal

from aws_cdk.aws_databrew import CfnDataset, CfnRecipe




class DdkApplicationStack(BaseStack):

    def __init__(self, scope: Construct, id: str, environment_id: str, **kwargs: Any) -> None:
        super().__init__(scope, id, environment_id, **kwargs)

        input_bucket = self._create_s3_bucket(environment_id = environment_id, 
                                            bucket_id = 'databrew-pipeline-input-bucket')

        self._upload_data_to_buckets(input_bucket)

        output_bucket = self._create_s3_bucket(environment_id = environment_id, 
                                            bucket_id = 'databrew-pipeline-output-bucket')
        
        self._create_databrew_environment(input_bucket, output_bucket)




        
    def _create_s3_bucket(self, environment_id: str, bucket_id: str) -> Bucket:
        
        s3_bucket = S3Factory.bucket(
            self,
            id=bucket_id,
            environment_id=environment_id,
            access_control=BucketAccessControl.BUCKET_OWNER_FULL_CONTROL,
            event_bridge_enabled=True
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
            ))

        return s3_bucket

    def _upload_data_to_buckets(self, input_bucket: Bucket) -> None:
        
        sales_file_deployment : BucketDeployment = BucketDeployment(
                self,
                'sales-data-deployment',
                sources = [Source.asset('./data/sales_pipeline')],
                destination_bucket = input_bucket,
                destination_key_prefix = 'sales'
                )

        marketing_file_deployment : BucketDeployment = BucketDeployment(
                self,
                'marketing-data-deployment',
                sources = [Source.asset('./data/marketing')],
                destination_bucket = input_bucket,
                destination_key_prefix = 'marketing'
                )
    
    def _create_databrew_environment(self,input_bucket : Bucket, output_bucket : Bucket) -> None:

        # defining the Dataset properties for the Databrew Job
        marketing_dataset_prop = CfnDataset.InputProperty(
            s3_input_definition=CfnDataset.S3LocationProperty(
                bucket=input_bucket.bucket_name,
                key="marketing/marketing_data.csv",
            )
        )

        # defining the Dataset properties for the Databrew Job
        sales_dataset_prop = CfnDataset.InputProperty(
            s3_input_definition=CfnDataset.S3LocationProperty(
                bucket=input_bucket.bucket_name,
                key="sales/sales_pipeline_data.csv",
            )
        )

        # creating the GlueDatabrew input dataset
        marketing_dataset = CfnDataset(
            self, 
            "marketing-dataset", 
            input = marketing_dataset_prop, 
            name = "marketing-dataset", 
            format="CSV")
        
        # creating the GlueDatabrew input dataset
        sales_dataset = CfnDataset(
            self, 
            "sales-dataset", 
            input = sales_dataset_prop, 
            name = "sales-dataset", 
            format="CSV")


        # defining the set of transformations withiin the Glue databrew job
        databrew_actions = [
            CfnRecipe.RecipeStepProperty(
                action = CfnRecipe.ActionProperty(
                    operation="YEAR",
                    parameters={"sourceColumn": "Date", "targetColumn": "year"},
                ),
                condition_expressions=None,
            ),
            CfnRecipe.RecipeStepProperty(
                action = CfnRecipe.ActionProperty(
                    operation="MONTH",
                    parameters={"sourceColumn": "Date", "targetColumn": "month"},
                ),
                condition_expressions=None,
            ),
            CfnRecipe.RecipeStepProperty(
                action = CfnRecipe.ActionProperty(
                    operation="DAY",
                    parameters={"sourceColumn": "Date", "targetColumn": "day"},
                ),
                condition_expressions=None,
            ),
        ]

        # creating the GlueDatabrew Recipe
        databrew_job_recipe: CfnRecipe = CfnRecipe(
            self,
            "databrew-recipe",
            name = "databrew-job-recipe",
            steps = databrew_actions,
        )

        












































            





            
        
    


        
