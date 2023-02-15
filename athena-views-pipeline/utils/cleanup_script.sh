#!/bin/bash

S3_BUCKET_NAME=$1
AWS_PROFILE=${2:-default} # the aws profile you are using (ie "sandbox" or "burner")

echo "Deleting S3 objects and their versions..."
aws s3api delete-objects --bucket ${S3_BUCKET_NAME} --delete "$(aws s3api list-object-versions --bucket ${S3_BUCKET_NAME} --query='{Objects: Versions[].{Key:Key,VersionId:VersionId}}' --profile ${AWS_PROFILE})" --profile $AWS_PROFILE --output text --no-cli-pager

echo "Delete the glue DB created foo example purposes"
aws glue delete-database --name "person_db"
aws glue delete-database --name "sales_db"

echo "Deleting CloudFormation stack..."
cdk destroy --profile $AWS_PROFILE
