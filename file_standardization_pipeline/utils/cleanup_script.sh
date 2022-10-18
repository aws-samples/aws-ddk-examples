#!/bin/bash

S3_BUCKET_NAME=$1
AWS_PROFILE=$2 # the aws profile you are using (ie "sandbox" or "burner")

echo "Deleting S3 objects and their versions..."
aws s3api delete-objects --bucket ${S3_BUCKET_NAME} --delete "$(aws s3api list-object-versions --bucket ${S3_BUCKET_NAME} --query='{Objects: Versions[].{Key:Key,VersionId:VersionId}}' --profile ${AWS_PROFILE})" --profile $AWS_PROFILE --output text --no-cli-pager

echo "Deleting CloudFormation stack..."
cdk destroy --profile $AWS_PROFILE