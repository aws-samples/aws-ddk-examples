#!/bin/bash

CURR_DIR=$(pwd)

S3_BUCKET_NAME=$1
AWS_PROFILE=$2 # the aws profile you are using (ie "sandbox" or "burner")


echo "Copying input data files to S3.."
aws s3 cp $CURR_DIR/utils/test_data s3://$S3_BUCKET_NAME/input_files/ --recursive --profile $AWS_PROFILE
