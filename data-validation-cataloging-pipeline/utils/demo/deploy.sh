# Copyright (c) 2021 Amazon.com, Inc. or its affiliates
# 
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

#!/bin/bash
PROFILE=${1:-default}
REGION=${2:-$(aws configure get region --profile ${PROFILE})}
DIRNAME=$(pwd)
SOURCE=${DIRNAME}/data/
MANIFEST_SOURCE=${DIRNAME}/manifests/
RAW_BUCKET_NAME="di-dev-us-east-1-111111111111-raw"


aws s3 cp ${SOURCE} "s3://${RAW_BUCKET_NAME}/data/" --profile ${PROFILE} --recursive
echo "Copied files to S3"



aws s3 cp ${MANIFEST_SOURCE} "s3://${RAW_BUCKET_NAME}/manifests/" --profile ${PROFILE} --recursive
echo "Copied manifest file to S3"
