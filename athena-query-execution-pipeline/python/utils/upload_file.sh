CURR_DIR=$(pwd)

S3_BUCKET_NAME=$1
AWS_PROFILE=${2:-default} # the aws profile you are using (ie "sandbox" or "burner")


echo "Copying input data files to S3.."
aws s3 cp $CURR_DIR/utils/data s3://$S3_BUCKET_NAME/data/sales/ --recursive --profile $AWS_PROFILE