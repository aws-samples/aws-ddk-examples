DIRNAME=$(pwd)
DATE=$(date +"%Y-%m-%d")
PROFILE='default'
REGION='us-east-1'
BUCKET_NAME='ENTER_SDLF_BUCKET_NAME'

aws s3 cp "${DIRNAME}"/examples/legislators/memberships.json \
      s3://${BUCKET_NAME}/demoteam/legislators/memberships.json \
      --profile ${PROFILE} --region ${REGION}

aws s3 cp "${DIRNAME}"/examples/legislators/persons.json \
      s3://${BUCKET_NAME}/demoteam/legislators/persons.json \
      --profile ${PROFILE} --region ${REGION}

aws s3 cp "${DIRNAME}"/examples/legislators/regions.json \
      s3://${BUCKET_NAME}/demoteam/legislators/regions.json \
      --profile ${PROFILE} --region ${REGION}

aws s3 cp "${DIRNAME}"/examples/legislators/organizations.json \
      s3://${BUCKET_NAME}/demoteam/legislators/organizations.json \
      --profile ${PROFILE} --region ${REGION}