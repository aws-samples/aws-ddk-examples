DIRNAME=$(pwd)
DATE=$(date +"%Y-%m-%d")
BUCKET_NAME="${1}"
REGION="${2:-us-east-1}"
PROFILE="${3:-default}"
DATASET='legislators'
TEAM='demoteam'

aws s3 cp "${DIRNAME}"/examples/legislators/memberships.json \
      s3://${BUCKET_NAME}/${TEAM}/${DATASET}/memberships.json \
      --profile ${PROFILE} --region ${REGION}

aws s3 cp "${DIRNAME}"/examples/legislators/persons.json \
      s3://${BUCKET_NAME}/${TEAM}/${DATASET}/persons.json \
      --profile ${PROFILE} --region ${REGION}

aws s3 cp "${DIRNAME}"/examples/legislators/regions.json \
      s3://${BUCKET_NAME}/${TEAM}/${DATASET}/regions.json \
      --profile ${PROFILE} --region ${REGION}

aws s3 cp "${DIRNAME}"/examples/legislators/organizations.json \
      s3://${BUCKET_NAME}/${TEAM}/${DATASET}/organizations.json \
      --profile ${PROFILE} --region ${REGION}