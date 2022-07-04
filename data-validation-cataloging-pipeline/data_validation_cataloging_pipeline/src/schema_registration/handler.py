import logging
import os
from typing import Any, Dict
import awswrangler as wr

import boto3

# Create logger for logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

glue_client = boto3.client("glue")
dynamodb = boto3.resource("dynamodb")
ENV = os.environ["ENV"]
PREFIX = os.environ["PREFIX"]

# Schema registration Process
def lambda_handler(event: Dict[str, Any], context: Any) -> None:
    try:
        logger.info("Schema Registration: lambda_handler() has been invoked")

        # event_ = event
        event = event["body"]
        dataset = event["dataset"]
        source = event["source"]
        schema_name = event["schema_name"]
        bucket_name = event["target_bucket"]
        glue_db_name = f"{PREFIX}-validation-cataloging-data-catalog-{ENV}"
        glue_table_name = source.upper() + "_" + dataset.upper()

        columns_types, partitions_types, partitions_values = wr.s3.store_parquet_metadata(
                path=f's3://{bucket_name}/{source}/{schema_name}/{dataset}/',
                database=glue_db_name,
                table=glue_table_name,
                dataset=True
            )
        

    except Exception as e:
        raise e
        
    return
