import datetime
import json
import logging
import os
import uuid
from typing import Any, Dict
import boto3

states_client = boto3.client("stepfunctions")

logger = logging.getLogger()
logger.setLevel(logging.INFO)

STEPFUNCTION = os.environ["STEPFUNCTION"]

def lambda_handler(event: Dict[str, Any], context: Any) -> None:

    try:
        logger.info(f"lambda got event: {event}")
        component = context.function_name
        logger.info(f"{component} lambda Started")

        event_body = event['Records'][0]['body']
        event_body_dict = json.loads(event_body)

        bucket = event_body_dict['detail']['bucket']['name']
        input_key = event_body_dict['detail']['object']['key']

        input_s3_path = f"s3://{bucket}/{input_key}"

        key_without_top_folder = ''.join(input_key.split('/',1)[1]) # removes "input_files/" from path

        output_s3_key = ''.join(key_without_top_folder.split('.',1)[0]) # removes file extension from path

        target_s3_path = f"s3://{bucket}/output/{output_s3_key}.parquet"

        event = {
            "statusCode": 200,
            "input_s3_path": input_s3_path,
            "target_s3_path": target_s3_path,
            "additional-python-modules": "pyarrow==2,awswrangler"
        }

        logger.info(f"passing the following event to step function: {event}")

        states_client.start_execution(
            stateMachineArn=STEPFUNCTION,
            name=f"file-standardization-{str(uuid.uuid4())}",
            input=json.dumps(event),
        )
        
        logger.info(f"triggered the following step function: {STEPFUNCTION}")

    except Exception as e:
        logger.error(f"{component} lambda Failed with the following exception: {e}")
        raise e
