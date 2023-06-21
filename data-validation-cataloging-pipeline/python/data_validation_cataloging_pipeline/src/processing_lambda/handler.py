import datetime
import json
import logging
import os
import uuid
from typing import Any, Dict, Tuple

import boto3

states_client = boto3.client("stepfunctions")
sns = boto3.client("sns")
s3_resource = boto3.resource("s3")

s3 = boto3.client("s3")

logger = logging.getLogger()
logger.setLevel(logging.INFO)

STEPFUNCTION = os.environ["STEPFUNCTION"]

def parse_event(event: Dict[str, Any]) -> Tuple[str, str]:
    bucket = event['detail']['bucket']['name']
    key = event['detail']['object']['key']
    return bucket, key


def json_serial(obj: Any) -> Any:
    """JSON serializer for objects not serializable by default"""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError("Type %s not serializable" % type(obj))


def lambda_handler(event: Dict[str, Any], context: Any) -> None:
    try:
        logger.info("lambda got event: {}".format(event))
        component = context.function_name
        logger.info("{} lambda Started".format(component))

        for event in event['Records']:
            bucket, key = parse_event(json.loads(event['body']))
        obj = json.load(s3.get_object(Bucket=bucket, Key=key)["Body"])

        print(obj)

        for key in obj:
            dataset = key
            keysToProcess = obj[key]
            key_list = keysToProcess[0].split("/")
            source = key_list[1]
            source_date = key_list[-3].split("=")[-1]
            schema = key_list[2]
            systemtimestamp = key_list[-2].split("=")[-1]
            target_prefix = f"validated/{source}/{schema}/{dataset}/SOURCEDATE={source_date}/SYSTEMTIMESTAMP={systemtimestamp}"

            key_copy_source = []
            for key in keysToProcess:
                key_copy_source_dict = {}
                key_copy_source_dict["full_path"] = key
                key_copy_source_dict["filename"] = key.split("/")[-1]
                key_copy_source.append(key_copy_source_dict)

            event = {
                "statusCode": 200,
                "body": {
                    "bucket": bucket,
                    "keysToProcess": keysToProcess,
                    "dataset": dataset,
                    "source": source,
                    "schema_name": schema,
                    "systemtimestamp": systemtimestamp,
                    "target_bucket": bucket,
                    "target_prefix": target_prefix,
                    "key_copy_source": key_copy_source,
                },
            }

            states_client.start_execution(
                stateMachineArn=STEPFUNCTION,
                name="{}-{}-{}".format(source, dataset, str(uuid.uuid4())),
                input=json.dumps(event, default=json_serial),
            )

    except Exception as e:
        logger.info("{} lambda Failed".format(component))

        raise e
