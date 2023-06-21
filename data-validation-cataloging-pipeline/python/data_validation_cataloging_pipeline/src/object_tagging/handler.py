import os
from typing import Any, Dict, List, Optional

import boto3

client = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")
s3_client = boto3.client("s3")

DIDC_TABLE = os.environ["DIDC"]
DIDC_TABLE_NAME = DIDC_TABLE.split("/")[-1]


def get_file_list(bucket: str, prefix: str) -> List[str]:
    try:
        print(f"bucket: {bucket}\n")
        print(f"perfix: {prefix}\n")
        # Get list of files to remove from the prefix
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        print(response)
        return [key["Key"] for key in response["Contents"] if "$folder$" not in key["Key"]]
    except Exception as e:
        print(f"Exception: {e}")
        raise e


# Tagging Process
def lambda_handler(event: Dict[str, Any], context: Optional[Dict[str, Any]]) -> Any:
    try:
        # event_ = event
        event = event["body"]
        source = event["source"]
        schema = event["schema_name"]
        dataset = event["dataset"]
        key = event["keysToProcess"]
        keysToProcess = event["keysToProcess"]
        bucket = event["target_bucket"]

        source_date = (keysToProcess[0].split("/")[-3]).split("=")[-1]
        systemtimestamp = event["systemtimestamp"]
        source_prefix = f"data/{source}/{schema}/{dataset}/SOURCEDATE={source_date}/SYSTEMTIMESTAMP={systemtimestamp}"

        keys = get_file_list(bucket, source_prefix)
        print("keys:{}".format(keys))

        # Query Dynamodb to get Table name
        table = dynamodb.Table(DIDC_TABLE_NAME)
        response = table.query(
            KeyConditionExpression=boto3.dynamodb.conditions.Key("TableName").eq(dataset.upper()),
            FilterExpression=boto3.dynamodb.conditions.Attr("TableStatus").eq("Enterprise Approved"),
        )
        data = response["Items"]

        TagSetDetails = []
        for key, val in data[0].items():
            print(key)
            print(type(val))
            if type(val) != dict:
                TagsetValue = {"Key": key, "Value": val}
                TagSetDetails.append(TagsetValue)

        print(TagSetDetails)
        for i in keys:
            print(i)
            client.put_object_tagging(Bucket=bucket, Key=i, Tagging={"TagSet": TagSetDetails})

        response = {"bucket": bucket, "prefix": "/".join(keys[0].split("/")[:-1])}
        print(response)

    except Exception as e:
        raise e

    return event
