# Copyright 2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License").
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import logging
import os
import uuid
from datetime import datetime
from urllib.parse import unquote_plus

import boto3
from boto3.dynamodb.conditions import Attr, Key
from botocore.config import Config
from botocore.exceptions import ClientError

session_config = Config(user_agent_extra="awsddksdlf/0.1.0")

logger = logging.getLogger()
logger.setLevel(logging.INFO)
sqs = boto3.resource("sqs", config=session_config)
ssm = boto3.client("ssm", config=session_config)
dynamodb = boto3.resource("dynamodb", config=session_config)
dataset_table = dynamodb.Table("octagon-Datasets-{}".format(os.environ["ENV"]))
catalog_table = dynamodb.Table(f"octagon-ObjectMetadata-{os.environ['ENV']}")
prefix = os.environ["PREFIX"]


def parse_s3_event(s3_event):
    return {
        "bucket": s3_event["detail"]["bucket"]["name"],
        "key": s3_event["detail"]["object"]["key"],
        "timestamp": int(round(datetime.utcnow().timestamp() * 1000, 0)),
        "last_modified_date": s3_event["time"].split(".")[0] + "+00:00",
    }


def get_item(table, team, dataset):
    try:
        response = table.get_item(Key={"name": "{}-{}".format(team, dataset)})
    except ClientError as e:
        print(e.response["Error"]["Message"])
    else:
        item = response["Item"]
        return item["pipeline"]


def delete_item(table, key):
    try:
        response = table.delete_item(Key=key)
    except ClientError as e:
        logger.error("Fatal error", exc_info=True)
        raise e
    else:
        return response


def put_item(table, item, key):
    try:
        response = table.put_item(
            Item=item,
            ConditionExpression=f"attribute_not_exists({key})",
        )
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            logger.info(e.response["Error"]["Message"])
        else:
            raise
    else:
        return response


def catalog_item(operation, message):
    try:
        logger.info(f"Performing Dynamo {operation} operation")

        if operation == "Object Deleted":
            id = "s3://{}/{}".format(message["bucket"], unquote_plus(message["key"]))
            delete_item(catalog_table, {"id": id})
        else:
            message["id"] = f"s3://{message['bucket']}/{message['key']}"
            message["stage"] = message["bucket"].split("-")[-1]
            if message["stage"] not in ["raw", "stage", "analytics"]:
                message["stage"] = "raw"
            put_item(catalog_table, message, "id")
    except ClientError as e:
        print(e.response["Error"]["Message"])
        logger.info(e.response["Error"]["Message"])
    else:
        return message


def lambda_handler(event, context):
    try:
        logger.info(f"Event: {event}, context: {context}")
        logger.info("Parsing S3 Event")
        message = parse_s3_event(event)
        message = catalog_item(event["detail-type"], message)

        if message["stage"] == "raw":
            team = message["key"].split("/")[0]
            dataset = message["key"].split("/")[1]

            logger.info(
                "team: {}; dataset: {}; bucket: {}; key: {}".format(
                    team, dataset, message["bucket"], message["key"]
                )
            )

            pipeline = get_item(dataset_table, team, dataset)

            message["team"] = team
            message["dataset"] = dataset
            message["pipeline"] = pipeline
            message["org"] = os.environ["ORG"]
            message["app"] = os.environ["APP"]
            message["env"] = os.environ["ENV"]
            message["pipeline_stage"] = "StageA"

        logger.info(
            "Sending event to {}-{} pipeline queue for processing".format(
                team, pipeline
            )
        )
        queue = sqs.get_queue_by_name(
            QueueName="{}-{}-{}-queue-a.fifo".format(prefix, team, pipeline)
        )
        queue.send_message(
            MessageBody=json.dumps(message),
            MessageGroupId="{}-{}".format(team, dataset),
            MessageDeduplicationId=str(uuid.uuid1()),
        )
    except Exception as e:
        logger.error("Fatal error", exc_info=True)
        raise e
    return
