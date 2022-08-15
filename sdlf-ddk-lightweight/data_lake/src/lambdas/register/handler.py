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


import logging
import os
from typing import Any, Dict, Optional, Union

import boto3
from botocore.exceptions import ClientError

_logger = logging.getLogger()
_logger.setLevel(logging.INFO)
logging.basicConfig(
    format="%(levelname)s %(threadName)s [%(filename)s:%(lineno)d] %(message)s",
    datefmt="%Y-%m-%d:%H:%M:%S",
    level=logging.INFO,
)

try:
    _logger.info("Container initialization completed")
except Exception as e:
    _logger.error(e, exc_info=True)
    init_failed = e


def put_item(table_name: str, item: Dict[str, Union[str, int]]) -> None:
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(table_name)
    try:
        table.put_item(Item=item)
    except ClientError as e:
        raise e


def delete_item(table_name: str, key: Dict[str, str]) -> None:
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(table_name)
    try:
        table.delete_item(Key=key)
    except ClientError as e:
        raise e


def on_create(event: Dict[str, Any], table_name: str, props: Dict[str, Any]) -> Dict[str, str]:
    physical_id = f"{props['id']}-ddb-item"
    put_item(table_name=table_name, item=props)
    _logger.info(f"Create resource {physical_id} with props {props}")
    return {"PhysicalResourceId": physical_id}


def on_update(event: Dict[str, Any], table_name: str, props: Dict[str, Any]) -> Dict[str, str]:
    physical_id = event["PhysicalResourceId"]
    put_item(table_name=table_name, item=props)
    _logger.info(f"Update resource {physical_id} with props {props}")
    return {"PhysicalResourceId": physical_id}


def on_delete(event: Dict[str, Any], table_name: str, props: Dict[str, Any]) -> Dict[str, str]:
    physical_id = event["PhysicalResourceId"]
    if "octagon-Pipelines" in table_name or "octagon-Datasets" in table_name:
        delete_item(table_name=table_name, key={"name": props["name"]})
        _logger.info(f"Delete resource {physical_id} with props {props}") 
    else:
        delete_item(table_name=table_name, key={"id": props["id"]})
        _logger.info(f"Delete resource {physical_id} with props {props}")
    return {"PhysicalResourceId": physical_id}


def on_event(event: Dict[str, Any], context: Optional[Dict[str, Any]]) -> Dict[str, str]:
    _logger.info(f"Received event: {event}")
    request_type = event["RequestType"]
    props = event["ResourceProperties"]["RegisterProperties"]
    table_name = os.environ[f"{props['type'].upper()}_TABLE_NAME"]
    del props["type"]

    if("octagon" in table_name):
        props['version'] = int(props['version'])
        if("Datasets" in table_name):
            props['min_items_process']['stage_c'] = int(props['min_items_process']['stage_c'])
            props['min_items_process']['stage_b'] = int(props['min_items_process']['stage_b'])
            props['max_items_process']['stage_c'] = int(props['max_items_process']['stage_c'])
            props['max_items_process']['stage_b'] = int(props['max_items_process']['stage_b'])
            
    if request_type == "Create":
        return on_create(event=event, table_name=table_name, props=props)
    elif request_type == "Update":
        return on_update(event=event, table_name=table_name, props=props)
    elif request_type == "Delete":
        return on_delete(event=event, table_name=table_name, props=props)
    raise Exception("Invalid request type: %s" % request_type)
