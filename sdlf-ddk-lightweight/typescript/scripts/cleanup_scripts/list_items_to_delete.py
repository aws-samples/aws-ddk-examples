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
import os
import re
import sys

import boto3

env = str(sys.argv[1])
profile_name = str(sys.argv[2])

session = boto3.session.Session(profile_name=profile_name)

s3_client = session.client("s3")
dynamodb_client = session.client("dynamodb")
kms_client = session.client("kms")
sqs_client = session.client("sqs")
lambda_client = session.client("lambda")
events_client = session.client("events")
cfn_client = session.client("cloudformation")
cw_client = session.client("logs")

MAX_ITEMS = 1000


def list_s3_buckets(prefix):
    bucket_list = []
    response = s3_client.list_buckets()
    for bucket in response["Buckets"]:
        if re.match(f"{prefix}-*", bucket["Name"]) or re.match("cdk-*", bucket["Name"]):
            bucket_list.append(bucket["Name"])
    return bucket_list


def list_ddb_tables(prefix):
    table_list = []
    response = dynamodb_client.list_tables()
    for table_name in response["TableNames"]:
        if (re.match(f"{prefix}-*", table_name)) or (
            re.match(f"octagon-*", table_name)
        ):
            table_list.append(table_name)
    return table_list


def list_kms_keys(max_items, prefix):
    key_id_list = []
    try:
        # creating paginator object for list_keys() method
        paginator = kms_client.get_paginator("list_aliases")

        # creating a PageIterator from the paginator
        response_iterator = paginator.paginate(PaginationConfig={"MaxItems": max_items})

        full_result = response_iterator.build_full_result()
        for page in full_result["Aliases"]:
            if (re.match(f"alias/{prefix}-*", page["AliasName"])) or (
                re.match(f"alias/ddk-*", page["AliasName"])
            ):
                response = kms_client.describe_key(KeyId=page["TargetKeyId"])
                if response["KeyMetadata"]["KeyState"] not in [
                    "Disabled",
                    "PendingDeletion",
                    "Unavailable",
                ]:
                    key_id_list.append(page["TargetKeyId"])
    except:
        print("Could not list KMS Keys.")
        raise
    else:
        return key_id_list


def list_sqs_queues(prefix):
    queue_list = []
    response = sqs_client.list_queues(QueueNamePrefix=f"{prefix}-")
    if "QueueUrls" in response:
        queue_urls = response["QueueUrls"]
        for queue in queue_urls:
            queue_list.append(queue)
    return queue_list


def list_lambda_layers():
    layer_list = []
    response = lambda_client.list_layers()
    for layer in response["Layers"]:
        if re.match(f"data-lake-library", layer["LayerName"]) or re.match(
            "AWSDataWrangler-Python39", layer["LayerName"]
        ):
            layer_list.append(
                {
                    "layerName": layer["LayerName"],
                    "version": layer["LatestMatchingVersion"]["Version"],
                }
            )
    return layer_list


def list_rules(prefix):
    rule_list = []
    response = events_client.list_rules(NamePrefix=f"{prefix}-")
    for rule in response["Rules"]:
        rule_list.append(rule["Name"])
    return rule_list


def list_cfn_template():
    stack_list = []
    response = cfn_client.list_stacks(
        StackStatusFilter=[
            "CREATE_COMPLETE",
            "ROLLBACK_COMPLETE",
            "UPDATE_COMPLETE",
            "UPDATE_ROLLBACK_COMPLETE",
        ]
    )
    for stack in response["StackSummaries"]:
        if re.match(
            f"{prefix}-[a-zA-Z0-9_.-]*-instance-[a-zA-Z0-9_.-]*", stack["StackName"]
        ):
            stack_list.append(stack["StackName"])
        else:
            continue
    return stack_list


def list_cw_logs(prefix):
    cw_log_list = []
    try:
        # creating paginator object for describe_log_groups() method
        paginator = cw_client.get_paginator("describe_log_groups")

        # creating a PageIterator from the paginator
        response_iterator = paginator.paginate(PaginationConfig={"MaxItems": MAX_ITEMS})

        full_result = response_iterator.build_full_result()
        for page in full_result["logGroups"]:
            if re.match(f"/aws/lambda/{prefix}-*", page["logGroupName"]) or re.match(
                "/aws/lambda/SDLF-*", page["logGroupName"]
            ):
                cw_log_list.append(page["logGroupName"])
            elif re.match(
                "/aws/codebuild/codepipelineAssetsFileAsset-*", page["logGroupName"]
            ):
                cw_log_list.append(page["logGroupName"])
    except:
        print("Could not list CloudWatch Logs.")
        raise
    else:
        return cw_log_list


if __name__ == "__main__":
    try:
        # Get Resource Prefix from DDK.json
        config_path = os.path.abspath(__file__).split("/")[:-3]
        config_path = "/".join(config_path) + "/ddk.json"

        with open(config_path) as config_data:
            config = json.loads(config_data.read())
            prefix = config["environments"][env]["resource_prefix"]

        print("Collecting Resources to Delete")
        resources = {}
        bucket_list = list_s3_buckets(prefix)
        resources["s3"] = bucket_list

        ddb_table_list = list_ddb_tables(prefix)
        resources["ddb"] = ddb_table_list

        kms_key_list = list_kms_keys(MAX_ITEMS, prefix)
        resources["kms"] = kms_key_list

        sqs_list = list_sqs_queues(prefix)
        resources["sqs"] = sqs_list

        lambda_layer_list = list_lambda_layers()
        resources["lambdaLayer"] = lambda_layer_list

        rules_list = list_rules(prefix)
        resources["eventbridge"] = rules_list

        cfn_template_list = list_cfn_template()
        resources["cloudformation"] = cfn_template_list

        cw_logs_list = list_cw_logs(prefix)
        resources["cwlogs"] = cw_logs_list

        print("Writing Items to Delete to JSON File: delete_file.json")
        with open("delete_file.json", "w+") as delete_file:
            json.dump(resources, delete_file)

    except Exception as e:
        print(f"Error: {e}")
