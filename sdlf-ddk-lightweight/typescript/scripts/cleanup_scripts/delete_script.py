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


import boto3
import json
import sys


profile_name = str(sys.argv[1])

session = boto3.session.Session(profile_name=profile_name)

s3_client = session.client('s3')
s3_resource = session.resource('s3')
dynamodb_client = session.client('dynamodb')
kms_client = session.client('kms')
sqs_client = session.client("sqs")
lambda_client = session.client("lambda")
events_client = session.client('events')
cfn_client = session.client('cloudformation')
cw_client = session.client('logs')

def empty_bucket(bucket_name):
    response = s3_client.list_objects_v2(Bucket=bucket_name)
    versions = s3_client.list_object_versions(Bucket=bucket_name) # list all versions in this bucket
    if 'Contents' in response:
        for item in response['Contents']:
            print('deleting file', item['Key'])
            s3_client.delete_object(Bucket=bucket_name, Key=item['Key'])
            while response['KeyCount'] == 1000:
                response = s3_client.list_objects_v2(
                Bucket=bucket_name,
                StartAfter=response['Contents'][0]['Key'],
                )
                for item in response['Contents']:
                    print('deleting file', item['Key'])
                    s3_client.delete_object(Bucket=bucket_name, Key=item['Key'])
    
    if 'Versions' in versions and len(versions['Versions'])>0:
        s3_bucket = s3_resource.Bucket(bucket_name)
        s3_bucket.object_versions.delete()

    return
    
def delete_bucket(bucket_name):
    response = s3_client.delete_bucket(Bucket=bucket_name)
    return

def delete_table(table_name):
    response = dynamodb_client.delete_table(
        TableName=table_name
    )
    return

def schedule_key_deletion(key_id):
    response = kms_client.schedule_key_deletion(
        KeyId=key_id,
        PendingWindowInDays=7
    )
    print(f"Key Deletion Scheduled: {key_id}")  
    return

def delete_queue(queue_url):
    sqs_client.delete_queue(
        QueueUrl=queue_url
    )
    return

def delete_lambda_layer(layer):
    lambda_client.delete_layer_version(
        LayerName=layer["layerName"],
        VersionNumber=layer["version"]
    )
    return

def delete_rule(rule_name):
    targets = []
    response = events_client.list_targets_by_rule(
        Rule=rule_name
    )
    for target in response["Targets"]:
        targets.append(target["Id"])

    events_client.remove_targets(
        Rule=rule_name,
        Ids=targets
    )

    events_client.delete_rule(
        Name=rule_name
    )
    return

def delete_cfn_stack(stack_name):
    cfn_client.delete_stack(
        StackName=stack_name
    )
    return

def delete_log_group(log_group):
    cw_client.delete_log_group(
        logGroupName=log_group
    )
    return 

if __name__ == "__main__":
    try: 
        with open('delete_file.json') as json_data:
            items = json.load(json_data)

            if len(items["s3"]) > 0:
                for s3_bucket in items["s3"]:
                    print(f"Emptying Content From: {s3_bucket}")
                    empty_bucket(s3_bucket)
                    print(f"Bucket: {s3_bucket} is Empty")
            
                for s3_bucket in items["s3"]:
                    print(f"Deleting Bucket: {s3_bucket}")
                    delete_bucket(s3_bucket)
                    print(f"Bucket: {s3_bucket} Deleted")

            if len(items["ddb"]) > 0:
                for ddb_table in items["ddb"]:
                    print(f"Deleting Table: {ddb_table}")
                    delete_table(ddb_table)
                    print(f"Table Name: {ddb_table} Deleted")

            if len(items["sqs"]) > 0:
                for queue_url in items["sqs"]:
                    print(f"Deleting SQS Queue: {queue_url}")
                    delete_queue(queue_url)
                    print(f"Queue Deleted: {queue_url}")
            
            if len(items["lambdaLayer"]) > 0:
                for layer in items["lambdaLayer"]:
                    print(layer)
                    print(f"Deleting Lambda Layer: {layer['layerName']} Version {layer['version']}")
                    delete_lambda_layer(layer)
                    print(f"Lambda Layer Deleted: {layer}")

            if len(items["eventbridge"]) > 0:
                for rule in items["eventbridge"]:
                    print(f"Deleting Eventbridge Rule: {rule}")
                    delete_rule(rule)
                    print(f"Eventbridge Rule Deleted: {rule}")
            
            if len(items["cloudformation"]) > 0:
                for stack in items["cloudformation"]:
                    print(f"Deleting Cloudformation Stack: {stack}")
                    delete_cfn_stack(stack)
                    print(f"Cloudformation Stack Deleted: {stack}")
            
            if len(items["cwlogs"]) > 0:
                for log_group in items["cwlogs"]:
                    print(f"Deleting Log Group: {log_group}")
                    delete_log_group(log_group)

            if len(items["kms"]) > 0:
                for key_id in items["kms"]:
                    print(f"Scheduling KMS Key Delete: {key_id}")
                    schedule_key_deletion(key_id)
            
            json_data.close()

    except Exception as e:
        print(f"Error: {e}")
        
        
        
    # IsTruncated = True
    # MaxKeys = 1000
    # KeyMarker = None
    # while IsTruncated == True:
    #     if not KeyMarker:
    #         version_list = s3_client.list_object_versions(Bucket=bucket_name,MaxKeys=MaxKeys)
    #     else:
    #         version_list = s3_client.list_object_versions(Bucket=bucket_name,MaxKeys=MaxKeys,KeyMarker=KeyMarker)
    #     try:
    #         objects = []
    #         versions = version_list['Versions']
    #         for v in versions:
    #             objects.append({'VersionId':v['VersionId'],'Key': v['Key']})
    #         response = s3_client.delete_objects(Bucket=bucket_name,Delete={'Objects':objects})
	   # except:
		  #  objects = []
    #     	delete_markers = version_list['DeleteMarkers']
    #     	for d in delete_markers:
    #             objects.append({'VersionId':d['VersionId'],'Key': d['Key']})
    #     	response = client.delete_objects(Bucket=Bucket,Delete={'Objects':objects})
	   # print(response)

    #     IsTruncated = version_list['IsTruncated']
    #     KeyMarker = version_list['NextKeyMarker']