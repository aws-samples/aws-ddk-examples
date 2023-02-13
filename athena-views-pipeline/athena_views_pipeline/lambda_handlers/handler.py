import json
import os
import boto3

DDB_TABLE = os.environ["DDB_TABLE"]
ddb_client = boto3.client("dynamodb")

def lambda_handler(event, context):
    for record in event["Records"]:
        body = json.loads(record["body"])
        sm_input = json.loads(body["detail"]["input"])
        
        db = sm_input["db"]
        view_name = sm_input["view"]
        query = sm_input["query"]
        execution_arn = body["detail"]["executionArn"]
        fail_time = body["time"]
        error_key = body["detail"]["error"]
        error_cause = body["detail"]["cause"]
        
        resp = ddb_client.get_item(
            TableName=DDB_TABLE,
            Key={
                'view_name': {'S': view_name},
                'db': {'S': db}
            }
        )
        
        if "Item" in resp:
            update_response = ddb_client.update_item(
                TableName=DDB_TABLE,
                Key={
                    'view_name': {'S': view_name},
                    'db': {'S': db}
                },
                UpdateExpression="set last_sm_execution_arn=:s, last_fail_time=:f, failure_count=:c, error_key=:d, error_cause=:g, error_json=:e",
                ExpressionAttributeValues={
                    ':s': {'S': execution_arn},
                    ':f': {'S': fail_time},
                    ':c': {'N': f"{int(resp['Item']['failure_count']['N']) + 1}"},
                    ':e': {'S': json.dumps(body)},
                    ':d': {'S': error_key},
                    ':g': {'S': error_cause}
                },
                ReturnValues="UPDATED_NEW"
            )
        else:
            update_response = ddb_client.put_item(
                TableName=DDB_TABLE,
                Item={
                    'view_name': {'S': view_name},
                    'db': {'S': db},
                    'last_sm_execution_arn': {'S': execution_arn},
                    'last_fail_time': {'S': fail_time},
                    'failure_count': {'N': '1'},
                    'error_json': {'S':  json.dumps(body)},
                    'query': {'S': query},
                    'error_key': {'S': error_key},
                    'error_cause': {'S': error_cause}
                },
            )

        
    return event
