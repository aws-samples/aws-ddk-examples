import json
import argparse
import boto3

argParser = argparse.ArgumentParser()
argParser.add_argument("-p", "--profile", help="profile name")
argParser.add_argument("-r", "--region", help="aws region")

args = argParser.parse_args()

profile = args.profile if args.profile else "default"
region = args.region if args.region else "us-east-1"

boto3.setup_default_session(profile_name=profile, region_name=region)

ddb = boto3.resource("dynamodb")

path = "./didc_data.json"

with open(path, "r") as json_file:
    data = json.load(json_file)

for item in data:
    columns = data[item]["columns"]
    schema_name = data[item]["SchemaName"]
    source_schema_name = data[item]["Source-SchemaName"]
    source = data[item]["Source"]
    tableid = data[item]["TableId"]
    tablename = data[item]["TableName"]
    tablestatus = data[item]["TableStatus"]

    table = ddb.Table("di-didc-dev")  # Enter DIDC Table Name Created In DynamoDB Here

    try:
        response = table.put_item(
            Item={
                "columns": columns,
                "SchemaName": schema_name,
                "Source-SchemaName": source_schema_name,
                "Source": source,
                "TableId": tableid,
                "TableName": tablename,
                "TableStatus": tablestatus,
            }
        )
        if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
            print(f"error uploading table: {tablename}")
        print(response)
    except Exception as e:
        print("Issue writting to DIDC")
        raise e
