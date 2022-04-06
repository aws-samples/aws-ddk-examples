import pandas as pd
import boto3
import json
import os
from datetime import datetime as dt

s3_client = boto3.client("s3")
s3_resource = boto3.resource("s3")
events_client = boto3.client("events")


def lambda_handler(event, context):
    for record in event["Records"]:
        payload = json.loads(record["body"])

        bucket_name = payload["detail"]["requestParameters"]["bucketName"]
        object_key = payload["detail"]["requestParameters"]["key"]

        raw_object = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        raw_data = json.loads(raw_object["Body"].read().decode("utf-8"))

        record_dates = [dt.strptime(r["dimensions"][0], "%Y%m%d%H") for r in raw_data["reports"][0]["data"]["rows"]]
        devices = [r["dimensions"][1] for r in raw_data["reports"][0]["data"]["rows"]]
        user_counts = [int(r["metrics"][0]["values"][0]) for r in raw_data["reports"][0]["data"]["rows"]]
        df = pd.DataFrame({
            "year": [r.year for r in record_dates],
            "month": [r.month for r in record_dates],
            "day": [r.day for r in record_dates],
            "hour": [r.hour for r in record_dates],
            "device": devices,
            "user_count": user_counts
        })

        output_file = dt.now().strftime("%Y%m%d%H%M%S")
        output_path = f"/tmp/{output_file}.parquet"
        df.to_parquet(output_path)

        output_key = f"ga-data/{output_file}.parquet"
        bucket = s3_resource.Bucket(bucket_name)
        bucket.upload_file(output_path, output_key)

        # Put event for next data stage to process (if any)
        events_client.put_events(Entries=[{
            "Source": os.environ["EVENT_SOURCE"],
            "DetailType": os.environ["EVENT_DETAIL_TYPE"],
            "Detail": json.dumps({"bucket": bucket_name, "key": output_key}),
            "Resources": [],
        }])
