import json
import os
from datetime import datetime as dt

import awswrangler as wr
import boto3
import pandas as pd

s3_client = boto3.client("s3")
events_client = boto3.client("events")

DB = os.environ["DB"]

def lambda_handler(event, context):
    for record in event["Records"]:
        payload = json.loads(record["body"])

        bucket_name = payload["detail"]["bucket"]["name"]
        object_key = payload["detail"]["object"]["key"]
        table = object_key.split("/")[-2]

        # Read JSON data
        raw_object = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        raw_data = json.loads(raw_object["Body"].read().decode("utf-8"))

        ## ADD your business logic here
        ##--------------------------------------

        df = pd.DataFrame(
            {
                "minutes_worked": [60 * r["hours_worked"] for r in raw_data["sales_data"]],
                "quantity_sold": [r["quantity_sold"] for r in raw_data["sales_data"]],
                "job_title": [r["job_title"] for r in raw_data["sales_data"]],
                "sales_person": [r["sales_person"] for r in raw_data["sales_data"]],
            }
        )

        ##----------------------------------------

        # Write parquet file to S3 and create metadata table
        wr.s3.to_parquet(
            df,
            f"s3://{bucket_name}/processed/{table}",
            dataset=True,
            database=DB,
            table=table,
        )