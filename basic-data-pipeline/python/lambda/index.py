import json
import os
from datetime import datetime

import awswrangler as wr
import boto3

s3_resource = boto3.resource("s3")
events_client = boto3.client("events")


def lambda_handler(event, context):
    dest_keys = []
    timestamp = datetime.now()
    for record in event["Records"]:
        # Parse JSON object
        payload = json.loads(record["body"])
        bucket_name = payload["detail"]["bucket"]["name"]
        key = payload["detail"]["object"]["key"]

        # Read JSON file from bucket with S3 Select into Pandas dataframe
        df = wr.s3.select_query(
            sql="SELECT * FROM s3object",
            path=f"s3://{bucket_name}/{key}",
            input_serialization="JSON",
            input_serialization_params={
                "Type": "Document",
            },
            compression="gzip",
        )
        df = df[df.SECTOR == "TECHNOLOGY"]  ## Filter for a sector
        df["dt"] = f"{timestamp.year}/{timestamp.month}/{timestamp.day}"  ## Add a date column

        # Write the dataframe back to S3 as parquet
        results = wr.s3.to_parquet(
            df=df,
            path=f"s3://{bucket_name}/stage",
            dataset=True,
            partition_cols=["dt"],
        )
        dest_keys.append(results["paths"])

    # Put event for next data stage to process (if any)
    events_client.put_events(Entries=[{
        "Source": os.environ["EVENT_SOURCE"],
        "DetailType": os.environ["EVENT_DETAIL_TYPE"],
        "Detail": json.dumps({"bucket": bucket_name, "key": dest_keys}),
        "Resources": [],
    }])
