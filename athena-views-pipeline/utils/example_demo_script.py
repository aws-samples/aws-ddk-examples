import argparse
import json
import os
import boto3

import awswrangler as wr
import pandas as pd

argParser = argparse.ArgumentParser()
argParser.add_argument("-b", "--bucket", help="bucket name")
argParser.add_argument("-p", "--profile", help="profile name")
argParser.add_argument("-r", "--region", help="aws region")

args = argParser.parse_args()

bucket = args.bucket
profile = args.profile if args.profile else "default"
region = args.region if args.region else "us-east-1"

boto3.setup_default_session(profile_name=profile, region_name=region)

subfolders = [f.path for f in os.scandir("./utils/data") if f.is_dir()]

for subfolder in subfolders:
    database_name = subfolder.split("/")[-1]
    print(database_name)

    try:
        wr.catalog.create_database(name=f"{database_name}")
    except Exception as e:
        print(e)
        print(f"the database {database_name} already exists")

    with open(
        f"./utils/data/{database_name}/{database_name}.json", encoding="utf-8"
    ) as config_file:
        json_file = json.load(config_file)

    df = pd.DataFrame.from_records(json_file)

    wr.s3.to_parquet(
        df,
        f"s3://{bucket}/data/{database_name}/{database_name}_table", 
        dataset=True,
        database=database_name,
        table=f"{database_name}_table",
        mode="overwrite"
    )