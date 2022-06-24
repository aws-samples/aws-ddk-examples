import logging
import os
from typing import Any, Dict

import boto3

# Create logger for logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

glue_client = boto3.client("glue")
dynamodb = boto3.resource("dynamodb")
ENV = os.environ["ENV"]
DIDC_TABLE = os.environ["DIDC"]
DIDC_TABLE_NAME = DIDC_TABLE.split("/")[-1]
PREFIX = os.environ["PREFIX"]



# Tagging Process
def lambda_handler(event: Dict[str, Any], context: Any) -> None:
    try:
        logger.info("Tagging-Lambda process: lambda_handler() has been invoked")

        # Read all required parameters from an event: source_name, schema_name , catalog_table_name etc
        # event_ = event
        event = event["body"]
        dataset = event["dataset"]
        source = event["source"]
        key = event["keysToProcess"]
        glue_db_name = f"{PREFIX}-ingestion-data-catalog-{ENV}"
        glue_table_name = source.upper() + "_" + dataset.upper()

        table = dynamodb.Table(DIDC_TABLE_NAME)
        response = table.query(
            KeyConditionExpression=boto3.dynamodb.conditions.Key("TableName").eq(dataset.upper()),
            FilterExpression=boto3.dynamodb.conditions.Attr("TableStatus").eq("Enterprise Approved"),
        )
        data = response["Items"]

        # Column Tag

        # Get Glue Catalog details
        glue_response = glue_client.get_table(DatabaseName=glue_db_name, Name=glue_table_name)

        New_Column = []
        for key, val in data[0].get("columns").items():
            for glue_column in glue_response["Table"]["StorageDescriptor"]["Columns"]:
                if glue_column["Name"].upper() == key.upper():
                    glue_column["Parameters"] = val
                    New_Column.append(glue_column)

        glue_response["Table"]["StorageDescriptor"]["Columns"] = New_Column

        response = glue_client.update_table(
            DatabaseName=glue_db_name,
            TableInput={
                "Name": glue_response["Table"]["Name"],
                "Retention": glue_response["Table"]["Retention"],
                "StorageDescriptor": glue_response["Table"]["StorageDescriptor"],
                "PartitionKeys": glue_response["Table"]["PartitionKeys"],
                "TableType": glue_response["Table"]["TableType"],
                "Parameters": glue_response["Table"]["Parameters"],
            },
        )

    except Exception as e:
        raise e
        
    return
