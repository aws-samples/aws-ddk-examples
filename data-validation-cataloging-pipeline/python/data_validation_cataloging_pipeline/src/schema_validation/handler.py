import logging
import os

import awswrangler as wr
import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)


dynamodb_client = boto3.client("dynamodb")

DIDC_TABLE = os.environ["DIDC"]
DIDC_TABLE_NAME = DIDC_TABLE.split("/")[-1]


class Error(Exception):
    """Base class for other exceptions"""

    pass


class COUNT_NOT_MATCHED_ERROR(Error):
    """Raised when the number of columns Not matched from DIDC and parquet file"""

    pass


class SCHEMA_MISMATCH_ERROR(Error):
    """Raised when schema in DIDC and available in Parquet source file not matching"""

    pass


def lambda_handler(event, context):
    try:
        logger.info("Started lambda execution")

        # Extract required values from event
        source_name = event["body"]["source"]
        bucket_name = event["body"]["bucket"]
        dataset_name = event["body"]["dataset"]
        schema_name = event["body"]["schema_name"]

        logger.info(
            f"source_name:{source_name} \n bucket_name:{bucket_name} \n DataSetName:{dataset_name} \n Schema_name:{schema_name}"
        )
        logger.info(f"source_name:{source_name}")
        logger.info(f"bucket_name:{bucket_name}")
        logger.info(f"dataset_name:{dataset_name}")

        logger.info("Capture schema validation status in stage-b Pipeline")

        logger.info("get DIDC schema from Dynamodb")
        logger.info("TableName:{}".format(dataset_name))
        logger.info("Source-SchemaName:{}".format(source_name.upper() + "-" + schema_name.upper()))

        response = dynamodb_client.get_item(
            TableName=DIDC_TABLE_NAME,
            Key={
                "TableName": {"S": dataset_name},
                "Source-SchemaName": {"S": source_name.upper() + "-" + schema_name.upper()},
            },
        )
        logger.info(response)
        data = response["Item"]["columns"]["M"]
        DIDC_Schema = []
        for i, j in data.items():
            if list(j["M"]["ColumnStatus"].values())[0] == "Enterprise Approved":
                DIDC_Schema.append({"Name": i.upper().replace(" ", "_")})
            else:
                logger.info("column skipped{}".format(i))

        logger.info("DIDC_Schema is :{}".format(DIDC_Schema))
        logger.info("Completed getting DIDC schema")
        logger.info("started getting schema from parquet file")
        logger.info(
            "parquet file for which scheme going to check:{}".format(
                "s3://" + bucket_name + "/" + event["body"]["keysToProcess"][0]
            )
        )
        glue_Schema_temp = wr.s3.read_parquet_metadata("s3://" + bucket_name + "/" + event["body"]["keysToProcess"][0])
        logger.info("glueschema from parquet:{}".format(glue_Schema_temp))
        glue_Schema = [{"Name": k.upper().replace(" ", "_")} for k, v in glue_Schema_temp[0].items()]
        logger.info("extracted scheme from parquet file:{}".format(glue_Schema))
        logger.info(glue_Schema)
        logger.info(DIDC_Schema)
        if len(glue_Schema) == len(DIDC_Schema):
            logger.info("Count of columns are matched between DIDC and source Parquet file")
        else:
            raise COUNT_NOT_MATCHED_ERROR

        # Each column validation.
        mimatched_schema = []
        for i in DIDC_Schema:
            if i not in glue_Schema:
                logger.info(i)
                mimatched_schema.append(i)
        if len(mimatched_schema) > 0:
            response = {
                "status": 400,
                "comment": "number of columns not matching between glue catalog and schema available in DIDC",
                "DIDC_table_name": dataset_name,
                "column Mimatched": mimatched_schema,
            }
            logger.info(response)
            raise SCHEMA_MISMATCH_ERROR

    except COUNT_NOT_MATCHED_ERROR:
        raise COUNT_NOT_MATCHED_ERROR
    except SCHEMA_MISMATCH_ERROR:
        raise SCHEMA_MISMATCH_ERROR
    except Exception as e:
        raise e
    return
