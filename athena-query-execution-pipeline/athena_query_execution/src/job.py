import json
import sys

import awswrangler as wr
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

sc = SparkContext()
glueContext = GlueContext(sc)
logger = glueContext.get_logger()

args = getResolvedOptions(sys.argv, ["SFN_QUERY_INPUT", "SFN_QUERY_OUTPUT"])

try:
    SFN_QUERY_INPUT = json.loads(args["SFN_QUERY_INPUT"])
    SFN_QUERY_OUTPUT = json.loads(args["SFN_QUERY_OUTPUT"])

    print(SFN_QUERY_INPUT)
    print(SFN_QUERY_OUTPUT)

    QueryExecution = SFN_QUERY_OUTPUT.get("QueryExecution", {})
    input_s3_path = QueryExecution.get("ResultConfiguration", {}).get(
        "OutputLocation", {}
    )
    print(input_s3_path)
    df = wr.s3.read_csv(input_s3_path, path_suffix="csv")

    """
    Add your business logic and analytics transformations here
    """

    bucket = "/".join(input_s3_path.split("/")[0:3])
    print(bucket)
    queryid = SFN_QUERY_INPUT.get("queryId")
    queryid = queryid.replace("-", "_")
    print(queryid)
    target_s3_path = f"{bucket}/analytics/{queryid}/{QueryExecution.get('QueryExecutionId')}.parquet"
    print(target_s3_path)

    logger.info(f"writing data to the following S3 path: {target_s3_path}")
    wr.s3.to_parquet(df, target_s3_path)

except Exception as e:
    logger.error(f"error: {e}")
