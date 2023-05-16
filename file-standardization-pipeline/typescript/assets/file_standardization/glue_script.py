import awswrangler as wr
import sys
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.utils import getResolvedOptions

sc = SparkContext()
glueContext = GlueContext(sc)
logger = glueContext.get_logger()

args = getResolvedOptions(sys.argv, ['input_s3_path', 'target_s3_path'])

input_s3_path = args['input_s3_path']
target_s3_path = args['target_s3_path']


try:
    logger.info(f"attempting to read input file from: {input_s3_path}")

    if '.json' in input_s3_path:
        df = wr.s3.read_json(input_s3_path)
    elif '.csv' in input_s3_path:
        df = wr.s3.read_csv(input_s3_path)
    elif '.parquet' in input_s3_path:
        df= wr.s3.read_parquet(input_s3_path)
    elif '.xlsx' in input_s3_path:
        df = wr.s3.read_excel(input_s3_path)

    else:
        raise Exception('Unexpected File Extension Error')

except Exception as e:
    logger.error(f"error reading input file: {e}")
    logger.error(f"Expected file extensions are: '.csv', '.parquet', '.xlsx', and '.json'")

else:
    logger.info("the input file has successfully been read")


###
# Add custom transformation logic here 
###


logger.info(f"writing data to the following S3 path: {target_s3_path}")
wr.s3.to_parquet(df, target_s3_path) 
