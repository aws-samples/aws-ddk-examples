import sys
from typing import List
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

args = getResolvedOptions(
    sys.argv, ['JOB_NAME', 'SOURCE_LOCATION', 'OUTPUT_LOCATION', 'DATABASE_NAME'])
source = args['SOURCE_LOCATION']
destination = args['OUTPUT_LOCATION']
database = args['DATABASE_NAME']

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

persons = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ['{}/{}'.format(source, 'persons_parsed.json')]
    },
    format_options={
        "withHeader": False
    },
    transformation_ctx="path={}".format('persons_df')
)

memberships = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ['{}/{}'.format(source, 'memberships_parsed.json')]
    },
    format_options={
        "withHeader": False
    },
    transformation_ctx="path={}".format('memberships_df')
)

organizations = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ['{}/{}'.format(source, 'organizations_parsed.json')]
    },
    format_options={
        "withHeader": False
    },
    transformation_ctx="path={}".format('organizations_df')
).rename_field('id', 'org_id').rename_field('name', 'org_name')

history = Join.apply(organizations,
                     Join.apply(persons, memberships, 'id', 'person_id'),
                     'org_id', 'organization_id').drop_fields(['person_id', 'org_id'])

def overwrite_table(frame: DynamicFrame, db: str, table: str, dest_path: str, partitions: List[str] = []):
    if spark.sql(f"SHOW TABLES FROM {db} LIKE '{table}'").count() == 1:
        glueContext.purge_s3_path(dest_path, options={"retentionPeriod": 0})
        glue_client = boto3.client('glue')
        glue_client.delete_table(DatabaseName=db, Name=table)

    sink = glueContext.getSink(
        connection_type="s3",
        path=dest_path,
        enableUpdateCatalog=True,
        updateBehavior="UPDATE_IN_DATABASE",
        partitionKeys=partitions
    )
    sink.setFormat("parquet", useGlueParquetWriter=True)
    sink.setCatalogInfo(catalogDatabase=db, catalogTableName=table)

    sink.writeFrame(frame)


overwrite_table(persons, database, "persons", f"{destination}/persons")
overwrite_table(organizations, database, "organizations", f"{destination}/organizations")
overwrite_table(memberships, database, "memberships", f"{destination}/memberships")
overwrite_table(history, database, "history", f"{destination}/history", ["org_name"])

job.commit()