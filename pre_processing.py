import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.functions import lit
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Amazon S3
AmazonS3_node1723463202371 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={"paths": ["s3://datalake-dbda-group5/vehicles-ingestion/"], "recurse": True},
    transformation_ctx="AmazonS3_node1723463202371"
)

# Script generated for node Drop Fields
DropFields_node1723463391225 = DropFields.apply(
    frame=AmazonS3_node1723463202371,
    paths=["region_url", "image_url", "description"],
    transformation_ctx="DropFields_node1723463391225"
)

# Convert DynamicFrame to DataFrame
dataFrame = DropFields_node1723463391225.toDF()

# Print number of partitions before coalescing
print("Number of partitions before coalescing:", dataFrame.rdd.getNumPartitions())

# List of columns to replace blank values
columns_to_fill = ["year", "manufacturer", "model", "condition", "cylinders", "fuel", "odometer", 
                    "title_status", "transmission", "VIN", "drive", "size", "type", "paint_color",
                    "county", "lat", "long", "posting_date"]

# Replace blank values with 'NA'
for column in columns_to_fill:
    dataFrame = dataFrame.withColumn(column, 
        F.when(F.col(column).isNull() | (F.col(column) == ""), lit("NA"))
         .otherwise(F.col(column))
    )

# Coalesce to a single partition (file)
dataFrame = dataFrame.coalesce(1)

# Print number of partitions after coalescing
print("Number of partitions after coalescing:", dataFrame.rdd.getNumPartitions())

# Convert DataFrame back to DynamicFrame
finalDynamicFrame = DynamicFrame.fromDF(dataFrame, glueContext, "finalDynamicFrame")

# Script generated for node Amazon S3
AmazonS3_node1723463967335 = glueContext.write_dynamic_frame.from_options(
    frame=finalDynamicFrame,
    connection_type="s3",
    format="glueparquet",
    connection_options={"path": "s3://datawarehouse-dbda-group5/vehicles-transform/", "partitionKeys": []},
    format_options={"compression": "snappy"},
    transformation_ctx="AmazonS3_node1723463967335"
)

job.commit()
