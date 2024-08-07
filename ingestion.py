import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Define the source and target S3 paths
source_s3_path = "s3://datasource-dataops-group5/vehicles-dataset/"
target_s3_path = "s3://datalake-dataops-group5/vehicles/"

# Read the data from the source S3 bucket
datasource0 = glueContext.create_dynamic_frame.from_options("s3", {'paths': [source_s3_path]}, format="csv")

# Write the data to the target S3 bucket
datasink4 = glueContext.write_dynamic_frame.from_options(frame=datasource0, connection_type="s3", connection_options={"path": target_s3_path}, format="parquet")

job.commit()
