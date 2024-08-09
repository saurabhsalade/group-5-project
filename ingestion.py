import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Initialize the Glue job
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

# Convert the dynamic frame to a Spark DataFrame
df = datasource0.toDF()

# Coalesce the DataFrame to a single partition
df_single_file = df.coalesce(1)

# Write the DataFrame as a single Parquet file to the target S3 bucket
df_single_file.write.mode("overwrite").parquet(target_s3_path)

# Commit the Glue job
job.commit()
