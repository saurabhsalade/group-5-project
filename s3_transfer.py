import os
from pyspark.sql import SparkSession

# Get AWS credentials from environment variables
aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_key = os.getenv("AWS_SECRET_KEY")
aws_session_token = os.getenv("AWS_SESSION_TOKEN")

# Initialize Spark session with S3 configurations
spark = SparkSession.builder \
    .appName("S3 to S3 Transfer") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.access.key", aws_access_key) \
    .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key) \
    .config("spark.hadoop.fs.s3a.session.token", aws_session_token) \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .getOrCreate()

# Define S3 paths
source_bucket = "s3://datasource-dataops-group5/"
destination_bucket = "s3://datalake-dataops-group5/"

# Read data from the source S3 bucket (CSV format)
df = spark.read.csv(source_bucket, header=True, inferSchema=True)

# Write data to the destination S3 bucket (Parquet format)
df.write.mode("overwrite").parquet(destination_bucket)

# Stop Spark session
spark.stop()
