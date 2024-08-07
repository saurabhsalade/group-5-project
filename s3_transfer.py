import os
from pyspark.sql import SparkSession

def main():
    # Retrieve AWS credentials from environment variables
    aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    aws_session_token = os.getenv('AWS_SESSION_TOKEN')

    # Initialize Spark session with AWS credentials
    spark = SparkSession.builder \
        .appName("S3 to Parquet") \
        .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id) \
        .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key) \
        .config("spark.hadoop.fs.s3a.session.token", aws_session_token) \
        .getOrCreate()

    # S3 bucket information
    source_s3_bucket = "s3://datasource-dataops-group5/vehicles-dataset/vehicles.csv"
    destination_s3_bucket = "s3://datalake-dataops-group5/vehicles/"

    # Read CSV data from S3
    df = spark.read.format("csv").option("header", "true").load(source_s3_bucket)

    # Write data to Parquet format in S3
    df.write.mode("overwrite").parquet(destination_s3_bucket)

    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    main()
