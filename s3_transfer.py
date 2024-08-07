from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("S3 to S3 Transfer") \
    .getOrCreate()

# Define S3 paths
source_bucket = "s3://source-bucket/path/to/data"
destination_bucket = "s3://destination-bucket/path/to/data"

# Read data from the source S3 bucket (CSV format)
df = spark.read.csv(source_bucket, header=True, inferSchema=True)

# Write data to the destination S3 bucket (Parquet format)
df.write.mode("overwrite").parquet(destination_bucket)

# Stop Spark session
spark.stop()
