from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("S3 to S3 Transfer") \
    .getOrCreate()

# Define S3 paths
source_bucket = "s3://datasource-dataops-group5/vehicles/"
destination_bucket = "s3://datalake-dataops-group5/vehicles/"

# Read data from the source S3 bucket (CSV format)
df = spark.read.csv(source_bucket, header=True, inferSchema=True)

# Write data to the destination S3 bucket (CSV format)
df.write.mode("overwrite").csv(destination_bucket, header=True)

# Stop Spark session
spark.stop()

