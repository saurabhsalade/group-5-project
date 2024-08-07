from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("S3 to S3 Transfer") \
    .config("spark.hadoop.fs.s3a.access.key", "your-access-key-id") \
    .config("spark.hadoop.fs.s3a.secret.key", "your-secret-access-key") \
    .config("spark.hadoop.fs.s3a.session.token", "your-session-token") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.multipart.size", "104857600") \
    .config("spark.hadoop.fs.s3a.fast.upload", "true") \
    .getOrCreate()

# Define S3 paths
source_bucket = "s3a://datasource-dataops-group5/vehicles/"
destination_bucket = "s3a://datalake-dataops-group5/vehicles/"

# Read data from the source S3 bucket
df = spark.read.option("header", "true").csv(source_bucket)

# Write data to the destination S3 bucket
df.write.mode("overwrite").parquet(destination_bucket)

# Stop Spark session
spark.stop()
