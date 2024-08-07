from pyspark.sql import SparkSession

# Initialize Spark session with Hadoop AWS and AWS SDK configurations
spark = SparkSession.builder \
    .appName("S3 to S3 Transfer") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .config("spark.hadoop.fs.s3a.connection.maximum", "100") \
    .config("spark.hadoop.fs.s3a.connection.timeout", "5000") \
    .config("spark.hadoop.fs.s3a.retry.limit", "10") \
    .config("spark.hadoop.fs.s3a.fast.upload", "true") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

# Define S3 paths
source_bucket = "s3a://datasource-dataops/vehicles/"
destination_bucket = "s3a://datalake-dataops/vehicles"

# Read data from the source S3 bucket (CSV format)
df = spark.read.csv(source_bucket, header=True, inferSchema=True)

# Write data to the destination S3 bucket (Parquet format)
df.write.mode("overwrite").parquet(destination_bucket)

# Stop Spark session
spark.stop()
