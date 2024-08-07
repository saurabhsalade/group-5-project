from pyspark.sql import SparkSession

# Initialize Spark session with Hadoop AWS and AWS SDK configurations
spark = SparkSession.builder.appName("CSV to Parquet").getOrCreate()

# Define S3 paths
source_bucket = "s3a://datasource-dataops/vehicles/"
destination_bucket = "s3a://datalake-dataops/vehicles"

# Read data from the source S3 bucket (CSV format)
df = spark.read.csv(source_bucket, header=True, inferSchema=True)

# Write data to the destination S3 bucket (Parquet format)
df.write.mode("overwrite").parquet(destination_bucket)

# Stop Spark session
spark.stop()
