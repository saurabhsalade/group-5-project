from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("parquet_example") \
    .getOrCreate()

def write_parquet_file():
    # Read data from S3
    df = spark.read.csv('s3://datasource-dataops-group5/vehicles-dataset/vehicles.csv', header=True)
    
    # Write data to another S3 bucket
    df.repartition(1).write.mode('overwrite').parquet('s3://datalake-dataops-group5/')

if __name__ == "__main__":
    write_parquet_file()
   
