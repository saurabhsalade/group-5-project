import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when

try:
    # AWS Glue-specific imports
    from awsglue.utils import getResolvedOptions
    from awsglue.context import GlueContext
    from awsglue.job import Job

    # Initialize Spark Session and Glue Context
    # In AWS Glue, a SparkSession is automatically available as 'spark'
    if 'glueContext' not in locals():
        glueContext = GlueContext(SparkSession.builder.getOrCreate())
        spark = glueContext.spark_session

    # If running in AWS Glue, initialize the job context
    if 'JOB_NAME' in sys.argv:
        args = getResolvedOptions(sys.argv, ['JOB_NAME'])
        job = Job(glueContext)
        job.init(args['JOB_NAME'], args)

    # Load data from S3 (Parquet format)
    s3_input_path = "s3://datalake-dbda-group5/vehicles-ingestion/"
    dataFrame = spark.read.parquet(s3_input_path)

    # Drop unwanted columns
    columns_to_drop = ["region_url", "image_url", "description"]
    dataFrame = dataFrame.drop(*columns_to_drop)

    # Print number of partitions before coalescing
    print("Number of partitions before coalescing:", dataFrame.rdd.getNumPartitions())

    # List of columns to replace blank values
    columns_to_fill = ["year", "manufacturer", "model", "condition", "cylinders", "fuel", "odometer", 
                       "title_status", "transmission", "VIN", "drive", "size", "type", "paint_color",
                       "county", "lat", "long", "posting_date"]

    # Replace blank values with 'NA'
    for column in columns_to_fill:
        dataFrame = dataFrame.withColumn(
            column, 
            when(col(column).isNull() | (col(column) == ""), lit("NA"))
            .otherwise(col(column))
        )

    # Coalesce to a single partition (file)
    dataFrame = dataFrame.coalesce(1)

    # Print number of partitions after coalescing
    print("Number of partitions after coalescing:", dataFrame.rdd.getNumPartitions())

    # Write the transformed data back to S3 in Parquet format with Snappy compression
    s3_output_path = "s3://datawarehouse-dbda-group5/vehicles-transform/"
    dataFrame.write.mode("overwrite").parquet(s3_output_path, compression="snappy")

    # If running in AWS Glue, commit the job
    if 'JOB_NAME' in sys.argv:
        job.commit()

except Exception as e:
    print(f"Error: {e}")
    if 'JOB_NAME' in sys.argv:
        job.commit()
finally:
    # Stop the Spark Session
    spark.stop()
