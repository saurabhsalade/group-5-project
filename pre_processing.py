import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when
from pyspark.sql.types import LongType, StringType, DoubleType, TimestampType, IntegerType

try:
    # AWS Glue-specific imports
    from awsglue.utils import getResolvedOptions
    from awsglue.context import GlueContext
    from awsglue.job import Job

    # Initialize Spark Session and Glue Context
    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY")
    glueContext = GlueContext(spark)
    
    # If running in AWS Glue, initialize the job context
    if 'JOB_NAME' in sys.argv:
        args = getResolvedOptions(sys.argv, ['JOB_NAME'])
        job = Job(glueContext)
        job.init(args['JOB_NAME'], args)

    # Load data from S3 (Parquet format)
    s3_input_path = "s3://datalake-dbda-group5/vehicles-ingestion/"
    dataFrame = spark.read.parquet(s3_input_path)

    # Debugging: Print schema and show data
    dataFrame.printSchema()
    dataFrame.show(5)

    if dataFrame.rdd.isEmpty():
        print("DataFrame is empty. Please check the input path and data availability.")
    else:
        # Drop unwanted columns
        columns_to_drop = ["region_url", "image_url", "description"]
        dataFrame = dataFrame.drop(*columns_to_drop)

        print("Number of partitions before coalescing:", dataFrame.rdd.getNumPartitions())

        # Replace blank or null values with 'NA' for string columns and null for numeric columns
        dataFrame = dataFrame.withColumn("id", 
                                         when(col("id").isNull(), lit(0).cast(LongType()))
                                         .otherwise(col("id").cast(LongType()))) \
                             .withColumn("price", 
                                         when(col("price").isNull(), lit(0).cast(LongType()))
                                         .otherwise(col("price").cast(LongType()))) \
                             .withColumn("year", 
                                         when(col("year").isNull(), lit(0).cast(IntegerType()))
                                         .otherwise(col("year").cast(IntegerType()))) \
                             .withColumn("odometer", 
                                         when(col("odometer").isNull(), lit(0).cast(LongType()))
                                         .otherwise(col("odometer").cast(LongType()))) \
                             .withColumn("lat", 
                                         when(col("lat").isNull(), lit(0.0).cast(DoubleType()))
                                         .otherwise(col("lat").cast(DoubleType()))) \
                             .withColumn("long", 
                                         when(col("long").isNull(), lit(0.0).cast(DoubleType()))
                                         .otherwise(col("long").cast(DoubleType()))) \
                             .withColumn("posting_date", 
                                         when(col("posting_date").isNull(), lit(None).cast(TimestampType()))
                                         .otherwise(col("posting_date").cast(TimestampType())))

        # Handle string columns for other fields with NA as replacement for empty strings or nulls
        columns_to_fill_with_na = ["manufacturer", "model", "condition", "cylinders", "fuel", "title_status", 
                                   "transmission", "Vin", "drive", "size", "type", "paint_color", "county"]
        for column in columns_to_fill_with_na:
            dataFrame = dataFrame.withColumn(
                column, 
                when(col(column).isNull() | (col(column) == ""), lit("NA").cast(StringType()))
                .otherwise(col(column).cast(StringType()))
            )

        # Coalesce to a single partition (file)
        dataFrame = dataFrame.coalesce(1)
        print("Number of partitions after coalescing:", dataFrame.rdd.getNumPartitions())

        # Write the transformed data back to S3 in Parquet format with Snappy compression
        s3_output_path = "s3://datawarehouse-dbda-group5/vehicles-transform/"
        dataFrame.write.mode("overwrite").parquet(s3_output_path, compression="snappy")

    if 'JOB_NAME' in sys.argv:
        job.commit()

except Exception as e:
    print(f"Error: {e}")
    if 'JOB_NAME' in sys.argv:
        job.commit()
finally:
    spark.stop()
