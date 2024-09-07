import sys
from pyspark.sql import SparkSession

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
    spark.read.parquet(s3_input_path).createOrReplaceTempView("vehicles")

    # Debugging: Print schema and show data
    spark.sql("DESCRIBE TABLE vehicles").show()
    spark.sql("SELECT * FROM vehicles LIMIT 5").show()

    # Check if the DataFrame is empty
    count = spark.sql("SELECT COUNT(*) FROM vehicles").collect()[0][0]
    if count == 0:
        print("DataFrame is empty. Please check the input path and data availability.")
    else:
        # Drop unwanted columns
        columns_to_drop = ["region_url", "image_url", "description"]
        selected_columns = [col for col in spark.table("vehicles").columns if col not in columns_to_drop]
        spark.sql(f"""
            SELECT {", ".join(selected_columns)}
            FROM vehicles
        """).createOrReplaceTempView("vehicles")

        print("Number of partitions before coalescing:", spark.table("vehicles").rdd.getNumPartitions())

        # Replace blank or null values with 'NA' for string columns and null for numeric columns
        sql_query = """
            SELECT 
                COALESCE(CAST(NULLIF(id, '') AS BIGINT), 0) AS id,
                COALESCE(CAST(NULLIF(price, '') AS BIGINT), 0) AS price,
                COALESCE(CAST(NULLIF(year, '') AS INT), 0) AS year,
                COALESCE(CAST(NULLIF(odometer, '') AS BIGINT), 0) AS odometer,
                COALESCE(CAST(NULLIF(lat, '') AS DOUBLE), 0.0) AS lat,
                COALESCE(CAST(NULLIF(long, '') AS DOUBLE), 0.0) AS long,
                COALESCE(CAST(NULLIF(posting_date, '') AS TIMESTAMP), NULL) AS posting_date,
                COALESCE(NULLIF(manufacturer, ''), 'NA') AS manufacturer,
                COALESCE(NULLIF(model, ''), 'NA') AS model,
                COALESCE(NULLIF(condition, ''), 'NA') AS condition,
                COALESCE(NULLIF(cylinders, ''), 'NA') AS cylinders,
                COALESCE(NULLIF(fuel, ''), 'NA') AS fuel,
                COALESCE(NULLIF(title_status, ''), 'NA') AS title_status,
                COALESCE(NULLIF(transmission, ''), 'NA') AS transmission,
                COALESCE(NULLIF(Vin, ''), 'NA') AS Vin,
                COALESCE(NULLIF(drive, ''), 'NA') AS drive,
                COALESCE(NULLIF(size, ''), 'NA') AS size,
                COALESCE(NULLIF(type, ''), 'NA') AS type,
                COALESCE(NULLIF(paint_color, ''), 'NA') AS paint_color,
                COALESCE(NULLIF(county, ''), 'NA') AS county
            FROM vehicles
        """
        spark.sql(sql_query).createOrReplaceTempView("vehicles_transformed")

        # Coalesce to a single partition (file)
        vehicles_df = spark.table("vehicles_transformed").coalesce(1)
        print("Number of partitions after coalescing:", vehicles_df.rdd.getNumPartitions())

        # Write the transformed data back to S3 in Parquet format with Snappy compression
        s3_output_path = "s3://datawarehouse-dbda-group5/vehicles-transform/"
        vehicles_df.write.mode("overwrite").parquet(s3_output_path, compression="snappy")

    if 'JOB_NAME' in sys.argv:
        job.commit()

except Exception as e:
    print(f"Error: {e}")
    if 'JOB_NAME' in sys.argv:
        job.commit()
finally:
    spark.stop()
