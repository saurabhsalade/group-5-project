import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Amazon S3
AmazonS3_node1723198797207 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://datalake-dataops-group5/vehicles/"]}, transformation_ctx="AmazonS3_node1723198797207")

# Script generated for node Drop Fields
DropFields_node1723199063184 = DropFields.apply(frame=AmazonS3_node1723198797207, paths=["region_url", "image_url", "description"], transformation_ctx="DropFields_node1723199063184")

# Script generated for node Amazon S3
AmazonS3_node1723199342593 = glueContext.write_dynamic_frame.from_options(frame=DropFields_node1723199063184, connection_type="s3", format="glueparquet", connection_options={"path": "s3://datawarehouse-dataops-group5/vehicles-transform/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1723199342593")

job.commit()
