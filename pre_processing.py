import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, coalesce, lit
from awsglue.dynamicframe import DynamicFrame  # Import DynamicFrame

# Initialize Glue Context and Job
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Amazon S3
AmazonS3_node1723198797207 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={"paths": ["s3://datalake-dataops-group5-bucket/vehicles/"], "recurse": True},
    transformation_ctx="AmazonS3_node1723198797207"
)

# Convert DynamicFrame to DataFrame for easier manipulation
df = AmazonS3_node1723198797207.toDF()

# Infer the schema of the DataFrame (this step is implicit in most cases)
df.printSchema()

# Define the columns where missing values should be handled
columns_to_handle = [
    'year', 'manufacturer', 'model', 'condition', 'cylinders', 'fuel', 'odometer',
    'title_status', 'transmission', 'VIN', 'drive', 'size', 'type', 'paint_color',
    'description', 'county', 'lat', 'long', 'posting_date'
]

# Replace missing values with "NA" using coalesce
for column in columns_to_handle:
    df = df.withColumn(column, coalesce(col(column), lit("NA")))

# Coalesce the DataFrame to one partition
df = df.coalesce(1)

# Convert back to DynamicFrame
dynamic_frame = DynamicFrame.fromDF(df, glueContext, "dynamic_frame")

# Script generated for node Drop Fields
DropFields_node1723199063184 = DropFields.apply(
    frame=dynamic_frame,
    paths=["region_url", "image_url", "description"],  # Drop any other unnecessary fields if needed
    transformation_ctx="DropFields_node1723199063184"
)

# Script generated for node Amazon S3
AmazonS3_node1723199342593 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1723199063184,
    connection_type="s3",
    format="glueparquet",
    connection_options={"path": "s3://datawarehouse-dataops-group5-bucket/vehicles-transform/", "partitionKeys": []},
    format_options={"compression": "snappy"},
    transformation_ctx="AmazonS3_node1723199342593"
)

job.commit()
