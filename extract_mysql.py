import pymysql
import csv
import boto3
import os

# MySQL Configuration from Environment Variables
hostname = os.getenv("MYSQL_HOSTNAME")
port = os.getenv("MYSQL_PORT", 3306)  # Default port if not set
username = os.getenv("MYSQL_USERNAME")
dbname = os.getenv("MYSQL_DATABASE")
password = os.getenv("MYSQL_PASSWORD")

# Check if all required MySQL configurations are provided
if not all([hostname, port, username, dbname, password]):
    raise ValueError("Missing MySQL configuration environment variables")

try:
    conn = pymysql.connect(
        host=hostname,
        user=username,
        password=password,
        db=dbname,
        port=int(port)
    )
    print("MySQL connection established!")
except pymysql.MySQLError as e:
    print(f"Error connecting to MySQL: {e}")
    raise

m_query = "SELECT * FROM customers;"
local_filename = "customer_extracts.csv"

try:
    with conn.cursor() as m_cursor:
        m_cursor.execute(m_query)
        results = m_cursor.fetchall()

        with open(local_filename, 'w', newline='') as fp:
            csv_w = csv.writer(fp, delimiter='|')
            csv_w.writerows(results)
        print(f"Data exported to {local_filename}")
except pymysql.MySQLError as e:
    print(f"Error executing MySQL query: {e}")
finally:
    conn.close()

# AWS Configuration from Environment Variables
access_key = os.getenv("AWS_ACCESS_KEY_ID")
secret_key = os.getenv("AWS_SECRET_KEY")
session_token = os.getenv("AWS_SESSION_TOKEN")
bucket_name = os.getenv("S3_BUCKET_NAME")

# Check if all required AWS configurations are provided
if not all([access_key, secret_key, session_token, bucket_name]):
    raise ValueError("Missing AWS configuration environment variables")

try:
    s3 = boto3.client(
        's3',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        aws_session_token=session_token
    )

    s3.upload_file(local_filename, bucket_name, local_filename)
    print(f"File uploaded to S3 bucket {bucket_name}")
except Exception as e:
    print(f"Error uploading file to S3: {e}")
