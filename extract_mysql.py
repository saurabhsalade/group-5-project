import pymysql
import csv
import boto3
import os

# MySQL Configuration from Environment Variables
hostname = os.getenv("MYSQL_HOSTNAME")
port = os.getenv("MYSQL_PORT")
username = os.getenv("MYSQL_USERNAME")
dbname = os.getenv("MYSQL_DATABASE")
password = os.getenv("MYSQL_PASSWORD")

conn = pymysql.connect(host=hostname,
                       user=username,
                       password=password,
                       db=dbname,
                       port=port)

if conn is None:
    print("Error connecting to the MySQL database")
else:
    print("MySQL connection established!")

m_query = "SELECT * FROM customers;"
local_filename = "customer_extracts.csv"

m_cursor = conn.cursor()
m_cursor.execute(m_query)
results = m_cursor.fetchall()

with open(local_filename, 'w') as fp:
    csv_w = csv.writer(fp, delimiter='|')
    csv_w.writerows(results)

fp.close()
m_cursor.close()
conn.close()

# AWS Configuration from Environment Variables
access_key = os.getenv("AWS_ACCESS_KEY_ID")
secret_key = os.getenv("AWS_SECRET_KEY")
session_token = os.getenv("AWS_SESSION_TOKEN")
bucket_name = os.getenv("S3_BUCKET_NAME")

s3 = boto3.client('s3',
                  aws_access_key_id=access_key,
                  aws_secret_access_key=secret_key,
                  aws_session_token=session_token)

s3_file = local_filename

s3.upload_file(local_filename, bucket_name, s3_file)
