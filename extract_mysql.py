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

try:
    conn = pymysql.connect(host=hostname, user=username, password=password, db=dbname, port=int(port))
    print("MySQL connection established!")

    m_query = "SELECT * FROM customers;"
    local_filename = "customer_extracts.csv"

    with conn.cursor() as cursor:
        cursor.execute(m_query)
        results = cursor.fetchall()

        with open(local_filename, 'w', newline='') as fp:
            csv_w = csv.writer(fp, delimiter='|')
            # Write headers
            csv_w.writerow([i[0] for i in cursor.description])
            # Write data
            csv_w.writerows(results)

    print(f"Data extracted to {local_filename}")

    # AWS Configuration from Environment Variables
    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    session_token = os.getenv("AWS_SESSION_TOKEN")
    bucket_name = os.getenv("S3_BUCKET_NAME")

    s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key, aws_session_token=session_token)

    s3_file = local_filename
    s3.upload_file(local_filename, bucket_name, s3_file)
    print(f"File uploaded to S3 bucket: {bucket_name}")

    # Optional: Remove local file
    os.remove(local_filename)
    print(f"Local file {local_filename} removed")

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    if 'conn' in locals() and conn.open:
        conn.close()
        print("MySQL connection closed")
