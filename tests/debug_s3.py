import os
import boto3
from dotenv import load_dotenv

load_dotenv()

s3 = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION")
)

bucket = os.getenv("S3_BUCKET")
print(f"Checking bucket: {bucket}")

try:
    resp = s3.list_objects_v2(Bucket=bucket)
    count = resp.get('KeyCount', 0)
    print(f"KeyCount: {count}")
    if count > 0:
        print("Files found:")
        for obj in resp.get('Contents', []):
            print(f"- {obj['Key']}")
    else:
        print("Bucket is empty.")
except Exception as e:
    print(f"Error: {e}")
