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
print(f"Checking tags for bucket: {bucket}")

try:
    # Check Object Tags
    print("\nFile Tags (test_s3_file.txt):")
    try:
        obj_tags = s3.get_object_tagging(Bucket=bucket, Key="test_s3_file.txt")
        print(obj_tags.get("TagSet"))
    except Exception as e:
        print(f"Error fetching object tags: {e}")

    # Check Bucket Tags
    print("\nBucket Tags:")
    try:
        bucket_tags = s3.get_bucket_tagging(Bucket=bucket)
        print(bucket_tags.get("TagSet"))
    except Exception as e:
        print(f"Error fetching bucket tags: {e}")

except Exception as e:
    print(f"Error: {e}")
