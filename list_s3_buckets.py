import boto3
import os
from dotenv import load_dotenv

load_dotenv()

def list_all_buckets():
    print("🔍 Fetching all S3 buckets...")
    try:
        s3 = boto3.client(
            "s3",
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name=os.getenv("AWS_REGION")
        )
        response = s3.list_buckets()
        buckets = [bucket["Name"] for bucket in response.get("Buckets", [])]
        
        if not buckets:
            print("❌ No buckets found in this account.")
        else:
            print(f"✅ Found {len(buckets)} buckets:")
            for b in buckets:
                print(f" - {b}")
                
    except Exception as e:
        print(f"❌ Failed to list buckets: {e}")

if __name__ == "__main__":
    list_all_buckets()
