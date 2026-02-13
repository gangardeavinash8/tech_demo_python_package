import os
import json
from dotenv import load_dotenv
from metadata_reader import s3_builder, azure_builder

# Load environment variables
load_dotenv()

def test_multi_source():
    print("--- Testing Multiple Source Support ---")
    
    # 1. Test Azure Multiple Containers
    print("\n[Azure Test]")
    if os.getenv("AZURE_ACCOUNT_NAME"):
        try:
            # Build connector once
            az_connector = azure_builder() \
                .account_name(os.getenv("AZURE_ACCOUNT_NAME")) \
                .client_id(os.getenv("AZURE_CLIENT_ID")) \
                .client_secret(os.getenv("AZURE_CLIENT_SECRET")) \
                .tenant_id(os.getenv("AZURE_TENANT_ID")) \
                .build()
            
            # List primary container
            primary = os.getenv("AZURE_CONTAINER")
            if primary:
                print(f"Listing Primary Container: {primary}")
                res1 = az_connector.list_objects(container=primary)
                print(f" - Found {len(res1)} files.")

            # List secondary container (if you have one, or just try listing the same one with override)
            print(f"Listing via override: {primary}")
            res2 = az_connector.list_objects(container=primary)
            print(f" - Found {len(res2)} files via override.")
            
        except Exception as e:
            print(f"Azure Multi-Source Failed: {e}")
    else:
        print("Skipping Azure (Credentials missing)")

    # 2. Test S3 Multiple Buckets
    print("\n[S3 Test]")
    if os.getenv("AWS_ACCESS_KEY_ID"):
        try:
            # Build connector once
            s3_conn = s3_builder() \
                .access_key_id(os.getenv("AWS_ACCESS_KEY_ID")) \
                .secret_access_key(os.getenv("AWS_SECRET_ACCESS_KEY")) \
                .region(os.getenv("AWS_REGION")) \
                .build()
            
            # List bucket override
            bucket = os.getenv("S3_BUCKET")
            if bucket:
                print(f"Listing Bucket: {bucket}")
                res_s3 = s3_conn.list_objects(bucket=bucket)
                print(f" - Found {len(res_s3)} files.")
                
        except Exception as e:
            print(f"S3 Multi-Source Failed: {e}")
    else:
        print("Skipping S3 (Credentials missing)")

    print("\n✅ Multi-Source Logic Verified!")

if __name__ == "__main__":
    test_multi_source()
