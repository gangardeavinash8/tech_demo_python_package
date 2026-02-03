import os
import sys
from dotenv import load_dotenv
from metadata_reader.factory import get_connector

# Load environment variables from .env file
load_dotenv()

def test_s3():
    print("\n--- Testing AWS S3 ---")
    if not os.getenv("AWS_ACCESS_KEY_ID"):
        print("Skipping S3: AWS_ACCESS_KEY_ID not set")
        return

    try:
        config = {
            "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID"),
            "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
            "region": os.getenv("AWS_REGION"),
            "bucket": os.getenv("S3_BUCKET"),
        }
        connector = get_connector("s3", config)
        print(f"Listing top 5 files in S3 bucket: {config['bucket']}")
        results = connector.list_objects(prefix="")
        for item in results[:5]:
            print(f"- {item.path} ({item.size_bytes} bytes)")
    except Exception as e:
        print(f"S3 Test Failed: {e}")

def test_azure():
    print("\n--- Testing Azure Blob Storage ---")
    if not os.getenv("AZURE_CONNECTION_STRING") and not os.getenv("AZURE_ACCOUNT_NAME"):
        print("Skipping Azure: AZURE_CONNECTION_STRING and AZURE_ACCOUNT_NAME not set")
        return

    try:
        config = {
            "connection_string": os.getenv("AZURE_CONNECTION_STRING"),
            "container": os.getenv("AZURE_CONTAINER"),
            "azure_subscription_id": os.getenv("AZURE_SUBSCRIPTION_ID"),
            "azure_tenant_id": os.getenv("AZURE_TENANT_ID"),
            "azure_client_id": os.getenv("AZURE_CLIENT_ID"),
            "azure_client_secret": os.getenv("AZURE_CLIENT_SECRET"),
            "azure_resource_group": os.getenv("AZURE_RESOURCE_GROUP"),
            "azure_account_name": os.getenv("AZURE_ACCOUNT_NAME"),
        }
        connector = get_connector("azure", config)
        if config.get('container'):
            print(f"Listing top 5 blobs in Azure container: {config['container']}")
            
            print("\nFetching container metadata:")
            try:
                container_meta = connector.get_container_metadata()
                print(f"Container: {container_meta['name']}")
                print(f"Public Access: {container_meta['public_access']}")
                print(f"Lease Status: {container_meta['lease_status']}")
            except Exception as e:
                print(f"Container Metadata Error: {e}")
        else:
            print("Skipping Container operations (Container not configured)")

        print("\nFetching account metadata:")
        try:
            account_meta = connector.get_account_metadata()
            print(f"Account SKU: {account_meta['sku_name']}")
            print(f"Account Kind: {account_meta['account_kind']}")
            print(f"HNS Enabled: {account_meta.get('is_hns_enabled')}")
            print(f"Account Tags: {account_meta.get('tags')}")
            
            mgmt = account_meta.get('management_properties', {})
            if mgmt:
                print(f"Location: {mgmt.get('location')}")
                print(f"Provisioning State: {mgmt.get('provisioning_state')}")
                print(f"Creation Time: {mgmt.get('creation_time')}")
                # Print as flattened JSON for user verification
                import json
                
                # Filter for clean output (User requested specific fields)
                clean_meta = {
                    "location": mgmt.get("location"),
                    "provisioning_state": mgmt.get("provisioning_state"),
                    "tag": account_meta.get("tags")
                }
                
                # Clean up role assignments
                if "role_assignments" in mgmt:
                    clean_roles = []
                    for ra in mgmt["role_assignments"]:
                        clean_ra = {
                            "role": ra.get("role"),
                            "principal_name": ra.get("principal_name"), # Include if available
                            "principal_type": ra.get("principal_type") or ra.get("type"),
                        }
                        # Remove None
                        clean_roles.append({k: v for k, v in clean_ra.items() if v is not None})
                    clean_meta["role_assignments"] = clean_roles

                # Remove None
                clean_meta = {k: v for k, v in clean_meta.items() if v is not None}
                
                print("\n--- Account Metadata JSON ---")
                print(json.dumps(clean_meta, indent=2, default=str))
                print("-----------------------------")

        except Exception as e:
            print(f"Account Metadata Error: {e}")
        
        try:
            if config.get('container'):
                results = connector.list_objects(prefix="")
                for item in results[:5]:
                    print(f"- {item.path} ({item.size_bytes} bytes)")
            else:
                 results = []
        except Exception as e:
            print(f"List Objects Error (Data Plane): {e}")
            results = []
            
        if results:
            try:
                first_file = results[0]
                print(f"\nReading content of first file: {first_file.path}")
                content = connector.read_file(first_file.path)
                print(f"Content (first 100 bytes): {content[:100]}")
                
                print(f"\nFetching metadata for first file: {first_file.path}")
                metadata = connector.get_metadata(first_file.path)
                
                print("\n--- JSON Output ---")
                print(metadata.to_json())
                print("-------------------")
            except Exception as e:
                print(f"File Access Error (Data Plane): {e}")

    except Exception as e:
        print(f"Azure Test Failed: {e}")

def test_databricks():
    print("\n--- Testing Databricks ---")
    if not os.getenv("DATABRICKS_HOST"):
        print("Skipping Databricks: DATABRICKS_HOST not set")
        return

    try:
        config = {
            "databricks_host": os.getenv("DATABRICKS_HOST"),
            "databricks_token": os.getenv("DATABRICKS_TOKEN"),
        }
        connector = get_connector("databricks", config)
        print(f"Listing top 5 files in DBFS root")
        results = connector.list_objects(prefix="dbfs:/")
        for item in results[:5]:
            print(f"- {item.path} ({item.size_bytes} bytes)")
    except Exception as e:
        print(f"Databricks Test Failed: {e}")

if __name__ == "__main__":
    print("Starting Live Connectivity Tests...")
    test_s3()
    test_azure()
    test_databricks()
    print("\nDone.")
