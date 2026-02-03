import os
import json
from dotenv import load_dotenv
from metadata_reader.factory import get_connector

# Load environment variables
load_dotenv()

def fetch_and_print(source_name, connector_type, config):
    print(f"\n--- {source_name} Metadata ---")
    try:
        connector = get_connector(connector_type, config)
        results = connector.list_objects()
        
        if not results:
            print(f"No files found in {source_name}.")
            return

        # Print first file details in JSON for inspection
        print(f"Found {len(results)} files. Showing metadata for the first file:")
        print(results[0].to_json())
        
    except Exception as e:
        print(f"Error fetching {source_name}: {e}")

def main():
    # AWS S3 Config
    s3_config = {
        "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID"),
        "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
        "region": os.getenv("AWS_REGION"),
        "bucket": os.getenv("S3_BUCKET"),
    }
    
    # Azure Config
    azure_config = {
        "connection_string": os.getenv("AZURE_CONNECTION_STRING"),
        "container": os.getenv("AZURE_CONTAINER"),
        "azure_subscription_id": os.getenv("AZURE_SUBSCRIPTION_ID"),
        "azure_tenant_id": os.getenv("AZURE_TENANT_ID"),
        "azure_client_id": os.getenv("AZURE_CLIENT_ID"),
        "azure_client_secret": os.getenv("AZURE_CLIENT_SECRET"),
        "azure_resource_group": os.getenv("AZURE_RESOURCE_GROUP"),
        "azure_account_name": os.getenv("AZURE_ACCOUNT_NAME"),
    }

    # Fetch AWS
    if s3_config["bucket"]:
        fetch_and_print("AWS S3", "s3", s3_config)
    else:
        print("Skipping AWS S3 (Configuration missing)")

    # Fetch Azure
    if azure_config["azure_account_name"] or azure_config["connection_string"]:
        fetch_and_print("Azure Blob Storage", "azure", azure_config)
    else:
        print("Skipping Azure (Configuration missing)")

if __name__ == "__main__":
    main()
