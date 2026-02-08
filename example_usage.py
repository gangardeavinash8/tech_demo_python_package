from metadata_reader.factory import get_connector
import os
import json

# Configuration for AWS S3
# You can also load these from environment variables (os.environ)
aws_config = {
    "aws_access_key_id": "YOUR_AWS_ACCESS_KEY",
    "aws_secret_access_key": "YOUR_AWS_SECRET_KEY",
    "region": "us-east-1",
    "bucket": "your-bucket-name"
}

# Configuration for Azure
azure_config = {
    "connection_string": "YOUR_AZURE_CONNECTION_STRING",
    "container": "your-container-name"
}

def main():
    print("--- Testing Installed Library ---")

    # Example: Using AWS S3
    # Uncomment the lines below if you have valid credentials in the config above
    try:
        # 1. Get the Connector
        # connector = get_connector("s3", aws_config)
        
        # 2. List Objects
        # results = connector.list_objects()
        
        # 3. Print Results
        # if results:
        #     print(results[0].to_json())
        pass 
    except Exception as e:
        print(f"AWS Error: {e}")

    # Example: Using Azure
    # Uncomment and fill config to test
    try:
        # connector = get_connector("azure", azure_config)
        # results = connector.list_objects()
        # if results:
        #     print(results[0].to_json())
        pass
    except Exception as e:
        print(f"Azure Error: {e}")

if __name__ == "__main__":
    main()
