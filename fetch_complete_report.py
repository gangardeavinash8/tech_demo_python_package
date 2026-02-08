import os
import json
from dotenv import load_dotenv
from metadata_reader.factory import get_connector
from metadata_reader.config import load_config_from_env

# Load environment variables
load_dotenv()

def fetch_all():
    config = load_config_from_env()
    all_metadata = []
    
    # 1. Provide Config for Connectors
    # The factory takes `config` dict.
    
    # --- AWS ---
    if config.get("aws_access_key_id") and config.get("bucket"):
        try:
            # print("Fetching AWS S3 metadata...", file=sys.stderr)
            s3 = get_connector("s3", config)
            results = s3.list_objects()
            all_metadata.extend(results)
        except Exception as e:
            # Log error to stderr, keep stdout clean for JSON
            import sys
            print(f"Error fetching AWS S3: {e}", file=sys.stderr)

    # --- Azure ---
    if config.get("connection_string") or config.get("azure_account_name"):
        try:
            # print("Fetching Azure Blob Storage metadata...", file=sys.stderr)
            az = get_connector("azure", config)
            results = az.list_objects()
            all_metadata.extend(results)
        except Exception as e:
            import sys
            print(f"Error fetching Azure: {e}", file=sys.stderr)

    # --- SharePoint ---
    if config.get("sharepoint_client_id") and (config.get("sharepoint_site_id") or config.get("sharepoint_site_url")):
        try:
            # print("Fetching SharePoint metadata...", file=sys.stderr)
            sp = get_connector("sharepoint", config)
            # Use recursive scanning implicitly if implemented or just list objects
            results = sp.list_objects()
            all_metadata.extend(results)
        except Exception as e:
            import sys
            print(f"Error fetching SharePoint: {e}", file=sys.stderr)

    # --- Output JSON ---
    json_output = [item.to_dict() for item in all_metadata]
    print(json.dumps(json_output, indent=2, default=str))

if __name__ == "__main__":
    fetch_all()
