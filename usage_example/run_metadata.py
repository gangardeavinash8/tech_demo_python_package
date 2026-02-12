import os
import json
import sys
from dotenv import load_dotenv

# Try importing from the installed package
try:
    from metadata_reader.factory import get_connector
    from metadata_reader.config import load_config_from_env
except ImportError:
    print("Error: 'metadata_reader' package not found. Please install it first.", file=sys.stderr)
    sys.exit(1)

# Load environment variables from .env file
load_dotenv()

def main():
    # Load configuration (reads from environment variables)
    config = load_config_from_env()
    
    all_metadata = []
    
    # Check for credentials and run connectors if configured
    
    # 1. AWS S3
    if config.get("aws_access_key_id") and config.get("bucket"):
        try:
            print("Fetching AWS S3 metadata...", file=sys.stderr)
            s3 = get_connector("s3", config)
            all_metadata.extend(s3.list_objects())
        except Exception as e:
            print(f"Error fetching AWS S3: {e}", file=sys.stderr)

    # 2. Azure Blob Storage
    if config.get("connection_string") or config.get("azure_account_name"):
        try:
            print("Fetching Azure Blob Storage metadata...", file=sys.stderr)
            az = get_connector("azure", config)
            all_metadata.extend(az.list_objects())
        except Exception as e:
            print(f"Error fetching Azure: {e}", file=sys.stderr)

    # 3. SharePoint
    # Support both SHAREPOINT_SITE_ID and SHAREPOINT_SITE_URL
    sp_id = config.get("sharepoint_client_id")
    sp_site = config.get("sharepoint_site_id") or config.get("sharepoint_site_url")
    
    if sp_id and sp_site:
        try:
            print("Fetching SharePoint metadata...", file=sys.stderr)
            sp = get_connector("sharepoint", config)
            all_metadata.extend(sp.list_objects())
        except Exception as e:
            print(f"Error fetching SharePoint: {e}", file=sys.stderr)
    else:
        # Debug why it might be skipped
        if config.get("sharepoint_client_id"):
            print("Skipping SharePoint: Missing 'sharepoint_site_id' or 'sharepoint_site_url'", file=sys.stderr)
        elif config.get("sharepoint_site_id") or config.get("sharepoint_site_url"):
            print("Skipping SharePoint: Missing 'sharepoint_client_id'", file=sys.stderr)

    # 4. Databricks
    if config.get("databricks_host") and config.get("databricks_token"):
        try:
            print("Fetching Databricks metadata...", file=sys.stderr)
            
            db = get_connector("databricks", config)
            
            # The connector uses `databricks_catalog`, `databricks_schema`, and `databricks_volume`
            # from the config to determine the root path. 
            # We do NOT need to pass the path manually unless we want a subfolder.
            
            try:
                # Just call list_objects() to scan the configured volume root
                all_metadata.extend(db.list_objects())
            except Exception as e:
                 print(f"Error calling list_objects for Databricks: {e}", file=sys.stderr)
                 
        except Exception as e:
            print(f"Error fetching Databricks: {e}", file=sys.stderr)

    # Output results as JSON
    json_output = [item.to_dict() for item in all_metadata]
    print(json.dumps(json_output, indent=2, default=str))

if __name__ == "__main__":
    main()
