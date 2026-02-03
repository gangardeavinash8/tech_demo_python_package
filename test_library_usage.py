from metadata_reader import connect, FileMetadata
import os
import json
from dotenv import load_dotenv

load_dotenv()

def main():
    print("--- Library Usage Demo ---")
    
    # Simulate user app config
    azure_config = {
        "azure_subscription_id": os.environ.get("AZURE_SUBSCRIPTION_ID"),
        "azure_client_id": os.environ.get("AZURE_CLIENT_ID"),
        "azure_client_secret": os.environ.get("AZURE_CLIENT_SECRET"),
        "azure_tenant_id": os.environ.get("AZURE_TENANT_ID"),
        "azure_resource_group": os.environ.get("AZURE_RESOURCE_GROUP"),
        "azure_account_name": os.environ.get("AZURE_ACCOUNT_NAME"),
    }
    
    try:
        # 1. Connect
        connector = connect("azure", azure_config)
        print(f"Connected to: {connector.__class__.__name__}")
        
        # 2. Get Account Metadata (The feature we just finished)
        meta = connector.get_account_metadata()
        
        # 3. Clean and Print (Simulating user app logic)
        print("\n[Result from Library Call]:")
        
        # Simple cleanup for display
        clean_res = {
            "location": meta.get("management_properties", {}).get("location"),
            "tags": meta.get("tags"),
            "role_assignments": [
                {
                    "role": r.get("role"),
                    "name": r.get("principal_name")
                } 
                for r in meta.get("management_properties", {}).get("role_assignments", [])
            ]
        }
        print(json.dumps(clean_res, indent=2))
        
    except Exception as e:
        print(f"Library Error: {e}")

if __name__ == "__main__":
    main()
