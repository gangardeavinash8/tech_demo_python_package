import os
import json
from dotenv import load_dotenv
from metadata_reader.factory import get_connector
from metadata_reader.config import load_config_from_env

# Load environment variables
load_dotenv()

def main():
    print("--- Testing SharePoint Connector ---")
    
    # 1. Load Configuration
    config = load_config_from_env()
    
    print("Debug: Loaded keys from environment:")
    for k, v in config.items():
        if v:
            print(f"  - {k}: {'*' * 4} (Present)")
        else:
            print(f"  - {k}: [MISSING]")
    print("-" * 30)

    # Check if critical keys are present
    if not config.get("sharepoint_client_id"):
        print("Error: SHAREPOINT_CLIENT_ID not found in .env")
        return
        
    try:
        # 2. Initialize Connector
        print(f"Initializing connector for site: {config.get('sharepoint_site_url') or config.get('sharepoint_site_id')}")
        connector = get_connector("sharepoint", config)
        
        # 3. List Objects
        print("Fetching files (this may take a moment)...")
        results = connector.list_objects()
        
        print(f"Found {len(results)} items.")
        
        # 4. Print First Result
        if results:
            print("\n--- JSON OUTPUT ---")
            json_output = [f.to_dict() for f in results]
            print(json.dumps(json_output, indent=2, default=str))
            print("-------------------")
            
    except Exception as e:
        print(f"\nFailed to fetch SharePoint data: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
