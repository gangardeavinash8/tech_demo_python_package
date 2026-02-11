import os
from dotenv import load_dotenv
from metadata_reader.factory import get_connector

# Load environment variables
load_dotenv()

def main():
    print("--- Testing Databricks Connector (Volumes) ---")
    
    # 1. Define Config
    config = {
        "databricks_host": os.getenv("DATABRICKS_HOST"),
        "databricks_token": os.getenv("DATABRICKS_TOKEN"),
        "databricks_catalog": os.getenv("DATABRICKS_CATALOG"),
        "databricks_schema": os.getenv("DATABRICKS_SCHEMA"),
        "databricks_volume": os.getenv("DATABRICKS_VOLUME"),
    }
    
    # Print debug info (masking token)
    print("Debug: Loaded keys from environment:")
    for k, v in config.items():
        if "token" in k and v:
            print(f"  - {k}: [PRESENT but masked]")
        else:
            print(f"  - {k}: {v if v else '[MISSING]'}")

    if not config["databricks_host"] or not config["databricks_token"]:
        print("Error: DATABRICKS_HOST and DATABRICKS_TOKEN are required in .env")
        return

    try:
        # 2. Get Connector
        print(f"Initializing connector for Volume: /Volumes/{config['databricks_catalog']}/{config['databricks_schema']}/{config['databricks_volume']}")
        connector = get_connector("databricks", config)
        
        # 3. Fetch Data
        print("Fetching files via Unity Catalog Volumes API...")
        results = connector.list_objects()
        
        # 4. Print Results
        if results:
            print(f"\nFound {len(results)} items:")
            for f in results:
                print(f" - {f.path} (Size: {f.size_bytes}b)")
            
            print(f"\nFirst Item Metadata (Detail):")
            print(results[0].to_json())
        else:
            print("\nNo files found in the specified Volume.")
            
    except Exception as e:
        print(f"\nFailed to fetch Databricks data: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
