import os
from databricks.sdk import WorkspaceClient
from dotenv import load_dotenv

load_dotenv()

host = os.getenv("DATABRICKS_HOST")
token = os.getenv("DATABRICKS_TOKEN")

client = WorkspaceClient(host=host, token=token)

try:
    catalog = "fetch_databricks_metadata"
    schema = "default"
    
    print(f"--- Listing Volumes in {catalog}.{schema} ---")
    vols = list(client.volumes.list(catalog, schema))
    if vols:
        v = vols[0]
        print(f"Volume Name: {v.name}")
        print(f"Attributes: {dir(v)}")
        # Check specific fields
        for field in ['owner', 'created_by', 'updated_by']:
            if hasattr(v, field):
                print(f"{field}: {getattr(v, field)}")
            else:
                print(f"{field}: Not found")
                
    # Now check files
    path = "/Volumes/fetch_databricks_metadata/default/raw_volume"
    print(f"\n--- Checking File Entries in {path} ---")
    items = list(client.files.list_directory_contents(path))
    if items:
        item = items[0]
        print(f"Item Name: {item.name}")
        print(f"Attributes: {dir(item)}")
        # Check specific fields
        for field in ['owner', 'created_by', 'last_modified_by']:
            if hasattr(item, field):
                print(f"{field}: {getattr(item, field)}")
            else:
                print(f"{field}: Not found")

except Exception as e:
    print(f"Error: {e}")
