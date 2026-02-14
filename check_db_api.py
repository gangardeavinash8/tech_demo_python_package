import os
from databricks.sdk import WorkspaceClient
from dotenv import load_dotenv

load_dotenv()

client = WorkspaceClient()
print(f"Volumes API methods: {[m for m in dir(client.volumes) if not m.startswith('_')]}")
