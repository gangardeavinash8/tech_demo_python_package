from datetime import datetime, timezone
from metadata_reader.models.metadata import FileMetadata

# Mock Data simulating Azure Blob properties and Account Tags
blob_name = "folder/file.csv"
container = "my-storage-container"
size = 123456
last_modified = datetime(2025, 1, 10, 12, 30, 0, tzinfo=timezone.utc)
last_accessed = datetime(2025, 1, 15, 9, 10, 0, tzinfo=timezone.utc)
account_tags = {"owner": "data-team", "project": "analytics"}
account_props = {"location": "eastus", "provisioning_state": "Succeeded"}

# Create Metadata Object (simulating AzureConnector logic)
metadata = FileMetadata(
    path=f"azure://{container}/{blob_name}",
    type="file",
    size_bytes=size,
    owner=account_tags.get('owner'), # Fallback to account owner
    last_modified=last_modified,
    last_accessed=last_accessed,
    source="blob_storage", # <--- UPDATED FIELD
    tags=account_tags,
    extra_metadata=account_props
)

print("--- Azure Metadata JSON Structure ---")
print(metadata.to_json())
print("-------------------------------------")
