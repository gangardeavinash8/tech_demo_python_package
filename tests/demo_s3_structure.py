from datetime import datetime, timezone
from metadata_reader.models.metadata import FileMetadata

# Mock Data simulating S3 Object properties
key = "folder/file.csv"
bucket = "my-s3-bucket"
size = 123456
last_modified = datetime(2025, 1, 10, 12, 30, 0, tzinfo=timezone.utc)
owner_display = "aws-data-team"

# Create Metadata Object (simulating S3Connector logic)
metadata = FileMetadata(
    path=f"s3://{bucket}/{key}",
    type="file",
    size_bytes=size,
    owner=owner_display,
    last_modified=last_modified,
    last_accessed=None, # S3 limitation
    source="s3",
    extra_metadata={
        "storage_class": "STANDARD",
        "etag": "\"1234567890abcdef\""
    }
)

print("--- AWS S3 Metadata JSON Structure ---")
print(metadata.to_json())
print("--------------------------------------")
