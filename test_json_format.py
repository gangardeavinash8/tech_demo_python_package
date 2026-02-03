from datetime import datetime
from metadata_reader.models.metadata import FileMetadata

def test_json_structure():
    # Simulate data matching user's request
    meta = FileMetadata(
        path="s3://bucket/folder/file.csv",
        type="file",
        size_bytes=123456,
        owner="data-team",
        last_modified=datetime.fromisoformat("2025-01-10T12:30:00+00:00"),
        last_accessed=datetime.fromisoformat("2025-01-15T09:10:00+00:00"),
        source="s3",
        tags={"account tag": "value"}, 
        extra_metadata={
            "location": "eastus",
            "provisioning_state": "Succeeded",
            "account_kind": "StorageV2",
            "role_assignments": [
                {"role": "Owner", "principal_id": "abc-123", "type": "User"},
                {"role": "Reader", "principal_id": "def-456", "type": "ServicePrincipal"}
            ]
        }
    )
    
    print("Generated JSON:")
    print(meta.to_json())

if __name__ == "__main__":
    test_json_structure()
