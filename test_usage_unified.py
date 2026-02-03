import os
from metadata_reader import connect, FileMetadata

def test_unified_api():
    print("\n--- Testing Unified API ---")
    
    # Check imports
    print("Successfully imported 'connect' and 'FileMetadata' from 'metadata_reader'")
    
    # Try to instantiate a connector (Azure or S3)
    # Using dummy config just to check factory logic
    try:
        print("Attempting to create Azure connector via connect()...")
        connector = connect(source="azure", config={
            "connection_string": "DefaultEndpointsProtocol=https;AccountName=dummy;AccountKey=dummy;EndpointSuffix=core.windows.net",
            "container": "dummy-container"
        })
        print(f"Connector created: {type(connector).__name__}")
        
    except Exception as e:
        print(f"Failed to create connector: {e}")

if __name__ == "__main__":
    test_unified_api()
