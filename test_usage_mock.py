import unittest
from unittest.mock import MagicMock, patch
from datetime import datetime, timezone
from metadata_reader.factory import get_connector
from metadata_reader.models.metadata import FileMetadata

class TestMetadataReader(unittest.TestCase):
    def test_s3_connector(self):
        # Mock config
        config = {
            "aws_access_key_id": "test",
            "aws_secret_access_key": "test",
            "bucket": "my-bucket",
            "region": "us-east-1"
        }

        # Mock boto3 client
        with patch("boto3.client") as mock_boto:
            mock_s3 = MagicMock()
            mock_boto.return_value = mock_s3
            
            # Mock S3 response
            mock_s3.list_objects_v2.return_value = {
                "Contents": [
                    {
                        "Key": "data/file.csv",
                        "Size": 1024,
                        "LastModified": datetime(2025, 1, 10, 12, 30, 0, tzinfo=timezone.utc)
                    }
                ]
            }

            connector = get_connector("s3", config)
            metadata = connector.list_objects(prefix="data/")

            self.assertEqual(len(metadata), 1)
            item = metadata[0]
            self.assertEqual(item.path, "s3://my-bucket/data/file.csv")
            self.assertEqual(item.size_bytes, 1024)
            self.assertEqual(item.source, "s3")
            print("S3 Connector Verified Successfully!")

    def test_azure_connector(self):
        # Mock config
        config = {
            "connection_string": "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=key;EndpointSuffix=core.windows.net",
            "container": "my-container"
        }

        # Mock azure client
        with patch("metadata_reader.connectors.azure.BlobServiceClient") as mock_service_client:
            mock_blob_client = MagicMock()
            mock_container_client = MagicMock()
            
            mock_service_client.from_connection_string.return_value = mock_blob_client
            mock_blob_client.get_container_client.return_value = mock_container_client

            # Mock Blob response
            mock_blob = MagicMock()
            mock_blob.name = "data/file.txt"
            mock_blob.size = 512
            mock_blob.last_modified = datetime(2025, 1, 15, 9, 10, 0, tzinfo=timezone.utc)
            
            mock_container_client.list_blobs.return_value = [mock_blob]

            connector = get_connector("azure", config)
            metadata = connector.list_objects(prefix="data/")

            self.assertEqual(len(metadata), 1)
            item = metadata[0]
            self.assertEqual(item.path, "azure://my-container/data/file.txt")
            self.assertEqual(item.source, "azure")
            print("Azure Connector Verified Successfully!")

    def test_databricks_connector(self):
        # Mock config
        config = {
            "databricks_host": "https://test-databricks.com",
            "databricks_token": "test-token"
        }

        # Mock databricks client
        with patch("metadata_reader.connectors.databricks.WorkspaceClient") as mock_ws_client:
            mock_client = MagicMock()
            mock_ws_client.return_value = mock_client
            
            mock_file_info = MagicMock()
            mock_file_info.path = "dbfs:/data/file.parquet"
            mock_file_info.is_dir = False
            mock_file_info.file_size = 2048
            mock_file_info.modification_time = 1675000000000 # Timestamp in ms
            
            # dbfs.list returns an iterator or list
            mock_client.dbfs.list.return_value = [mock_file_info]

            connector = get_connector("databricks", config)
            metadata = connector.list_objects(prefix="data/")

            self.assertEqual(len(metadata), 1)
            item = metadata[0]
            self.assertEqual(item.path, "dbfs:/data/file.parquet")
            self.assertEqual(item.size_bytes, 2048)
            self.assertEqual(item.source, "databricks")
            print("Databricks Connector Verified Successfully!")

if __name__ == "__main__":
    unittest.main()
