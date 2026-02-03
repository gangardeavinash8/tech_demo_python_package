import unittest
from unittest.mock import MagicMock, patch
from datetime import datetime
from metadata_reader.connectors.s3 import S3Connector
from metadata_reader.connectors.azure import AzureConnector
from metadata_reader.connectors.databricks import DatabricksConnector
from metadata_reader.models.metadata import FileMetadata

class TestConnectors(unittest.TestCase):

    @patch("boto3.client")
    def test_s3_connector(self, mock_boto):
        # Mock S3 response
        mock_s3 = MagicMock()
        mock_boto.return_value = mock_s3
        mock_s3.list_objects_v2.return_value = {
            "Contents": [
                {
                    "Key": "test_file.txt",
                    "Size": 1024,
                    "LastModified": datetime(2023, 1, 1)
                }
            ]
        }

        config = {
            "bucket": "test-bucket",
            "aws_access_key_id": "test",
            "aws_secret_access_key": "test",
            "region": "us-east-1"
        }
        connector = S3Connector(config)
        results = connector.list_objects()

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].path, "s3://test-bucket/test_file.txt")
        self.assertEqual(results[0].size_bytes, 1024)
        self.assertEqual(results[0].source, "s3")

    @patch("azure.storage.blob.BlobServiceClient.from_connection_string")
    def test_azure_connector(self, mock_service_client):
        # Mock Azure response
        mock_client = MagicMock()
        mock_service_client.return_value = mock_client
        mock_container_client = MagicMock()
        mock_client.get_container_client.return_value = mock_container_client

        mock_blob = MagicMock()
        mock_blob.name = "test_blob.txt"
        mock_blob.size = 2048
        mock_blob.last_modified = datetime(2023, 1, 2)
        mock_blob.last_accessed_on = datetime(2023, 1, 3)
        
        mock_container_client.list_blobs.return_value = [mock_blob]

        config = {
            "connection_string": "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=test;",
            "container": "test-container"
        }
        connector = AzureConnector(config)
        results = connector.list_objects()

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].path, "azure://test-container/test_blob.txt")
        self.assertEqual(results[0].size_bytes, 2048)
        self.assertEqual(results[0].source, "blob_storage")

    @patch("metadata_reader.connectors.databricks.WorkspaceClient")
    def test_databricks_connector(self, mock_ws_client):
        # Mock Databricks response
        mock_client = MagicMock()
        mock_ws_client.return_value = mock_client
        
        mock_file_info = MagicMock()
        mock_file_info.path = "dbfs:/test/file.txt"
        mock_file_info.is_dir = False
        mock_file_info.file_size = 512
        mock_file_info.modification_time = 1672531200000 # 2023-01-01
        
        mock_client.dbfs.list.return_value = [mock_file_info]

        config = {
            "databricks_host": "https://test-databricks.com",
            "databricks_token": "test-token"
        }
        connector = DatabricksConnector(config)
        results = connector.list_objects(prefix="/test")

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].path, "dbfs:/test/file.txt")
        self.assertEqual(results[0].size_bytes, 512)
        self.assertEqual(results[0].source, "databricks")
