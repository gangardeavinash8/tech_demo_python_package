from databricks.sdk import WorkspaceClient
from typing import List, Dict, Any
from .base import BaseConnector
from ..models.metadata import FileMetadata
from datetime import datetime

class DatabricksConnector(BaseConnector):
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.host = config["databricks_host"]
        self.token = config["databricks_token"]
        self.client = WorkspaceClient(host=self.host, token=self.token)

    def list_objects(self, prefix: str = "") -> List[FileMetadata]:
        # DBFS paths start with dbfs:/
        # If prefix is not provided, start at root "dbfs:/"
        # However, listing root might be too much, usually we want a specific path.
        # Let's assume prefix acts as the path to list.
        
        path_to_list = prefix if prefix.startswith("dbfs:") else f"dbfs:{prefix}"
        if path_to_list == "dbfs:":
             path_to_list = "dbfs:/"
             
        results = []
        try:
            for file_info in self.client.dbfs.list(path_to_list):
                 results.append(FileMetadata(
                    path=file_info.path,
                    type="directory" if file_info.is_dir else "file",
                    size_bytes=file_info.file_size or 0,
                    owner=None, # DBFS API might not return owner
                    last_modified=datetime.fromtimestamp(file_info.modification_time / 1000) if file_info.modification_time else None,
                    last_accessed=None,
                    source="databricks"
                ))
        except Exception as e:
            # Handle case where path doesn't exist or other errors
             print(f"Error listing Databricks path {path_to_list}: {e}")
             
        return results

    def read_file(self, path: str) -> bytes:
        raise NotImplementedError("Databricks file reading is not yet implemented")

    def get_metadata(self, path: str) -> FileMetadata:
        raise NotImplementedError("Databricks metadata fetching is not yet implemented")

    def get_container_metadata(self) -> Dict[str, Any]:
        raise NotImplementedError("Databricks container metadata fetching is not yet implemented")

    def get_account_metadata(self) -> Dict[str, Any]:
        raise NotImplementedError("Databricks account metadata fetching is not yet implemented")
