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
        self.catalog = config.get("databricks_catalog")
        self.schema = config.get("databricks_schema")
        self.volume = config.get("databricks_volume")
        import os # Ensure os is available for join
        
        if not self.host or not self.token:
             raise ValueError("Databricks Host and Token are required.")
             
        self.client = WorkspaceClient(host=self.host, token=self.token)

    def list_objects(self, prefix: str = "", catalog: str = None, schema: str = None, volume: str = None) -> List[FileMetadata]:
        """
        Lists files from the configured Unity Catalog Volume.
        Path format: /Volumes/catalog/schema/volume
        """
        target_catalog = catalog or self.catalog
        target_schema = schema or self.schema
        target_volume = volume or self.volume

        if not target_catalog or not target_schema or not target_volume:
             if prefix.startswith("dbfs:"):
                 return self._list_dbfs(prefix)
             raise ValueError("Databricks Catalog, Schema, and Volume must be configured or provided as overrides.")

        # Construct Volume Root Path
        volume_root = f"/Volumes/{target_catalog}/{target_schema}/{target_volume}"
        
        # If prefix provided, append it (handle leading slashes)
        search_path = volume_root
        if prefix:
             search_path = os.path.join(volume_root, prefix.lstrip("/"))
             
        results = []
        try:
            # Recursive listing helper
            self._fetch_volume_files(search_path, results)
        except Exception as e:
             print(f"Error listing Databricks path {search_path}: {e}")
             
        return results

    def _fetch_volume_files(self, path: str, results: List[FileMetadata]):
        # Databricks SDK for Files API (Unity Catalog Volumes)
        # client.files.list_directory_contents(path)
        try:
            for item in self.client.files.list_directory_contents(path):
                if item.is_directory:
                    self._fetch_volume_files(item.path, results)
                else:
                    # modification_time might be missing or named differently
                    mod_time = None
                    if hasattr(item, 'modification_time'):
                        mod_time = datetime.fromtimestamp(item.modification_time / 1000)
                    elif hasattr(item, 'last_modified'): # common alternative
                         # Validated: item.last_modified is int (epoch ms)
                         mod_time = datetime.fromtimestamp(item.last_modified / 1000)
                    
                    results.append(FileMetadata(
                        path=item.path, # e.g. /Volumes/main/default/myvol/file.txt
                        type="file",
                        size_bytes=item.file_size or 0,
                        last_modified=mod_time,
                        source="databricks_volume",
                        owner=None # SDK doesn't always return owner for volume files
                    ))
        except Exception as e:
             print(f"Error scanning directory {path}: {e}")

    def _list_dbfs(self, prefix: str) -> List[FileMetadata]:
        # Legacy DBFS support (kept for backward compatibility)
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
                    owner=None,
                    last_modified=datetime.fromtimestamp(file_info.modification_time / 1000) if file_info.modification_time else None,
                    source="databricks_dbfs"
                ))
        except Exception as e:
             print(f"Error listing Databricks DBFS path {path_to_list}: {e}")
        return results

    def read_file(self, path: str) -> bytes:
        raise NotImplementedError("Databricks file reading is not yet implemented")

    def get_metadata(self, path: str) -> FileMetadata:
        raise NotImplementedError("Databricks metadata fetching is not yet implemented")

    def get_container_metadata(self) -> Dict[str, Any]:
        raise NotImplementedError("Databricks container metadata fetching is not yet implemented")

    def get_account_metadata(self) -> Dict[str, Any]:
        raise NotImplementedError("Databricks account metadata fetching is not yet implemented")

class DatabricksConnectorBuilder:
    def __init__(self):
        self._config = {}

    def host(self, host: str):
        self._config["databricks_host"] = host
        return self

    def token(self, token: str):
        self._config["databricks_token"] = token
        return self

    def catalog(self, catalog: str):
        self._config["databricks_catalog"] = catalog
        return self

    def schema(self, schema: str):
        self._config["databricks_schema"] = schema
        return self

    def volume(self, volume: str):
        self._config["databricks_volume"] = volume
        return self

    def build(self) -> DatabricksConnector:
        if not self._config.get("databricks_host") or not self._config.get("databricks_token"):
            raise ValueError("Databricks Host and Token are required.")
        return DatabricksConnector(self._config)
