from abc import ABC, abstractmethod
from typing import List, Dict, Any
from ..models.metadata import FileMetadata

class BaseConnector(ABC):
    def __init__(self, config: Dict[str, Any]):
        self.config = config

    def _get_owner_from_tags(self, tags: Dict[str, str]) -> str:
        """
        Helper to robustly extract owner from tags, handling case sensitivity and whitespace.
        Looks for keys like "owner", "Owner", "owner ", " owner", etc.
        """
        if not tags:
            return None
        
        for k, v in tags.items():
            if k.strip().lower() == "owner":
                return v
        return None

    @abstractmethod
    def list_objects(self, prefix: str = "", recursive: bool = True) -> List[FileMetadata]:
        pass

    @abstractmethod
    def read_file(self, path: str) -> bytes:
        """
        Reads the content of a file.
        
        Args:
            path: The path/key of the file to read.
            
        Returns:
            The content of the file as bytes.
        """
        pass

    @abstractmethod
    def get_metadata(self, path: str) -> FileMetadata:
        """
        Retrieves metadata for a specific file.
        
        Args:
            path: The path/key of the file.
            
        Returns:
            A FileMetadata object.
        """
        pass

    @abstractmethod
    def get_container_metadata(self) -> Dict[str, Any]:
        """
        Retrieves metadata for the container/bucket itself.
        
        Returns:
            A dictionary containing container metadata.
        """
        pass

    @abstractmethod
    def get_account_metadata(self) -> Dict[str, Any]:
        """
        Retrieves metadata for the storage account (if applicable).
        
        Returns:
            A dictionary containing account metadata.
        """
        pass
