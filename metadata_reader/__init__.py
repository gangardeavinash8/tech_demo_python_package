from .factory import get_connector as connect
from .models.metadata import FileMetadata

__all__ = ["connect", "FileMetadata"]