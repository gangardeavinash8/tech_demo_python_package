import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Dict, Any

@dataclass
class FileMetadata:
    path: str
    type: str  # 'file' or 'directory'
    size_bytes: int
    last_modified: Optional[datetime]
    source: str
    owner: Optional[str] = None
    last_accessed: Optional[datetime] = None
    content_type: Optional[str] = None
    etag: Optional[str] = None
    tags: Dict[str, str] = field(default_factory=dict)
    extra_metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """
        Converts the metadata to a dictionary with serializable values.
        Datetimes are converted to ISO format strings.
        """
        base_dict = {
            "path": self.path,
            "type": self.type,
            "size_bytes": self.size_bytes,
            "last_modified": self.last_modified.isoformat() if self.last_modified else None,
            "source": self.source,
            "owner": self.owner,
            "last_accessed": self.last_accessed.isoformat() if self.last_accessed else None,
            "content_type": self.content_type,
            "etag": self.etag,
            "tag": self.tags,
        }
        
        # Merge extra_metadata into the base dict (Flattening)
        if self.extra_metadata:
            base_dict.update(self.extra_metadata)
            
        # Remove keys with None values to match clean screenshot style? 
        # User screenshot didn't show 'content_type': null.
        # Let's filter out None values.
        return {k: v for k, v in base_dict.items() if v is not None}

    def to_json(self) -> str:
        """
        Converts the metadata to a JSON string.
        """
        return json.dumps(self.to_dict(), indent=2)
