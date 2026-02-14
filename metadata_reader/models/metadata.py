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
        Converts the metadata to a dictionary with exactly 7 fields.
        Includes resource tags as the 7th field.
        """
        # Strict 7 fields in the specific order requested
        return {
            "path": self.path,
            "type": self.type,
            "size_bytes": self.size_bytes,
            "owner": self.owner,
            "last_modified": self.last_modified.isoformat() if self.last_modified else None,
            "source": self.source,
            "tags": self.tags
        }

    def to_json(self) -> str:
        """
        Converts the metadata to a JSON string.
        """
        return json.dumps(self.to_dict(), indent=2)
