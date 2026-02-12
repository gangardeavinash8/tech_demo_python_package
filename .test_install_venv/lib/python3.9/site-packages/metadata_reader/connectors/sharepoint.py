from typing import List, Dict, Any
import requests
from azure.identity import ClientSecretCredential
from .base import BaseConnector
from ..models.metadata import FileMetadata

class SharePointConnector(BaseConnector):
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.tenant_id = config.get("sharepoint_tenant_id")
        self.client_id = config.get("sharepoint_client_id")
        self.client_secret = config.get("sharepoint_client_secret")
        self.site_id = config.get("sharepoint_site_id")
        self.site_url = config.get("sharepoint_site_url") # New config
        self.drive_id = config.get("sharepoint_drive_id") 

        if not all([self.tenant_id, self.client_id, self.client_secret]):
             raise ValueError("SharePoint configuration missing (tenant_id, client_id, client_secret).")

        self.credential = ClientSecretCredential(
            tenant_id=self.tenant_id,
            client_id=self.client_id,
            client_secret=self.client_secret
        )
        
        # Resolve Site ID if only URL is provided
        if not self.site_id and self.site_url:
            self.site_id = self._resolve_site_id_from_url(self.site_url)
            
    def _resolve_site_id_from_url(self, url: str) -> str:
        try:
            # Simple parsing: remove https://, split by first /
            clean_url = url.replace("https://", "").replace("http://", "").rstrip("/")
            parts = clean_url.split("/", 1)
            hostname = parts[0]
            relative_path = parts[1] if len(parts) > 1 else ""
            
            # Construct Graph API URL
            api_url = f"https://graph.microsoft.com/v1.0/sites/{hostname}:/{relative_path}"
            
            token = self._get_token()
            headers = {"Authorization": f"Bearer {token}"}
            
            resp = requests.get(api_url, headers=headers)
            
            if resp.status_code == 200:
                data = resp.json()
                return data["id"]
            else:
                print(f"Failed to resolve Site ID from URL: {resp.text}")
                return None
        except Exception as e:
            print(f"Error resolving Site ID: {e}")
            return None

    def _get_token(self) -> str:
        token = self.credential.get_token("https://graph.microsoft.com/.default")
        return token.token

    def _get_headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self._get_token()}",
            "Content-Type": "application/json"
        }

    def list_objects(self, prefix: str = "") -> List[FileMetadata]:
        headers = self._get_headers()
        results = []
        
        # If drive_id is not provided, list all drives (Document Libraries) and their children
        drives = []
        if self.drive_id:
            drives = [{"id": self.drive_id}]
        else:
            # Fetch default drive or all drives
            url = f"https://graph.microsoft.com/v1.0/sites/{self.site_id}/drives"
            resp = requests.get(url, headers=headers)
            resp.raise_for_status()
            drives = resp.json().get("value", [])

        for drive in drives:
            drive_id = drive["id"]
            # drive_name = drive.get("name", "Unknown Drive")
            # print(f"DEBUG: Scanning Drive: {drive_name} ({drive_id})", flush=True)
            
            # Recursively list children
            url = f"https://graph.microsoft.com/v1.0/sites/{self.site_id}/drives/{drive_id}/root/children"
            self._fetch_children(url, headers, drive_id, results)

        return results

    def _fetch_children(self, url: str, headers: Dict[str, str], drive_id: str, results: List[FileMetadata]):
        while url:
            resp = requests.get(url, headers=headers)
            if resp.status_code != 200:
                # print(f"Error fetching SharePoint items: {resp.text}", flush=True)
                break
                
            data = resp.json()
            items = data.get("value", [])
            # print(f"DEBUG: Found {len(items)} items in current page.", flush=True)
            
            for item in items:
                # print(f"DEBUG: Found item: {item.get('name')} (Type: {'folder' if 'folder' in item else 'file'})", flush=True)
                if "file" in item:
                    results.append(self._map_item_to_metadata(item, drive_id))
                elif "folder" in item:
                    # Basic recursion
                    child_url = f"https://graph.microsoft.com/v1.0/sites/{self.site_id}/drives/{drive_id}/items/{item['id']}/children"
                    self._fetch_children(child_url, headers, drive_id, results)
            
            url = data.get("@odata.nextLink")

    def _map_item_to_metadata(self, item: Dict[str, Any], drive_id: str) -> FileMetadata:
        # Map Graph API Item to FileMetadata
        # User: createdBy, lastModifiedBy
        user = item.get("createdBy", {}).get("user", {}).get("displayName")
        
        # Extra metadata
        extra = {
            "drive_id": drive_id,
            "item_id": item["id"],
            "web_url": item.get("webUrl"),
            "created_by": item.get("createdBy"),
            "last_modified_by": item.get("lastModifiedBy")
        }

        
        # Parse date string to datetime object
        last_modified = None
        if item.get("lastModifiedDateTime"):
            try:
                from datetime import datetime
                # Python 3.11+ handles ISO 8601 strings well, but Graph might return 'Z'
                dt_str = item.get("lastModifiedDateTime").replace("Z", "+00:00")
                last_modified = datetime.fromisoformat(dt_str)
            except Exception:
                pass

        return FileMetadata(
            path=f"sharepoint://{self.site_id}/{drive_id}/{item.get('name')}",
            type="file",
            size_bytes=item.get("size", 0),
            last_modified=last_modified, 
            source="sharepoint",
            owner=user,
            content_type=item.get("file", {}).get("mimeType"),
            etag=item.get("eTag"),
            extra_metadata=extra
        )

    def read_file(self, path: str) -> bytes:
        # Path format: sharepoint://site_id/drive_id/filename (simplified)
        # Verify path parsing or assume we have drive_id and item_id if we want efficient lookup
        # For now, not implementing read_file complex logic without item_id
        raise NotImplementedError("Reading files from SharePoint by path is not fully implemented yet.")

    def get_metadata(self, path: str) -> FileMetadata:
        raise NotImplementedError("Get metadata by path is not fully implemented yet.")

    def get_container_metadata(self) -> Dict[str, Any]:
         return {"source": "sharepoint", "site_id": self.site_id}

    def get_account_metadata(self) -> Dict[str, Any]:
         return {"source": "sharepoint", "tenant_id": self.tenant_id}
