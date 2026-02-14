from typing import List, Dict, Any
import requests
import os
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

    def list_sites(self, search: str = "*") -> List[Dict[str, str]]:
        """Lists all accessible SharePoint sites."""
        headers = self._get_headers()
        url = f"https://graph.microsoft.com/v1.0/sites?search={search}"
        sites = []
        try:
            while url:
                resp = requests.get(url, headers=headers)
                resp.raise_for_status()
                data = resp.json()
                for site in data.get("value", []):
                    sites.append({
                        "id": site["id"],
                        "name": site.get("displayName") or site.get("name"),
                        "webUrl": site.get("webUrl")
                    })
                url = data.get("@odata.nextLink")
        except Exception as e:
            print(f"Error listing SharePoint sites: {e}")
        return sites

    def _get_token(self) -> str:
        token = self.credential.get_token("https://graph.microsoft.com/.default")
        return token.token

    def _get_headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self._get_token()}",
            "Content-Type": "application/json"
        }

    def list_objects(self, prefix: str = "", site_id: str = None, drive_id: str = None) -> List[FileMetadata]:
        headers = self._get_headers()
        results = []
        
        target_site_id = site_id or self.site_id
        target_drive_id = drive_id or self.drive_id

        # Discovery Mode: If no site_id is specified, scan all accessible sites
        if not target_site_id:
            import os
            print("🔍 Discovering accessible SharePoint Sites...", file=os.sys.stderr)
            sites = self.list_sites()
            print(f"Found {len(sites)} sites: {', '.join([s['name'] for s in sites])}", file=os.sys.stderr)
            
            all_files = []
            for site in sites:
                try:
                    all_files.extend(self.list_objects(prefix=prefix, site_id=site["id"], drive_id=drive_id))
                except Exception as e:
                    print(f"  ❌ Error scanning site {site['name']}: {e}", file=os.sys.stderr)
            return all_files

        # If drive_id is not provided, list all drives (Document Libraries) and their children
        drives = []
        if target_drive_id:
            drives = [{"id": target_drive_id}]
        else:
            # Fetch default drive or all drives
            url = f"https://graph.microsoft.com/v1.0/sites/{target_site_id}/drives"
            resp = requests.get(url, headers=headers)
            resp.raise_for_status()
            drives = resp.json().get("value", [])

        for drive in drives:
            d_id = drive["id"]
            
            # Recursively list children
            url = f"https://graph.microsoft.com/v1.0/sites/{target_site_id}/drives/{d_id}/root/children"
            self._fetch_children(url, headers, target_site_id, d_id, results)

        return results

    def _fetch_children(self, url: str, headers: Dict[str, str], site_id: str, drive_id: str, results: List[FileMetadata]):
        while url:
            resp = requests.get(url, headers=headers)
            if resp.status_code != 200:
                break
                
            data = resp.json()
            items = data.get("value", [])
            
            for item in items:
                if "file" in item:
                    results.append(self._map_item_to_metadata(item, site_id, drive_id))
                elif "folder" in item:
                    # Calculate folder size via traversal
                    folder_size = self._calculate_folder_size(item['id'], headers, site_id, drive_id)
                    results.append(self._map_item_to_metadata(item, site_id, drive_id, folder_size_override=folder_size))
            
            url = data.get("@odata.nextLink")

    def _map_item_to_metadata(self, item: Dict[str, Any], site_id: str, drive_id: str, folder_size_override: int = None) -> FileMetadata:
        # Map Graph API Item to FileMetadata
        user = item.get("createdBy", {}).get("user", {}).get("displayName")
        
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
            path=f"sharepoint://{site_id}/{drive_id}/{item.get('name')}",
            type="directory" if "folder" in item else "file",
            size_bytes=folder_size_override if folder_size_override is not None else item.get("size", 0),
            last_modified=last_modified, 
            source="sharepoint",
            owner=user,
            etag=item.get("eTag")
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

    def _calculate_folder_size(self, item_id: str, headers: Dict[str, str], site_id: str, drive_id: str) -> int:
        """Calculates total size of a folder by traversing its contents."""
        total_size = 0
        url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/items/{item_id}/children"
        
        while url:
            try:
                response = requests.get(url, headers=headers)
                if response.status_code != 200:
                    break
                data = response.json()
                items = data.get("value", [])
                for item in items:
                    if "file" in item:
                        total_size += item.get("size", 0)
                    elif "folder" in item:
                        total_size += self._calculate_folder_size(item['id'], headers, site_id, drive_id)
                
                url = data.get("@odata.nextLink")
            except Exception:
                break
            
        return total_size

class SharePointConnectorBuilder:
    def __init__(self):
        self._config = {}

    def tenant_id(self, tenant_id: str):
        self._config["sharepoint_tenant_id"] = tenant_id
        return self

    def client_id(self, client_id: str):
        self._config["sharepoint_client_id"] = client_id
        return self

    def client_secret(self, client_secret: str):
        self._config["sharepoint_client_secret"] = client_secret
        return self

    def site_id(self, site_id: str):
        self._config["sharepoint_site_id"] = site_id
        return self

    def site_url(self, site_url: str):
        self._config["sharepoint_site_url"] = site_url
        return self

    def drive_id(self, drive_id: str):
        self._config["sharepoint_drive_id"] = drive_id
        return self

    def build(self) -> SharePointConnector:
        if not all([self._config.get("sharepoint_tenant_id"), 
                    self._config.get("sharepoint_client_id"), 
                    self._config.get("sharepoint_client_secret")]):
             raise ValueError("SharePoint tenant_id, client_id, and client_secret are required.")
        return SharePointConnector(self._config)
