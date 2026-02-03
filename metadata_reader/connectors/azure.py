from azure.storage.blob import BlobServiceClient
from azure.storage.filedatalake import DataLakeServiceClient
from azure.mgmt.storage import StorageManagementClient
from azure.mgmt.authorization import AuthorizationManagementClient
from azure.identity import DefaultAzureCredential, ClientSecretCredential
import requests
from typing import List, Dict, Any
from .base import BaseConnector
from ..models.metadata import FileMetadata

class AzureConnector(BaseConnector):
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.container = config.get("container")
        
        # Initialize Data Plane Clients (Blob + DataLake)
        if config.get("connection_string"):
            self.client = BlobServiceClient.from_connection_string(config["connection_string"])
            self.datalake_client = DataLakeServiceClient.from_connection_string(config["connection_string"])
        elif config.get("azure_account_name"):
            # Use Token Auth (SP credentials)
            account_url = f"https://{config['azure_account_name']}.blob.core.windows.net"
            dfs_url = f"https://{config['azure_account_name']}.dfs.core.windows.net"
            
            self._init_credential(config)
            
            self.client = BlobServiceClient(account_url, credential=self.credential)
            self.datalake_client = DataLakeServiceClient(dfs_url, credential=self.credential)
        else:
            raise ValueError("Either AZURE_CONNECTION_STRING or AZURE_ACCOUNT_NAME+Credentials must be provided.")

        if self.container:
            self.file_system_client = self.datalake_client.get_file_system_client(self.container)
        else:
            self.file_system_client = None
        
        # Initialize Management Client if credentials are provided
        self.mgmt_client = None
        if config.get("azure_subscription_id") and self.credential:
            self.mgmt_client = StorageManagementClient(self.credential, config["azure_subscription_id"])

    def _init_credential(self, config: Dict[str, Any]):
        """Helper to initialize the best available credential"""
        if (config.get("azure_client_id") and 
            config.get("azure_client_secret") and 
            config.get("azure_tenant_id")):
            # PREFER explicit config values if present
            self.credential = ClientSecretCredential(
                tenant_id=config["azure_tenant_id"],
                client_id=config["azure_client_id"],
                client_secret=config["azure_client_secret"]
            )
        else:
            # Fallback to Environment Variables / Managed Identity
            self.credential = DefaultAzureCredential()

    def list_objects(self, prefix: str = "") -> List[FileMetadata]:
        if not self.container:
            raise ValueError("Container not configured. Cannot list objects.")
            
        container_client = self.client.get_container_client(self.container)
        results = []

        # Try to fetch account tags/props once to apply to all files
        account_tags = {}
        account_props = {}
        try:
            # We use get_account_metadata which now safely handles missing permissions
            acct_meta = self.get_account_metadata()
            account_tags = acct_meta.get("tags") or {}
            account_props = acct_meta.get("management_properties") or {}
        except Exception:
            pass

        # name_starts_with acts as a prefix filter
        for blob in container_client.list_blobs(name_starts_with=prefix):
            results.append(FileMetadata(
                path=f"azure://{self.container}/{blob.name}",
                type="file", # Blobs are files
                size_bytes=blob.size,
                owner=account_tags.get('owner') or account_tags.get('Owner'), # Fallback to account owner
                last_modified=blob.last_modified,
                last_accessed=blob.last_accessed_on,
                source="blob_storage",
                tags=account_tags,  # Apply account level tags to file
                extra_metadata=account_props # Include detailed account props
            ))

        return results

    def read_file(self, path: str) -> bytes:
        # Expect path to be like azure://container/blob_name or just blob_name
        # If it's the full path we construct, it's "azure://{container}/{blob.name}"
        
        # Simple parsing logic
        prefix = f"azure://{self.container}/"
        if path.startswith(prefix):
            blob_name = path[len(prefix):]
        else:
            blob_name = path
            
        blob_client = self.client.get_blob_client(container=self.container, blob=blob_name)
        return blob_client.download_blob().readall()

    def get_metadata(self, path: str) -> FileMetadata:
        # Simple parsing logic
        prefix = f"azure://{self.container}/"
        if path.startswith(prefix):
            blob_name = path[len(prefix):]
        else:
            blob_name = path
            
        blob_client = self.client.get_blob_client(container=self.container, blob=blob_name)
        props = blob_client.get_blob_properties()
        
        # Try to fetch owner using DataLake client (requires HNS)
        owner = None
        try:
            file_client = self.file_system_client.get_file_client(blob_name)
            acl = file_client.get_access_control()
            owner = acl.get('owner')
        except Exception:
            # Fallback if HNS is not enabled or ACLs are inaccessible
            pass
        
        # Priority 2: Check Blob Tags (Index Tags)
        if not owner or owner == '$superuser':
            try:
                tags = blob_client.get_blob_tags()
                owner = tags.get('owner') or tags.get('Owner') or owner
            except Exception:
                pass
        
        # Priority 3: Check Container Metadata
        if not owner or owner == '$superuser':
            # Check container metadata (which can store 'tags' like owner)
            try:
                container_client = self.client.get_container_client(self.container)
                container_props = container_client.get_container_properties()
                # Check for 'owner' or 'Owner' in metadata
                meta = container_props.metadata or {}
                owner = meta.get('owner') or meta.get('Owner') or owner
            except Exception:
                pass

        # Priority 4: Check Account Tags (Management Plane)
        account_tags = {}
        account_props = {}
        try:
            account_meta = self.get_account_metadata()
            account_tags = account_meta.get('tags') or {}
            account_props = account_meta.get('management_properties') or {}
            
            if not owner or owner == '$superuser':
                owner = account_tags.get('owner') or account_tags.get('Owner') or owner
        except Exception:
            pass
        
        # Merge extra_metadata from blob props with account props
        # Blob props take precedence if key collision (unlikely)
        final_extra_metadata = account_props.copy()
        if props.metadata:
            final_extra_metadata.update(props.metadata)

        return FileMetadata(
            path=path,
            type="file",
            size_bytes=props.size,
            last_modified=props.last_modified,
            source="blob_storage",
            owner=owner,
            content_type=props.content_settings.content_type,
            etag=props.etag,
            tags=account_tags, # Include account tags in output
            extra_metadata=final_extra_metadata
        )

    def get_container_metadata(self) -> Dict[str, Any]:
        if not self.container:
             raise ValueError("Container not configured. Cannot get container metadata.")
             
        container_client = self.client.get_container_client(self.container)
        props = container_client.get_container_properties()
        
        return {
            "name": props.name,
            "last_modified": props.last_modified,
            "etag": props.etag,
            "lease_status": props.lease.status,
            "lease_state": props.lease.state,
            "public_access": props.public_access,
            "metadata": props.metadata,
            "source": "azure"
        }

    def get_account_metadata(self) -> Dict[str, Any]:
        info = {}
        try:
            info = self.client.get_account_information()
        except Exception as e:
            print(f"Warning: Data Plane access failed (check RBAC roles): {e}")
        
        tags = {}
        mgmt_props = {}
        if self.mgmt_client and self.config.get("azure_resource_group") and self.config.get("azure_account_name"):
            try:
                account = self.mgmt_client.storage_accounts.get_properties(
                    self.config["azure_resource_group"],
                    self.config["azure_account_name"]
                )
                tags = account.tags or {}
                
                # Fetch IAM Role Assignments
                role_assignments = []
                try:
                    # Scope for the storage account
                    scope = account.id
                    auth_client = AuthorizationManagementClient(self.credential, self.config["azure_subscription_id"])
                    
                    # Store role definitions cache
                    role_defs = {}
                    
                    for assignment in auth_client.role_assignments.list_for_scope(scope):
                        # Get Role Name (display name like 'Owner')
                        role_id = assignment.role_definition_id
                        if role_id not in role_defs:
                            try:
                                # Role ID is usually full resource path, we need the last part (GUID) or use get_by_id
                                # Actually get_by_id takes the full ID
                                role_def = auth_client.role_definitions.get_by_id(role_id)
                                role_defs[role_id] = role_def.role_name
                            except:
                                role_defs[role_id] = "Unknown Role"
                        
                        role_assignments.append({
                            "role_assignment_id": assignment.id,
                            "role": role_defs[role_id],
                            "principal_id": assignment.principal_id,
                            "principal_type": assignment.principal_type,
                            "scope": assignment.scope
                        })
                    
                    # Resolve Principal Names using Graph API
                    try:
                        principal_ids = [ra["principal_id"] for ra in role_assignments]
                        if principal_ids:
                            names = self._resolve_principal_names(principal_ids)
                            for ra in role_assignments:
                                pid = ra["principal_id"]
                                if pid in names:
                                    ra["principal_name"] = names[pid]
                    except Exception as e:
                        print(f"Warning: Failed to resolve principal names (Graph API): {e}")

                except Exception as e:
                    print(f"Warning: Failed to fetch IAM roles: {e}")

                mgmt_props = {
                    "location": account.location,
                    "id": account.id,
                    "type": account.type,
                    "provisioning_state": account.provisioning_state,
                    "creation_time": str(account.creation_time) if hasattr(account, 'creation_time') else None,
                    "primary_endpoints": account.primary_endpoints.as_dict() if account.primary_endpoints else None,
                    "role_assignments": role_assignments
                }
            except Exception as e:
                print(f"Warning: Failed to fetch account tags/props: {e}")

        return {
            "sku_name": info.get('sku_name'),
            "account_kind": info.get('account_kind'),
            "is_hns_enabled": info.get('is_hns_enabled'),
            "tags": tags,
            "management_properties": mgmt_props,
            "source": "azure"
        }

    def _resolve_principal_names(self, principal_ids: List[str]) -> Dict[str, str]:
        """
        Resolves a list of Principal IDs to display names using Microsoft Graph API.
        Requires Directory.Read.All or User.Read.All permissions.
        """
        if not self.credential:
            return {}

        # Get Graph Token
        token = self.credential.get_token("https://graph.microsoft.com/.default")
        headers = {
            "Authorization": f"Bearer {token.token}",
            "Content-Type": "application/json"
        }

        # Use directoryObjects/getByIds for batch resolution
        # Limit is 1000 items per request
        url = "https://graph.microsoft.com/v1.0/directoryObjects/getByIds"
        
        # Deduplicate IDs
        unique_ids = list(set(principal_ids))
        name_map = {}

        # Simple batching (assuming < 1000 for now)
        payload = {
            "ids": unique_ids,
            "types": ["user", "group", "servicePrincipal"]
        }
        
        resp = requests.post(url, headers=headers, json=payload)
        resp.raise_for_status()
        
        data = resp.json()
        for item in data.get("value", []):
            pid = item.get("id")
            # displayName is common, specific types might optionally have other fields
            name = item.get("displayName") or item.get("userPrincipalName") or item.get("appDisplayName")
            if pid and name:
                name_map[pid] = name
                
        return name_map
