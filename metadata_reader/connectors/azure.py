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
        elif config.get("azure_subscription_id"):
            # Discovery-only mode
            self._init_credential(config)
            self.client = None
            self.datalake_client = None
        else:
            raise ValueError("Either AZURE_CONNECTION_STRING, AZURE_ACCOUNT_NAME, or AZURE_SUBSCRIPTION_ID (for discovery) must be provided.")

        if self.container:
            self.file_system_client = self.datalake_client.get_file_system_client(self.container)
        else:
            self.file_system_client = None
        
        # Initialize Management Client if credentials are provided
        self.mgmt_client = None
        if config.get("azure_subscription_id") and self.credential:
            self.mgmt_client = StorageManagementClient(self.credential, config["azure_subscription_id"])
            
        self._account_metadata_cache = None
        
        # Initial clients
        self._init_data_clients(config.get("azure_account_name"))

    def _init_data_clients(self, account_name: str = None):
        """Initializes or re-initializes blob and datalake clients for a specific account."""
        if self.config.get("connection_string"):
            self.client = BlobServiceClient.from_connection_string(self.config["connection_string"])
            self.datalake_client = DataLakeServiceClient.from_connection_string(self.config["connection_string"])
        elif account_name:
            account_url = f"https://{account_name}.blob.core.windows.net"
            dfs_url = f"https://{account_name}.dfs.core.windows.net"
            self.client = BlobServiceClient(account_url, credential=self.credential)
            self.datalake_client = DataLakeServiceClient(dfs_url, credential=self.credential)
        else:
            self.client = None
            self.datalake_client = None

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

    def list_objects(self, prefix: str = "", container: str = None, recursive: bool = True) -> List[FileMetadata]:
        target_container = container or self.container
        
        # Discovery Mode: If no container is specified, scan all accessible storage accounts and containers
        if not target_container:
            import os
            print("🔍 Discovering accessible Azure Storage Accounts...", file=os.sys.stderr)
            accounts = self.list_storage_accounts()
            print(f"Found {len(accounts)} accounts: {', '.join([a['name'] for a in accounts])}", file=os.sys.stderr)
            
            all_files = []
            for acct in accounts:
                try:
                    # Switch account context
                    print(f"  📂 Switching to account: {acct['name']}", file=os.sys.stderr)
                    original_acct = self.config.get("azure_account_name")
                    self.config["azure_account_name"] = acct["name"]
                    self._init_data_clients(acct["name"]) # IMPORTANT: Re-init data plane clients
                    
                    containers = self.list_containers(acct["name"])
                    print(f"  📦 Account {acct['name']}: Found {len(containers)} containers: {', '.join(containers)}", file=os.sys.stderr)
                    
                    # Add storage account entry itself to results
                    all_files.append(FileMetadata(
                        path=f"azure://{acct['name']}",
                        type="storage_account",
                        size_bytes=0,
                        last_modified=None,
                        source="azure",
                        owner=acct["tags"].get("owner") or acct["tags"].get("Owner"),
                        tags=acct["tags"],
                        extra_metadata={
                            "location": acct["location"],
                            "resource_group": acct["resource_group"]
                        }
                    ))
                    
                    for cont in containers:
                        try:
                            all_files.extend(self.list_objects(prefix=prefix, container=cont, recursive=recursive))
                        except Exception as e:
                            print(f"    ❌ Error scanning container {cont} in {acct['name']}: {e}", file=os.sys.stderr)
                            
                    # Restore original account context
                    self.config["azure_account_name"] = original_acct
                    self._init_data_clients(original_acct)
                    self._account_metadata_cache = None # Clear cache for next account
                except Exception as e:
                    print(f"  ❌ Error scanning account {acct['name']}: {e}", file=os.sys.stderr)
            return all_files
            
        container_client = self.client.get_container_client(target_container)
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

        # Use walk_blobs to get files and prefixes
        # Optimization: Include metadata in the initial listing
        # Avoid including 'tags' as it requires higher permissions (Data Owner) that may be missing
        walk_args = {
            "name_starts_with": prefix,
            "include": ['metadata', 'tags']
        }
        if not recursive:
            walk_args["delimiter"] = '/'

        try:
            blobs = container_client.walk_blobs(**walk_args)
        except Exception:
            # Fallback to no-include if metadata access is restricted
            if "include" in walk_args:
                del walk_args["include"]
            blobs = container_client.walk_blobs(**walk_args)

        for blob in blobs:
            if hasattr(blob, 'size'):
                # Regular Blob
                blob_metadata = getattr(blob, 'metadata', {}) or {}
                blob_tags = getattr(blob, 'tag_count', 0) > 0 and getattr(blob, 'tags', {}) or {}
                
                # Merge tags: blob tags take precedence over account tags
                final_tags = account_tags.copy()
                if blob_tags:
                    final_tags.update(blob_tags)

                owner = blob_tags.get('owner') or blob_tags.get('Owner') or \
                        blob_metadata.get('owner') or blob_metadata.get('Owner') or \
                        account_tags.get('owner') or account_tags.get('Owner')
                
                # Fetch POSIX owner if HNS is enabled
                if self.datalake_client:
                    try:
                        fs_client = self.datalake_client.get_file_system_client(target_container)
                        f_client = fs_client.get_file_client(blob.name)
                        acl = f_client.get_access_control()
                        owner = acl.get('owner') or owner
                    except Exception:
                        pass

                results.append(FileMetadata(
                    path=f"azure://{target_container}/{blob.name}",
                    type="file",
                    size_bytes=blob.size,
                    owner=owner,
                    last_modified=blob.last_modified,
                    source="blob_storage",
                    etag=getattr(blob, 'etag', None),
                    tags=final_tags
                ))
            else:
                # BlobPrefix (Directory)
                folder_size = self._calculate_folder_size(blob.name, target_container)
                # Fetch directory properties if datalake client is available
                folder_modified = None
                folder_metadata = {}
                folder_tags = account_tags.copy()
                folder_owner = None
                
                if self.datalake_client:
                    try:
                        # Ensure we have a file system client for this container
                        fs_client = self.datalake_client.get_file_system_client(target_container)
                        dir_client = fs_client.get_directory_client(blob.name.rstrip('/'))
                        dir_props = dir_client.get_directory_properties()
                        folder_modified = dir_props.last_modified
                        folder_metadata = dir_props.metadata or {}
                        
                        # Fetch POSIX owner
                        try:
                            acl = dir_client.get_access_control()
                            folder_owner = acl.get('owner')
                        except Exception:
                            pass

                        # Try to fetch tags via Blob API for the directory path
                        try:
                            temp_blob_client = container_client.get_blob_client(blob.name.rstrip('/'))
                            tags = temp_blob_client.get_blob_tags()
                            if tags:
                                folder_tags.update(tags)
                        except Exception:
                            pass
                    except Exception:
                        # Directory might not be a real object (non-HNS)
                        pass

                results.append(FileMetadata(
                    path=f"azure://{target_container}/{blob.name}",
                    type="directory",
                    size_bytes=folder_size,
                    owner=folder_owner or folder_metadata.get('owner') or folder_metadata.get('Owner'),
                    last_modified=folder_modified,
                    source="blob_storage",
                    tags=folder_tags
                ))

        return results

    def read_file(self, path: str, container: str = None) -> bytes:
        # Expect path to be like azure://container/blob_name or just blob_name
        # If it's the full path we construct, it's "azure://{container}/{blob.name}"
        
        target_container = container or self.container
        # Simple parsing logic
        prefix = f"azure://{target_container}/"
        if path.startswith(prefix):
            blob_name = path[len(prefix):]
        else:
            blob_name = path
            
        blob_client = self.client.get_blob_client(container=target_container, blob=blob_name)
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
        
        # Try to fetch owner using DataLake client if possible (HNS), but avoid fetching full ACL
        owner = None
        try:
             # We skip get_access_control() as per request to not fetch access control
             pass
        except Exception:
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
            "metadata": props.metadata,
            "source": "azure"
        }

    def get_account_metadata(self) -> Dict[str, Any]:
        if self._account_metadata_cache:
            return self._account_metadata_cache

        info = {}
        try:
            info = self.client.get_account_information()
        except Exception as e:
            print(f"Warning: Data Plane access failed (check RBAC roles): {e}")
        
        tags = self.config.get("azure_account_tags") or {}
        mgmt_props = {}
        
        # Determine resource group and account name
        rg = self.config.get("azure_resource_group")
        account_name = self.config.get("azure_account_name")

        if self.mgmt_client and rg and account_name:
            try:
                account = self.mgmt_client.storage_accounts.get_properties(rg, account_name)
                # Merge management plane tags with any passed-in tags
                if account.tags:
                    new_tags = account.tags.copy()
                    new_tags.update(tags)
                    tags = new_tags
                
                mgmt_props = {
                    "location": account.location,
                    "id": account.id,
                    "type": account.type,
                    "provisioning_state": account.provisioning_state,
                    "creation_time": str(account.creation_time) if hasattr(account, 'creation_time') else None,
                    "primary_endpoints": account.primary_endpoints.as_dict() if account.primary_endpoints else None
                }
            except Exception as e:
                print(f"Warning: Failed to fetch account tags/props for {account_name} in {rg}: {e}")

        self._account_metadata_cache = {
            "sku_name": info.get('sku_name'),
            "account_kind": info.get('account_kind'),
            "is_hns_enabled": info.get('is_hns_enabled'),
            "tags": tags,
            "management_properties": mgmt_props,
            "source": "azure"
        }
        return self._account_metadata_cache

    def _calculate_folder_size(self, prefix: str, container: str = None) -> int:
        """Calculates total size of a folder by traversing its contents."""
        target_container = container or self.container
        if not target_container: return 0
        container_client = self.client.get_container_client(target_container)
        total_size = 0
        try:
            for blob in container_client.list_blobs(name_starts_with=prefix):
                 total_size += blob.size
        except Exception:
            pass
        return total_size

    def list_storage_accounts(self) -> List[Dict[str, Any]]:
        """Lists all storage accounts with their metadata."""
        if not self.mgmt_client:
            raise ValueError("Management client not initialized. Ensure subscription_id and credentials are provided.")
        
        accounts = []
        try:
            for account in self.mgmt_client.storage_accounts.list():
                # Extact Resource Group from ID
                # ID format: /subscriptions/{sub}/resourceGroups/{rg}/providers/Microsoft.Storage/storageAccounts/{name}
                rg = "unknown"
                if account.id and "/resourceGroups/" in account.id:
                    rg = account.id.split("/resourceGroups/")[1].split("/")[0]
                
                accounts.append({
                    "name": account.name,
                    "resource_group": rg,
                    "tags": account.tags or {},
                    "location": account.location
                })
        except Exception as e:
            print(f"Error listing storage accounts: {e}")
            
        return accounts

    def list_containers(self, account_name: str = None) -> List[str]:
        """Lists all containers in a storage account. Requires Data Plane access."""
        # Note: This requires full BlobServiceClient for the specific account
        # If account_name is provided, we might need a temporary client
        client = self.client
        if account_name and account_name != self.config.get("azure_account_name"):
             account_url = f"https://{account_name}.blob.core.windows.net"
             client = BlobServiceClient(account_url, credential=self.credential)
             
        if not client:
             return []
             
        containers = []
        try:
            for container in client.list_containers():
                containers.append(container.name)
        except Exception as e:
            print(f"Error listing containers: {e}")
            
        return containers

class AzureConnectorBuilder:
    def __init__(self):
        self._config = {}

    def connection_string(self, conn_str: str):
        self._config["connection_string"] = conn_str
        return self

    def container(self, container: str):
        self._config["container"] = container
        return self

    def subscription_id(self, sub_id: str):
        self._config["azure_subscription_id"] = sub_id
        return self

    def client_id(self, client_id: str):
        self._config["azure_client_id"] = client_id
        return self

    def client_secret(self, secret: str):
        self._config["azure_client_secret"] = secret
        return self

    def tenant_id(self, tenant_id: str):
        self._config["azure_tenant_id"] = tenant_id
        return self

    def resource_group(self, rg: str):
        self._config["azure_resource_group"] = rg
        return self

    def account_name(self, account_name: str):
        self._config["azure_account_name"] = account_name
        return self

    def account_tags(self, tags: Dict[str, str]):
        self._config["azure_account_tags"] = tags
        return self

    def build(self) -> AzureConnector:
        if not any([self._config.get("connection_string"), 
                   self._config.get("azure_account_name"),
                   self._config.get("azure_subscription_id")]):
            raise ValueError("Azure Connection String, Account Name, or Subscription ID must be provided.")
        return AzureConnector(self._config)
