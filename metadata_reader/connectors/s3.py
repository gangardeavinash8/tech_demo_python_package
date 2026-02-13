import boto3
from typing import List, Dict, Any
from .base import BaseConnector
from ..models.metadata import FileMetadata

class S3Connector(BaseConnector):
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.bucket = config["bucket"]
        self.client = boto3.client(
            "s3",
            aws_access_key_id=config.get("aws_access_key_id"),
            aws_secret_access_key=config.get("aws_secret_access_key"),
            region_name=config.get("region")
        )

    def list_objects(self, prefix: str = "", bucket: str = None) -> List[FileMetadata]:
        target_bucket = bucket or self.bucket
        if not target_bucket:
             raise ValueError("Bucket not configured and no override provided.")

        # Fetch Context Data (Tags, Permissions, Users)
        bucket_tags = {}
        try:
            tag_resp = self.client.get_bucket_tagging(Bucket=target_bucket)
            bucket_tags = {t["Key"].strip(): t["Value"] for t in tag_resp.get("TagSet", [])}
        except Exception:
            pass

        access_control = self._get_bucket_permissions(target_bucket)

        # FetchOwner=True is required to get valid Owner info in the response
        try:
            response = self.client.list_objects_v2(Bucket=target_bucket, Prefix=prefix, FetchOwner=True)
        except Exception:
            # Fallback if FetchOwner is not supported or permission denied
            response = self.client.list_objects_v2(Bucket=target_bucket, Prefix=prefix)
            
        results = []

        for obj in response.get("Contents", []):
            owner_display = None
            if "Owner" in obj:
                owner_display = obj["Owner"].get("DisplayName") or obj["Owner"].get("ID")
            
            if not owner_display and "owner" in bucket_tags:
                owner_display = bucket_tags["owner"]

            results.append(FileMetadata(
                path=f"s3://{target_bucket}/{obj['Key']}",
                type="file", 
                size_bytes=obj["Size"],
                owner=owner_display,
                last_modified=obj["LastModified"],
                last_accessed=None, 
                source="s3",
                tags=bucket_tags, 
                extra_metadata={
                    "storage_class": obj.get("storage_class"),
                    "etag": obj.get("ETag"),
                    "access_control": access_control
                }
            ))

        return results

    def read_file(self, path: str, bucket: str = None) -> bytes:
        # Parse bucket/key from path or assume it's just the key if prefix provided in init (but init takes bucket)
        # Expected path format: s3://bucket/key or just key
        target_bucket = bucket or self.bucket
        key = self._parse_path(path, target_bucket)
        response = self.client.get_object(Bucket=target_bucket, Key=key)
        return response["Body"].read()

    def get_metadata(self, path: str) -> FileMetadata:
        key = self._parse_path(path)
        response = self.client.head_object(Bucket=self.bucket, Key=key)
        
        # Fetch Context Data
        bucket_tags = {}
        try:
            tag_resp = self.client.get_bucket_tagging(Bucket=self.bucket)
            bucket_tags = {t["Key"].strip(): t["Value"] for t in tag_resp.get("TagSet", [])}
        except Exception:
            pass
            
        access_control = self._get_bucket_permissions(self.bucket)

        # Try to get object tagging
        tags = bucket_tags.copy()
        try:
            tag_resp = self.client.get_object_tagging(Bucket=self.bucket, Key=key)
            obj_tags = {t["Key"].strip(): t["Value"] for t in tag_resp.get("TagSet", [])}
            tags.update(obj_tags) # Object tags override bucket tags if collision
        except Exception:
            pass

        # Try to get ACL for owner
        owner_display = None
        try:
            acl_resp = self.client.get_object_acl(Bucket=self.bucket, Key=key)
            owner_display = acl_resp["Owner"].get("DisplayName") or acl_resp["Owner"].get("ID")
        except Exception:
            pass

        return FileMetadata(
            path=path,
            type="file",
            size_bytes=response["ContentLength"],
            owner=owner_display,
            last_modified=response["LastModified"],
            last_accessed=None, 
            source="s3",
            content_type=response.get("ContentType"),
            etag=response.get("ETag"),
            tags=tags,
            extra_metadata={
                "storage_class": response.get("StorageClass"),
                "version_id": response.get("VersionId"),
                "metadata": response.get("Metadata"),
                "access_control": access_control
            }
        )

    def get_container_metadata(self, bucket: str = None) -> Dict[str, Any]:
        """Returns metadata about the bucket."""
        target_bucket = bucket or self.bucket
        # HeadBucket checks existence and permission
        self.client.head_bucket(Bucket=target_bucket)
        
        # Get Location
        loc_resp = self.client.get_bucket_location(Bucket=target_bucket)
        region = loc_resp.get("LocationConstraint") or "us-east-1"
        
        return {
            "name": target_bucket,
            "region": region,
            "source": "s3",
            "access_control": self._get_bucket_permissions(target_bucket)
        }

    def _get_bucket_permissions(self, bucket: str = None) -> Dict[str, Any]:
        target_bucket = bucket or self.bucket
        permissions = {"policy": None, "acl": None}
        
        # Get Bucket Policy
        try:
            policy_resp = self.client.get_bucket_policy(Bucket=target_bucket)
            permissions["policy"] = policy_resp.get("Policy")
        except Exception:
            # Policy might not exist or permission denied
            pass
            
        # Get Bucket ACL
        try:
            acl_resp = self.client.get_bucket_acl(Bucket=target_bucket)
            grants = []
            for grant in acl_resp.get("Grants", []):
                grantee = grant.get("Grantee", {})
                grants.append({
                    "grantee_type": grantee.get("Type"),
                    "display_name": grantee.get("DisplayName"),
                    "id": grantee.get("ID"),
                    "uri": grantee.get("URI"),
                    "permission": grant.get("Permission")
                })
            permissions["acl"] = grants
        except Exception:
            pass
            
        return permissions

    def get_account_metadata(self) -> Dict[str, Any]:
        """Returns metadata about the AWS Account (Caller)."""
        sts = boto3.client(
            "sts",
            aws_access_key_id=self.config.get("aws_access_key_id"),
            aws_secret_access_key=self.config.get("aws_secret_access_key"),
            region_name=self.config.get("region")
        )
        identity = sts.get_caller_identity()
        return {
            "account_id": identity.get("Account"),
            "arn": identity.get("Arn"),
            "user_id": identity.get("UserId"),
            "source": "s3"
        }

    def _parse_path(self, path: str, bucket: str = None) -> str:
        target_bucket = bucket or self.bucket
        prefix = f"s3://{target_bucket}/"
        if path.startswith(prefix):
            return path[len(prefix):]
        return path

class S3ConnectorBuilder:
    def __init__(self):
        self._config = {}

    def access_key_id(self, access_key_id: str):
        self._config["aws_access_key_id"] = access_key_id
        return self

    def secret_access_key(self, secret_access_key: str):
        self._config["aws_secret_access_key"] = secret_access_key
        return self

    def region(self, region: str):
        self._config["region"] = region
        return self

    def bucket(self, bucket: str):
        self._config["bucket"] = bucket
        return self

    def build(self) -> S3Connector:
        # Check for shared config or credentials
        if not self._config.get("aws_access_key_id") and not os.getenv("AWS_ACCESS_KEY_ID"):
             # boto3 might find credentials elsewhere, but for our builder we expect explicit or env
             pass
        return S3Connector(self._config)
