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

    def list_objects(self, prefix: str = "") -> List[FileMetadata]:
        # Fetch Bucket Tags once to apply to all objects
        bucket_tags = {}
        try:
            tag_resp = self.client.get_bucket_tagging(Bucket=self.bucket)
            bucket_tags = {t["Key"].strip(): t["Value"] for t in tag_resp.get("TagSet", [])}
        except Exception:
            pass

        # FetchOwner=True is required to get valid Owner info in the response
        try:
            response = self.client.list_objects_v2(Bucket=self.bucket, Prefix=prefix, FetchOwner=True)
        except Exception:
            # Fallback if FetchOwner is not supported or permission denied
            response = self.client.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
            
        results = []

        for obj in response.get("Contents", []):
            owner_display = None
            if "Owner" in obj:
                owner_display = obj["Owner"].get("DisplayName") or obj["Owner"].get("ID")
            
            # Fallback to bucket owner tag if no object owner (though S3 objects usually have owners)
            # OR if user specifically wants the tag owner info
            if not owner_display and "owner" in bucket_tags:
                owner_display = bucket_tags["owner"]
            elif "owner" in bucket_tags:
                 # If user expects the tag to represent the business owner, we might prioritize it or leave it in tags
                 # Azure logic fell back. S3 logic: Object Owner is technical (Account/User ID). Tag is business owner.
                 # Let's keep technical owner in 'owner' field if present, but the tag is now in 'tags'.
                 # WAIT: User said "why is it showing empty". The field 'tag' was empty. 
                 # And 'owner' field was canonical ID. 
                 # They probably want 'owner' field to show "rohit sharama" OR at least 'tag' to show it.
                 # For consistency with Azure, I will use tag owner if present as a fallback? 
                 # Or just ensure it's in the tags.
                 pass

            # Explicit logic: If bucket tag has 'owner', and we want to show it in 'owner' field when technical owner is obscure?
            # User probably just wants the tags visible.
            
            # Update: If technical owner is a long hash (Canonical ID) and tag["owner"] exists, user might prefer tag.
            # But let's safely put it in tags first.

            # Combined Tags: S3 doesn't return object tags in ListObjects. So 'tags' will just be bucket_tags here.
            
            results.append(FileMetadata(
                path=f"s3://{self.bucket}/{obj['Key']}",
                type="file", 
                size_bytes=obj["Size"],
                owner=owner_display,
                last_modified=obj["LastModified"],
                last_accessed=None, 
                source="s3",
                tags=bucket_tags, 
                extra_metadata={
                    "storage_class": obj.get("StorageClass"),
                    "etag": obj.get("ETag")
                }
            ))

        return results

    def read_file(self, path: str) -> bytes:
        # Parse bucket/key from path or assume it's just the key if prefix provided in init (but init takes bucket)
        # Expected path format: s3://bucket/key or just key
        key = self._parse_path(path)
        response = self.client.get_object(Bucket=self.bucket, Key=key)
        return response["Body"].read()

    def get_metadata(self, path: str) -> FileMetadata:
        key = self._parse_path(path)
        response = self.client.head_object(Bucket=self.bucket, Key=key)
        
        # Fetch Bucket Tags
        bucket_tags = {}
        try:
            tag_resp = self.client.get_bucket_tagging(Bucket=self.bucket)
            bucket_tags = {t["Key"].strip(): t["Value"] for t in tag_resp.get("TagSet", [])}
        except Exception:
            pass

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
                "metadata": response.get("Metadata")
            }
        )

    def get_container_metadata(self) -> Dict[str, Any]:
        """Returns metadata about the bucket."""
        # HeadBucket checks existence and permission
        self.client.head_bucket(Bucket=self.bucket)
        
        # Get Location
        loc_resp = self.client.get_bucket_location(Bucket=self.bucket)
        region = loc_resp.get("LocationConstraint") or "us-east-1"
        
        return {
            "name": self.bucket,
            "region": region,
            "source": "s3"
        }

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

    def _parse_path(self, path: str) -> str:
        if path.startswith(f"s3://{self.bucket}/"):
            return path[len(f"s3://{self.bucket}/"):]
        return path
