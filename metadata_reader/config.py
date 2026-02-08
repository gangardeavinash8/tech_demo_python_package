import os
from typing import Dict, Any

def load_config_from_env() -> Dict[str, Any]:
    """
    Load configuration from environment variables.
    Returns a dictionary with keys expected by connectors.
    """
    return {
        "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID"),
        "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
        "region": os.getenv("AWS_REGION"),
        "bucket": os.getenv("S3_BUCKET"),
        "connection_string": os.getenv("AZURE_CONNECTION_STRING"),
        "container": os.getenv("AZURE_CONTAINER"),
        "azure_subscription_id": os.getenv("AZURE_SUBSCRIPTION_ID"),
        "sharepoint_site_id": os.getenv("SHAREPOINT_SITE_ID"),
        "sharepoint_site_url": os.getenv("SHAREPOINT_SITE_URL"), # New
        "sharepoint_drive_id": os.getenv("SHAREPOINT_DRIVE_ID"),
        "sharepoint_client_id": os.getenv("SHAREPOINT_CLIENT_ID"),
        "sharepoint_client_secret": os.getenv("SHAREPOINT_CLIENT_SECRET"),
        "sharepoint_tenant_id": os.getenv("SHAREPOINT_TENANT_ID"),
        "azure_tenant_id": os.getenv("AZURE_TENANT_ID"),
        "azure_client_id": os.getenv("AZURE_CLIENT_ID"),
        "azure_client_secret": os.getenv("AZURE_CLIENT_SECRET"),
        "azure_resource_group": os.getenv("AZURE_RESOURCE_GROUP"),
        "azure_account_name": os.getenv("AZURE_ACCOUNT_NAME"),
        "databricks_host": os.getenv("DATABRICKS_HOST"),
        "databricks_token": os.getenv("DATABRICKS_TOKEN"),
    }
