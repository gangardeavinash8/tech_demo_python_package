from typing import Dict, Any
from .connectors.base import BaseConnector

def get_connector(source: str, config: Dict[str, Any]) -> BaseConnector:
    """
    Factory function to get the appropriate connector.
    
    Args:
        source: The source type ('s3', 'azure', etc.)
        config: Configuration dictionary for the connector
        
    Returns:
        An instance of a class inheriting from BaseConnector
    """
    if source == "s3":
        from .connectors.s3 import S3Connector
        return S3Connector(config)
    elif source == "azure":
        from .connectors.azure import AzureConnector
        return AzureConnector(config)
    elif source == "databricks":
        from .connectors.databricks import DatabricksConnector
        return DatabricksConnector(config)
    elif source == "sharepoint":
        from .connectors.sharepoint import SharePointConnector
        return SharePointConnector(config)
    else:
        raise ValueError(f"Unsupported source: {source}")

def azure_builder() -> Any:
    """Returns a new AzureConnectorBuilder instance."""
    from .connectors.azure import AzureConnectorBuilder
    return AzureConnectorBuilder()

def s3_builder() -> Any:
    """Returns a new S3ConnectorBuilder instance."""
    from .connectors.s3 import S3ConnectorBuilder
    return S3ConnectorBuilder()

def databricks_builder() -> Any:
    """Returns a new DatabricksConnectorBuilder instance."""
    from .connectors.databricks import DatabricksConnectorBuilder
    return DatabricksConnectorBuilder()

def sharepoint_builder() -> Any:
    """Returns a new SharePointConnectorBuilder instance."""
    from .connectors.sharepoint import SharePointConnectorBuilder
    return SharePointConnectorBuilder()
