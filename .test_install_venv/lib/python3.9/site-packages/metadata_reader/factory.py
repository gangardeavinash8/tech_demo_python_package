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
