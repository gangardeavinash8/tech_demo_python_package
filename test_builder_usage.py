import os
import json
from dotenv import load_dotenv
from metadata_reader import azure_builder

# Load environment variables
load_dotenv()

def test_azure_builder():
    print("--- Testing Azure Connector Builder ---")
    
    try:
        # 1. Use the new Builder Pattern!
        # This replaces the flat dictionary approach.
        builder = azure_builder() \
            .account_name(os.getenv("AZURE_ACCOUNT_NAME")) \
            .container(os.getenv("AZURE_CONTAINER")) \
            .client_id(os.getenv("AZURE_CLIENT_ID")) \
            .client_secret(os.getenv("AZURE_CLIENT_SECRET")) \
            .tenant_id(os.getenv("AZURE_TENANT_ID")) \
            .subscription_id(os.getenv("AZURE_SUBSCRIPTION_ID")) \
            .resource_group(os.getenv("AZURE_RESOURCE_GROUP"))

        # 2. Build the connector
        print("Building connector...")
        connector = builder.build()
        print(f"Connector created: {type(connector).__name__}")

        # 3. Test functionality
        print("\nFetching account info using Builder-created connector:")
        info = connector.get_account_metadata()
        print(f"Account SKU: {info.get('sku_name')}")
        print(f"Location: {info.get('management_properties', {}).get('location')}")

        if os.getenv("AZURE_CONTAINER"):
            print(f"\nListing files in container '{os.getenv('AZURE_CONTAINER')}' (top 3):")
            results = connector.list_objects()
            for item in results[:3]:
                print(f" - {item.path}")

        print("\n✅ Builder Pattern Test Successful!")

    except Exception as e:
        print(f"\n❌ Builder Pattern Test Failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_azure_builder()
