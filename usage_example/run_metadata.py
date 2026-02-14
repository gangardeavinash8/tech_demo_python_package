import os
import json
import sys
from dotenv import load_dotenv

# Try importing from the installed package
try:
    from metadata_reader import (
        s3_builder, 
        azure_builder, 
        databricks_builder, 
        sharepoint_builder
    )
except ImportError:
    print("Error: 'metadata_reader' package not found. Please install it first.", file=sys.stderr)
    sys.exit(1)

# Load environment variables from .env file
load_dotenv()

def main():
    all_metadata = []
    summary = []
    
    # helper for summary
    def add_to_summary(source, location, count, status="Success"):
        summary.append({
            "Source": source,
            "Location": location,
            "Files Found": count,
            "Status": status
        })

    print("🚀 Starting MetaData Reader...", file=sys.stderr)
    print("="*40, file=sys.stderr)
    
    # 1. AWS S3
    if os.getenv("AWS_ACCESS_KEY_ID"):
        # Support comma-separated buckets
        buckets = os.getenv("S3_BUCKET", "").split(",")
        try:
            s3_conn = s3_builder() \
                .access_key_id(os.getenv("AWS_ACCESS_KEY_ID")) \
                .secret_access_key(os.getenv("AWS_SECRET_ACCESS_KEY")) \
                .region(os.getenv("AWS_REGION")) \
                .build()
            
            for bucket in buckets:
                bucket = bucket.strip()
                if not bucket: continue
                try:
                    print(f"📦 Scanning S3 Bucket: {bucket}", file=sys.stderr)
                    files = s3_conn.list_objects(bucket=bucket)
                    all_metadata.extend(files)
                    add_to_summary("AWS S3", bucket, len(files))
                except Exception as e:
                    print(f" ❌ Error scanning S3 bucket {bucket}: {e}", file=sys.stderr)
                    add_to_summary("AWS S3", bucket, 0, f"Error: {str(e)[:50]}...")
        except Exception as e:
            print(f" ❌ S3 Builder failed: {e}", file=sys.stderr)

    # 2. Azure Blob Storage
    found_azure_creds = os.getenv("AZURE_SUBSCRIPTION_ID") or os.getenv("AZURE_ACCOUNT_NAME") or os.getenv("AZURE_CONNECTION_STRING")
    if found_azure_creds:
        try:
            # Build a base connector for discovery/shared config
            az_discovery = azure_builder() \
                .subscription_id(os.getenv("AZURE_SUBSCRIPTION_ID")) \
                .client_id(os.getenv("AZURE_CLIENT_ID")) \
                .client_secret(os.getenv("AZURE_CLIENT_SECRET")) \
                .tenant_id(os.getenv("AZURE_TENANT_ID")) \
                .connection_string(os.getenv("AZURE_CONNECTION_STRING")) \
                .build()

            # Determine accounts to scan
            if os.getenv("AZURE_ACCOUNT_NAME"):
                accounts = [a.strip() for a in os.getenv("AZURE_ACCOUNT_NAME").split(",") if a.strip()]
            else:
                print("🔍 Discovering Azure Storage Accounts...", file=sys.stderr)
                accounts = az_discovery.list_storage_accounts()
                print(f"Found {len(accounts)} accounts: {', '.join([a['name'] if isinstance(a, dict) else a for a in accounts])}", file=sys.stderr)

            # Determine containers to scan
            config_containers = [c.strip() for c in os.getenv("AZURE_CONTAINER", "").split(",") if c.strip()]
            
            for acct in accounts:
                try:
                    account_name = acct["name"] if isinstance(acct, dict) else acct
                    resource_group = acct["resource_group"] if isinstance(acct, dict) else None
                    account_tags = acct["tags"] if isinstance(acct, dict) else {}

                    az_conn = azure_builder() \
                        .account_name(account_name) \
                        .resource_group(resource_group) \
                        .account_tags(account_tags) \
                        .client_id(os.getenv("AZURE_CLIENT_ID")) \
                        .client_secret(os.getenv("AZURE_CLIENT_SECRET")) \
                        .tenant_id(os.getenv("AZURE_TENANT_ID")) \
                        .build()
                        
                    # Discover containers for this account if not specified
                    containers = config_containers if config_containers else az_conn.list_containers()
                    
                    for container in containers:
                        try:
                            print(f"☁️  Scanning Azure Account: {account_name} | Container: {container}", file=sys.stderr)
                            files = az_conn.list_objects(container=container)
                            all_metadata.extend(files)
                            add_to_summary("Azure Blob", f"{account_name}/{container}", len(files))
                        except Exception as e:
                            print(f" ❌ Error scanning Azure {account_name}/{container}: {e}", file=sys.stderr)
                            add_to_summary("Azure Blob", f"{account_name}/{container}", 0, f"Error: {str(e)[:50]}...")
                except Exception as e:
                    print(f" ❌ Azure Builder failed for {acct if isinstance(acct, str) else acct.get('name')}: {e}", file=sys.stderr)
        except Exception as e:
            print(f" ❌ Azure Discovery failed: {e}", file=sys.stderr)

    # 3. SharePoint
    if os.getenv("SHAREPOINT_CLIENT_ID"):
        try:
            site = os.getenv("SHAREPOINT_SITE_URL")
            print(f"📂 Scanning SharePoint Site: {site}", file=sys.stderr)
            sp = sharepoint_builder() \
                .tenant_id(os.getenv("SHAREPOINT_TENANT_ID")) \
                .client_id(os.getenv("SHAREPOINT_CLIENT_ID")) \
                .client_secret(os.getenv("SHAREPOINT_CLIENT_SECRET")) \
                .site_url(site) \
                .build()
            files = sp.list_objects()
            all_metadata.extend(files)
            add_to_summary("SharePoint", site, len(files))
        except Exception as e:
            print(f" Error fetching SharePoint: {e}", file=sys.stderr)
            add_to_summary("SharePoint", os.getenv("SHAREPOINT_SITE_URL"), 0, f"Error: {str(e)[:50]}...")

    # 4. Databricks
    if os.getenv("DATABRICKS_HOST"):
        try:
            # Build a base connector for discovery/shared config
            db_discovery = databricks_builder() \
                .host(os.getenv("DATABRICKS_HOST")) \
                .token(os.getenv("DATABRICKS_TOKEN")) \
                .build()

            # Determine volumes to scan
            config_volumes = os.getenv("DATABRICKS_VOLUME")
            if config_volumes:
                # Support comma-separated volumes
                vols_to_scan = []
                for v in config_volumes.split(","):
                    v = v.strip()
                    if not v: continue
                    # Check for catalog.schema.volume format
                    parts = v.split(".")
                    if len(parts) == 3:
                        vols_to_scan.append({"catalog": parts[0], "schema": parts[1], "name": parts[2]})
                    else:
                        vols_to_scan.append({
                            "catalog": os.getenv("DATABRICKS_CATALOG"),
                            "schema": os.getenv("DATABRICKS_SCHEMA"),
                            "name": v
                        })
            else:
                print("🔍 Discovering Databricks Volumes...", file=sys.stderr)
                vols_to_scan = db_discovery.list_volumes()
                print(f"Found {len(vols_to_scan)} accessible volumes", file=sys.stderr)

            for vol_info in vols_to_scan:
                catalog = vol_info.get("catalog")
                schema = vol_info.get("schema")
                vol_name = vol_info.get("name")
                owner = vol_info.get("owner")
                
                if not all([catalog, schema, vol_name]):
                    continue

                try:
                    vol_path = f"{catalog}.{schema}.{vol_name}"
                    print(f"🧱 Scanning Databricks Volume: {vol_path}", file=sys.stderr)
                    
                    db = databricks_builder() \
                        .host(os.getenv("DATABRICKS_HOST")) \
                        .token(os.getenv("DATABRICKS_TOKEN")) \
                        .catalog(catalog) \
                        .schema(schema) \
                        .volume(vol_name) \
                        .owner(owner) \
                        .build()
                        
                    files = db.list_objects()
                    all_metadata.extend(files)
                    add_to_summary("Databricks", vol_path, len(files))
                except Exception as e:
                    vol_path = f"{catalog}.{schema}.{vol_name}" if vol_name else "Unknown"
                    print(f" ❌ Error scanning Databricks volume {vol_path}: {e}", file=sys.stderr)
                    add_to_summary("Databricks", vol_path, 0, f"Error: {str(e)[:50]}...")
        except Exception as e:
            print(f" ❌ Databricks Discovery failed: {e}", file=sys.stderr)

    print("\n" + "="*40, file=sys.stderr)
    print("📊 EXTRACTION SUMMARY:", file=sys.stderr)
    for s in summary:
        print(f" - {s['Source']} ({s['Location']}): {s['Files Found']} files [{s['Status']}]", file=sys.stderr)
    print("="*40 + "\n", file=sys.stderr)

    # Output results as JSON
    json_output = [item.to_dict() for item in all_metadata]
    
    # Print to console
    print(json.dumps(json_output, indent=2, default=str))
    
    # Save to file
    output_path = os.path.join(os.getcwd(), "metadata_output.json")
    with open(output_path, "w") as f:
        json.dump(json_output, f, indent=2, default=str)
    
    print(f"\n✅ Results saved to: {output_path}", file=sys.stderr)

if __name__ == "__main__":
    main()
