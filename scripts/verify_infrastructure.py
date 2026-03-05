# scripts/verify_infrastructure.py
import os
import sys

# Set credentials path
KEY_FILE_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 
    'config', 
    'GcpKey.json'  # Make sure this matches your actual key file name
)

if not os.path.exists(KEY_FILE_PATH):
    print(f"❌ Key file not found at: {KEY_FILE_PATH}")
    print("Please make sure your service account key file exists in config/ folder")
    exit(1)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = KEY_FILE_PATH
print(f"✅ Using credentials: {KEY_FILE_PATH}")

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

try:
    from config import gcp_details_config
    print(f"✅ Project ID: {gcp_details_config.PROJECT_ID}")
except ImportError as e:
    print(f"❌ Config import failed: {e}")
    exit(1)

from google.cloud import pubsub_v1, bigquery, storage
import google.api_core.exceptions

def verify_pubsub_topic():
    """Verify Pub/Sub topic exists and is accessible"""
    try:
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(gcp_details_config.PROJECT_ID, "twitter-raw-tweets")
        publisher.get_topic(request={"topic": topic_path})
        print("✅ Pub/Sub: Topic exists and accessible")
        return True
    except google.api_core.exceptions.PermissionDenied:
        print("❌ Pub/Sub: Permission denied - need 'pubsub.viewer' role")
        return False
    except google.api_core.exceptions.NotFound:
        print("❌ Pub/Sub: Topic not found - create it manually")
        return False
    except Exception as e:
        print(f"❌ Pub/Sub: Unexpected error - {e}")
        return False

def verify_bigquery_resources():
    """Verify BigQuery dataset and table exist"""
    try:
        client = bigquery.Client()
        dataset_ref = client.dataset("twitter_bronze")
        dataset = client.get_dataset(dataset_ref)
        print("✅ BigQuery: Dataset exists")
        
        table_ref = dataset_ref.table("raw_tweets")
        table = client.get_table(table_ref)
        print("✅ BigQuery: Table exists")
        
        print("📋 BigQuery Table Schema:")
        for field in table.schema:
            print(f"   - {field.name} ({field.field_type})")
        return True
        
    except google.api_core.exceptions.NotFound:
        print("❌ BigQuery: Dataset or table not found")
        return False
    except Exception as e:
        print(f"❌ BigQuery: Unexpected error - {e}")
        return False

def verify_storage_bucket():
    """Verify Cloud Storage bucket exists and is accessible"""
    try:
        client = storage.Client()
        
        # List buckets to test basic permissions
        buckets = list(client.list_buckets())
        print(f"✅ Storage: Can list buckets ({len(buckets)} found)")
        
        # Try to access our specific bucket
        bucket_name = f"twitter-raw-data-{gcp_details_config.PROJECT_ID}"
        try:
            bucket = client.get_bucket(bucket_name)
            print(f"✅ Storage: Bucket '{bucket_name}' exists and accessible")
            return True
        except google.api_core.exceptions.NotFound:
            print(f"❌ Storage: Bucket '{bucket_name}' not found")
            return False
            
    except google.api_core.exceptions.PermissionDenied:
        print("❌ Storage: Permission denied - need 'storage.admin' or 'storage.objectAdmin' role")
        return False
    except Exception as e:
        print(f"❌ Storage: Unexpected error - {e}")
        return False

def main():
    print("🔍 Verifying GCP Infrastructure...")
    print("=" * 50)
    
    success = True
    success &= verify_pubsub_topic()
    print("-" * 30)
    success &= verify_bigquery_resources() 
    print("-" * 30)
    success &= verify_storage_bucket()
    print("=" * 50)
    
    if success:
        print("\n🎉 ALL INFRASTRUCTURE VERIFIED!")
        print("You can now proceed to Phase 1: Building the Twitter collector")
    else:
        print("\n❌ Some infrastructure issues need fixing.")
        print("Fix the specific permissions/resources mentioned above before proceeding.")

if __name__ == "__main__":
    main()