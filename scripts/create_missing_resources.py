# scripts/create_missing_resources.py
import os
import sys

# Set credentials path
KEY_FILE_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'config', 'GcpKey.json')
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = KEY_FILE_PATH

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from config import gcp_details_config
from google.cloud import bigquery

def create_bigquery_table():
    """Create the BigQuery table if it doesn't exist"""
    try:
        client = bigquery.Client()
        
        # Create dataset if it doesn't exist
        dataset_id = f"{gcp_details_config.PROJECT_ID}.twitter_bronze"
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "US"
        dataset = client.create_dataset(dataset, exists_ok=True)
        print(f"✅ Dataset created/verified: {dataset_id}")
        
        # Create table schema
        schema = [
            bigquery.SchemaField("tweet_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("text", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("author_id", "STRING"),
            bigquery.SchemaField("created_at", "TIMESTAMP"),
            bigquery.SchemaField("query", "STRING"),
            bigquery.SchemaField("retweet_count", "INTEGER"),
            bigquery.SchemaField("like_count", "INTEGER"),
            bigquery.SchemaField("reply_count", "INTEGER"),
            bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
        ]
        
        table_id = f"{dataset_id}.raw_tweets"
        table = bigquery.Table(table_id, schema=schema)
        table = client.create_table(table, exists_ok=True)
        print(f"✅ Table created/verified: {table_id}")
        
        return True
        
    except Exception as e:
        print(f"❌ Failed to create BigQuery resources: {e}")
        return False

if __name__ == "__main__":
    print("🔧 Creating missing resources...")
    create_bigquery_table()