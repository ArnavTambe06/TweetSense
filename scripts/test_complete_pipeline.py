# scripts/test_complete_pipeline.py
import os
import sys

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

KEY_FILE_PATH = os.path.join(project_root, 'config', 'GcpKey.json')
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = KEY_FILE_PATH

from src.ingestion.twitter_collector import TwitterCollector
from google.cloud import bigquery
import time
import logging

def test_complete_pipeline():
    print("🚀 TESTING COMPLETE PIPELINE")
    print("=" * 50)
    
    try:
        # Step 1: Initialize Twitter collector
        print("1. Initializing Twitter collector...")
        collector = TwitterCollector()
        
        # Step 2: Collect tweets
        print("   Searching for: 'politics'")
        count = collector.collect_tweets("politics", max_results=10)
        print(f"   Collected {count} tweets")
        
        # Wait for Cloud Function processing
        print("2. Waiting for Cloud Function processing...")
        time.sleep(15)
        
        # Step 3: Check BigQuery
        print("3. Checking BigQuery...")
        client = bigquery.Client()
        
        # Check raw_tweets table (which exists and is working)
        try:
            query = "SELECT COUNT(*) as count FROM `twitter_bronze.raw_tweets`"
            result = client.query(query).result()
            for row in result:
                print(f"   ✅ Total tweets in raw_tweets: {row.count}")
        except Exception as e:
            print(f"   ❌ Error checking raw_tweets: {e}")
        
        # Check tweet_metrics table (might not exist yet)
        try:
            query = "SELECT COUNT(*) as count FROM `twitter_bronze.tweet_metrics`"
            result = client.query(query).result()
            for row in result:
                print(f"   ✅ Total records in tweet_metrics: {row.count}")
        except Exception as e:
            if "Not found" in str(e) or "notFound" in str(e):
                print("   ⚠️  tweet_metrics table doesn't exist yet (this is normal for first run)")
            else:
                print(f"   ❌ Error checking tweet_metrics: {e}")
        
        # Let's also check the latest tweets to verify data quality
        try:
            query = """
            SELECT 
                tweet_id, 
                username,
                text,
                created_at
            FROM `twitter_bronze.raw_tweets` 
            ORDER BY created_at DESC 
            LIMIT 3
            """
            result = client.query(query).result()
            print("\n   📝 Latest 3 tweets:")
            for row in result:
                print(f"      - {row.username}: {row.text[:50]}...")
        except Exception as e:
            print(f"   ❌ Error fetching sample tweets: {e}")
        
        print("=" * 50)
        print("🎉 PIPELINE TEST COMPLETE")
        print("💡 Note: tweet_metrics table will be created during data processing stages")
        
    except Exception as e:
        print(f"❌ Pipeline test failed: {e}")
        logging.error(f"Pipeline test error: {e}")

if __name__ == "__main__":
    test_complete_pipeline()