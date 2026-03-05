# scripts/check_bigquery_simple.py
import os
import sys

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

KEY_FILE_PATH = os.path.join(project_root, 'config', 'GcpKey.json')
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = KEY_FILE_PATH

from google.cloud import bigquery

def check_bigquery():
    print("🔍 Checking BigQuery for processed tweets...")
    
    client = bigquery.Client()
    
    try:
        # Count total tweets
        query = "SELECT COUNT(*) as count FROM `twitter_bronze.raw_tweets`"
        result = client.query(query).result()
        
        for row in result:
            count = row.count
            print(f"✅ Total tweets in BigQuery: {count}")
            
            if count > 0:
                # Show latest tweets
                sample_query = """
                SELECT tweet_id, text, query, ingested_at
                FROM `twitter_bronze.raw_tweets`
                ORDER BY ingested_at DESC
                LIMIT 3
                """
                samples = client.query(sample_query).result()
                print("\n📊 Latest tweets:")
                for sample in samples:
                    print(f"   - {sample.tweet_id}: {sample.text[:60]}...")
            else:
                print("❌ No tweets found in BigQuery yet")
                
        return count > 0
        
    except Exception as e:
        print(f"❌ Error checking BigQuery: {e}")
        return False

if __name__ == "__main__":
    check_bigquery()