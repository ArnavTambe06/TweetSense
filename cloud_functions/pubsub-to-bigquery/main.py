import base64
import json
import logging
from google.cloud import bigquery
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO)
client = bigquery.Client()

def pubsub_to_bigquery(event, context):
    try:
        pubsub_message = base64.b64decode(event['data']).decode('utf-8')
        tweet_data = json.loads(pubsub_message)
        
        logging.info(f"Processing tweet: {tweet_data.get('tweet_id', 'Unknown')}")
        
        required_fields = ['tweet_id', 'text', 'author_id']
        for field in required_fields:
            if field not in tweet_data:
                logging.error(f"Missing required field: {field}")
                return
        
        row = {
            'tweet_id': tweet_data['tweet_id'],
            'text': tweet_data['text'],
            'author_id': tweet_data['author_id'],
            'created_at': tweet_data.get('created_at'),
            'query': tweet_data.get('query', 'unknown'),
            'retweet_count': tweet_data.get('retweet_count', 0),
            'like_count': tweet_data.get('like_count', 0),
            'reply_count': tweet_data.get('reply_count', 0),
            'ingested_at': datetime.utcnow().isoformat()
        }
        
        dataset_id = "twitter_bronze"
        table_id = "raw_tweets"
        table_ref = client.dataset(dataset_id).table(table_id)
        
        errors = client.insert_rows_json(table_ref, [row])
        
        if errors:
            logging.error(f"BigQuery insert errors: {errors}")
        else:
            logging.info(f"Successfully inserted tweet {tweet_data['tweet_id']}")
            
    except Exception as e:
        logging.error(f"Error: {e}")