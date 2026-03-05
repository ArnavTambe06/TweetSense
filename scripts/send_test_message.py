# scripts/send_test_message.py
import os
import sys
import json

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

KEY_FILE_PATH = os.path.join(project_root, 'config', 'GcpKey.json')
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = KEY_FILE_PATH

from config import gcp_details_config
from google.cloud import pubsub_v1
from datetime import datetime

def send_test_message():
    print("📨 Sending test message to Pub/Sub...")
    
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(gcp_details_config.PROJECT_ID, "twitter-raw-tweets")
    
    test_tweet = {
        'tweet_id': 'test_123456',
        'text': 'This is a test tweet for our pipeline deployment! 🚀',
        'author_id': 'test_author_123',
        'created_at': datetime.utcnow().isoformat(),
        'query': 'test',
        'retweet_count': 5,
        'like_count': 10,
        'reply_count': 2,
        'collected_at': datetime.utcnow().isoformat()
    }
    
    try:
        data = json.dumps(test_tweet).encode('utf-8')
        future = publisher.publish(topic_path, data)
        message_id = future.result()
        print(f"✅ Test message sent: {message_id}")
        return True
    except Exception as e:
        print(f"❌ Failed to send test message: {e}")
        return False

if __name__ == "__main__":
    send_test_message()