# scripts/check_pubsub_messages.py
import os
import sys
import json

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

KEY_FILE_PATH = os.path.join(project_root, 'config', 'GcpKey.json')
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = KEY_FILE_PATH

from config import gcp_details_config
from google.cloud import pubsub_v1

def check_pubsub_messages():
    print("🔍 Checking Pub/Sub messages...")
    
    subscriber = pubsub_v1.SubscriberClient()
    project_id = gcp_details_config.PROJECT_ID
    topic_id = "twitter-raw-tweets"
    
    # Create temporary subscription
    subscription_id = "temp-check-subscription"
    topic_path = subscriber.topic_path(project_id, topic_id)
    subscription_path = subscriber.subscription_path(project_id, subscription_id)
    
    try:
        # Create subscription
        subscriber.create_subscription(
            request={"name": subscription_path, "topic": topic_path}
        )
        print("✅ Temporary subscription created")
        
        # Pull messages
        response = subscriber.pull(
            request={"subscription": subscription_path, "max_messages": 10}
        )
        
        if response.received_messages:
            print(f"📨 Found {len(response.received_messages)} messages in queue:")
            for i, message in enumerate(response.received_messages, 1):
                data = json.loads(message.message.data.decode('utf-8'))
                print(f"   {i}. Tweet {data['tweet_id']}: {data['text'][:50]}...")
                
            # DON'T acknowledge - leave for Cloud Function to process
            print("⚠️  Messages left in queue for Cloud Function")
        else:
            print("❌ No messages in queue")
            
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        # Clean up subscription
        try:
            subscriber.delete_subscription(request={"subscription": subscription_path})
            print("✅ Subscription cleaned up")
        except:
            pass

if __name__ == "__main__":
    check_pubsub_messages()