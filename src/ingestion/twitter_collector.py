import os
import sys
import json
import time
from datetime import datetime

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

# Set credentials
KEY_FILE_PATH = os.path.join(project_root, 'config', 'GcpKey.json')
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = KEY_FILE_PATH

from config import twitter_config, gcp_details_config
from google.cloud import pubsub_v1
import tweepy

class TwitterCollector:
    def __init__(self):
        print("🚀 Initializing Twitter Collector...")
        
        # Twitter client
        self.client = tweepy.Client(
            bearer_token=twitter_config.BEARER_TOKEN,
            consumer_key=twitter_config.API_KEY,
            consumer_secret=twitter_config.API_SECRET_KEY,
            access_token=twitter_config.ACCESS_TOKEN,
            access_token_secret=twitter_config.ACCESS_TOKEN_SECRET
        )
        
        # Pub/Sub client
        self.publisher = pubsub_v1.PublisherClient()
        self.topic_path = self.publisher.topic_path(
            gcp_details_config.PROJECT_ID, 
            gcp_details_config.PUBSUB_TOPICS["raw_tweets"]
        )
        
        print("✅ Twitter Collector initialized")
    
    def format_tweet_data(self, tweet, query):
        """Format tweet data for Pub/Sub"""
        return {
            'tweet_id': str(tweet.id),
            'text': tweet.text,
            'author_id': str(tweet.author_id),
            'created_at': tweet.created_at.isoformat() if tweet.created_at else None,
            'query': query,
            'retweet_count': tweet.public_metrics.get('retweet_count', 0) if hasattr(tweet, 'public_metrics') else 0,
            'like_count': tweet.public_metrics.get('like_count', 0) if hasattr(tweet, 'public_metrics') else 0,
            'reply_count': tweet.public_metrics.get('reply_count', 0) if hasattr(tweet, 'public_metrics') else 0,
            'collected_at': datetime.utcnow().isoformat()
        }
    
    def publish_to_pubsub(self, tweet_data):
        """Publish tweet to Pub/Sub"""
        try:
            data = json.dumps(tweet_data).encode('utf-8')
            future = self.publisher.publish(self.topic_path, data)
            message_id = future.result()
            return message_id
        except Exception as e:
            print(f"❌ Failed to publish tweet {tweet_data['tweet_id']}: {e}")
            return None
    
    def collect_tweets(self, query, max_results=10):  # CHANGED: 10 is minimum for Twitter API
        """Collect tweets for a specific query"""
        print(f"🔍 Searching for: '{query}'")
        
        try:
            # Get tweets from Twitter API
            tweets = self.client.search_recent_tweets(
                query=query,
                max_results=max_results,  # Twitter API requires 10-100
                tweet_fields=['author_id', 'created_at', 'public_metrics', 'lang']
            )
            
            if not tweets.data:
                print(f"   No tweets found for: {query}")
                return 0
            
            published_count = 0
            for tweet in tweets.data:
                # Format tweet data
                tweet_data = self.format_tweet_data(tweet, query)
                
                # Publish to Pub/Sub
                message_id = self.publish_to_pubsub(tweet_data)
                if message_id:
                    published_count += 1
                    print(f"   ✅ Published: {tweet.id}")
                else:
                    print(f"   ❌ Failed: {tweet.id}")
            
            print(f"   📊 Published {published_count}/{len(tweets.data)} tweets")
            return published_count
            
        except tweepy.errors.TooManyRequests:
            print("⚠️ Rate limit hit. Waiting 1 minute...")  # REDUCED: 1 minute instead of 15
            time.sleep(300)  # REDUCED: Wait 1 minute
            return self.collect_tweets(query, max_results)  # Retry
            
        except Exception as e:
            print(f"❌ Collection failed for '{query}': {e}")
            return 0
    
    def run_collection(self):
        """Run collection for all configured queries"""
        print("\n" + "="*50)
        print("🎯 STARTING TWITTER DATA COLLECTION")
        print("="*50)
        
        total_published = 0
        
        for query in twitter_config.SEARCH_QUERIES:
            print(f"\n--- Query: {query} ---")
            count = self.collect_tweets(query, max_results=10)  # CHANGED: 10 is minimum
            total_published += count
            time.sleep(1)  # REDUCED: 1 second between queries
        
        print("\n" + "="*50)
        print(f"🎉 COLLECTION COMPLETE")
        print(f"📊 Total tweets published to Pub/Sub: {total_published}")
        print("="*50)
        
        return total_published

if __name__ == "__main__":
    collector = TwitterCollector()
    collector.run_collection()