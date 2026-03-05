import os
import sys
import json
from datetime import datetime, timedelta

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

KEY_FILE_PATH = os.path.join(project_root, 'config', 'GcpKey.json')
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = KEY_FILE_PATH

from config import gcp_details_config
from src.processing.sentiment_analyzer import SentimentAnalyzer
from google.cloud import bigquery
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SilverLayerPipeline:
    """Create Silver layer with sentiment analysis"""
    
    def __init__(self):
        self.bq_client = bigquery.Client()
        self.analyzer = SentimentAnalyzer()
        
        # Table references
        self.bronze_table = f"{gcp_details_config.PROJECT_ID}.{gcp_details_config.BIGQUERY_DATASETS['bronze']}.raw_tweets"
        self.silver_table = f"{gcp_details_config.PROJECT_ID}.{gcp_details_config.BIGQUERY_DATASETS['silver']}.tweet_sentiment"
    
    def _convert_datetime(self, dt_obj):
        """Convert datetime object to ISO string for JSON serialization"""
        if dt_obj is None:
            return None
        if hasattr(dt_obj, 'isoformat'):
            return dt_obj.isoformat()
        return dt_obj
    
    def create_silver_table(self):
        """Create Silver table schema if it doesn't exist"""
        
        # First, create the silver dataset if it doesn't exist
        dataset_id = f"{gcp_details_config.PROJECT_ID}.{gcp_details_config.BIGQUERY_DATASETS['silver']}"
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "US"
        self.bq_client.create_dataset(dataset, exists_ok=True)
        logger.info(f"✅ Dataset created/verified: {dataset_id}")
        
        # Define schema for silver table
        schema = [
            bigquery.SchemaField("tweet_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("original_text", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("cleaned_text", "STRING"),
            bigquery.SchemaField("author_id", "STRING"),
            bigquery.SchemaField("created_at", "TIMESTAMP"),
            bigquery.SchemaField("query", "STRING"),
            bigquery.SchemaField("retweet_count", "INTEGER"),
            bigquery.SchemaField("like_count", "INTEGER"),
            bigquery.SchemaField("reply_count", "INTEGER"),
            bigquery.SchemaField("ingested_at", "TIMESTAMP"),
            bigquery.SchemaField("textblob_polarity", "FLOAT64"),
            bigquery.SchemaField("textblob_subjectivity", "FLOAT64"),
            bigquery.SchemaField("vader_compound", "FLOAT64"),
            bigquery.SchemaField("vader_positive", "FLOAT64"),
            bigquery.SchemaField("vader_negative", "FLOAT64"),
            bigquery.SchemaField("vader_neutral", "FLOAT64"),
            bigquery.SchemaField("sentiment_label", "STRING"),
            bigquery.SchemaField("confidence_score", "FLOAT64"),
            bigquery.SchemaField("processed_at", "TIMESTAMP", mode="REQUIRED")
        ]
        
        table = bigquery.Table(self.silver_table, schema=schema)
        
        # Create table with partitioning and clustering
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="processed_at"
        )
        table.clustering_fields = ["sentiment_label", "query"]
        
        table = self.bq_client.create_table(table, exists_ok=True)
        logger.info(f"✅ Silver table created/verified: {self.silver_table}")
        return True
    
    def fetch_unprocessed_tweets(self, limit=100):
        """Fetch tweets from Bronze layer that haven't been processed"""
        
        query = f"""
        SELECT 
            tweet_id,
            text,
            author_id,
            created_at,
            query,
            retweet_count,
            like_count,
            reply_count,
            ingested_at
        FROM `{self.bronze_table}`
        WHERE tweet_id NOT IN (
            SELECT tweet_id FROM `{self.silver_table}`
        )
        ORDER BY ingested_at DESC
        LIMIT {limit}
        """
        
        try:
            query_job = self.bq_client.query(query)
            results = query_job.result()
            
            tweets = []
            for row in results:
                tweets.append({
                    'tweet_id': row.tweet_id,
                    'text': row.text,
                    'author_id': row.author_id,
                    'created_at': row.created_at,
                    'query': row.query,
                    'retweet_count': row.retweet_count,
                    'like_count': row.like_count,
                    'reply_count': row.reply_count,
                    'ingested_at': row.ingested_at
                })
            
            logger.info(f"📊 Found {len(tweets)} unprocessed tweets")
            return tweets
            
        except Exception as e:
            logger.error(f"❌ Error fetching tweets: {e}")
            return []
    
    def process_tweets(self, tweets):
        """Process tweets and add sentiment analysis"""
        
        processed_rows = []
        
        for tweet in tweets:
            # Analyze sentiment
            sentiment = self.analyzer.analyze_sentiment(tweet['text'])
            
            # Convert datetime objects to ISO strings
            created_at = self._convert_datetime(tweet['created_at'])
            ingested_at = self._convert_datetime(tweet['ingested_at'])
            
            # Prepare row for Silver table
            row = {
                'tweet_id': tweet['tweet_id'],
                'original_text': sentiment['original_text'],
                'cleaned_text': sentiment['cleaned_text'],
                'author_id': tweet['author_id'],
                'created_at': created_at,
                'query': tweet['query'],
                'retweet_count': tweet['retweet_count'],
                'like_count': tweet['like_count'],
                'reply_count': tweet['reply_count'],
                'ingested_at': ingested_at,
                'textblob_polarity': sentiment['textblob_polarity'],
                'textblob_subjectivity': sentiment['textblob_subjectivity'],
                'vader_compound': sentiment['vader_compound'],
                'vader_positive': sentiment['vader_positive'],
                'vader_negative': sentiment['vader_negative'],
                'vader_neutral': sentiment['vader_neutral'],
                'sentiment_label': sentiment['sentiment_label'],
                'confidence_score': sentiment['confidence_score'],
                'processed_at': datetime.utcnow().isoformat()
            }
            
            # Validate JSON serialization before adding
            try:
                json.dumps(row)
                processed_rows.append(row)
                logger.debug(f"Processed tweet: {tweet['tweet_id']} - {sentiment['sentiment_label']}")
            except Exception as e:
                logger.error(f"❌ Failed to serialize tweet {tweet['tweet_id']}: {e}")
                # Create a fallback row without datetime issues
                fallback_row = row.copy()
                fallback_row['created_at'] = None
                fallback_row['ingested_at'] = None
                try:
                    json.dumps(fallback_row)
                    processed_rows.append(fallback_row)
                    logger.warning(f"Used fallback for tweet {tweet['tweet_id']}")
                except:
                    logger.error(f"❌ Could not create fallback for tweet {tweet['tweet_id']}")
        
        return processed_rows
    
    def insert_to_silver(self, rows):
        """Insert processed rows to Silver table"""
        
        if not rows:
            logger.info("No rows to insert")
            return 0
        
        try:
            errors = self.bq_client.insert_rows_json(self.silver_table, rows)
            
            if errors:
                logger.error(f"❌ Insert errors: {errors}")
                return 0
            else:
                logger.info(f"✅ Successfully inserted {len(rows)} tweets to Silver layer")
                return len(rows)
                
        except Exception as e:
            logger.error(f"❌ Error inserting to Silver: {e}")
            # Try to log what went wrong with each row
            for i, row in enumerate(rows):
                try:
                    json.dumps(row)
                except Exception as row_error:
                    logger.error(f"Row {i} JSON error: {row_error}")
                    logger.error(f"Row data: {row}")
            return 0
    
    def run_pipeline(self, limit=50):
        """Run the complete Silver layer pipeline"""
        
        logger.info("🚀 Starting Silver Layer Pipeline")
        logger.info("=" * 50)
        
        # Step 1: Ensure table exists
        self.create_silver_table()
        
        # Step 2: Fetch unprocessed tweets
        tweets = self.fetch_unprocessed_tweets(limit=limit)
        
        if not tweets:
            logger.info("No new tweets to process")
            return 0
        
        # Step 3: Process tweets (sentiment analysis)
        processed_rows = self.process_tweets(tweets)
        
        if not processed_rows:
            logger.warning("No rows could be processed (serialization issues)")
            return 0
        
        # Step 4: Insert to Silver table
        inserted_count = self.insert_to_silver(processed_rows)
        
        # Step 5: Show summary
        logger.info("=" * 50)
        if inserted_count > 0:
            # Show sentiment distribution
            sentiment_counts = {}
            for row in processed_rows:
                label = row['sentiment_label']
                sentiment_counts[label] = sentiment_counts.get(label, 0) + 1
            
            logger.info("📊 Sentiment Distribution:")
            for label, count in sentiment_counts.items():
                logger.info(f"   {label}: {count} tweets")
        
        logger.info(f"🎉 Pipeline complete. Processed {inserted_count} tweets")
        return inserted_count

def main():
    """Main function to run the pipeline"""
    pipeline = SilverLayerPipeline()
    pipeline.run_pipeline(limit=20)  # Start with small batch

if __name__ == "__main__":
    main()