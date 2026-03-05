import os
import sys
import logging
from datetime import datetime
from google.cloud import bigquery

# -------------------------------------------------
# Project Path Setup (Optional - keep if needed)
# -------------------------------------------------
project_root = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)
sys.path.insert(0, project_root)

KEY_FILE_PATH = os.path.join(project_root, "config", "GcpKey.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = KEY_FILE_PATH


from config import gcp_details_config


# -------------------------------------------------
# Logging
# -------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)


# -------------------------------------------------
# Gold Layer Pipeline
# -------------------------------------------------
class GoldLayerPipeline:

    def __init__(self):

        self.client = bigquery.Client()

        # Project / Datasets
        self.project = gcp_details_config.PROJECT_ID
        self.silver_ds = gcp_details_config.BIGQUERY_DATASETS["silver"]
        self.gold_ds = gcp_details_config.BIGQUERY_DATASETS["gold"]

        # Tables
        self.silver_table = (
            f"{self.project}.{self.silver_ds}.tweet_sentiment"
        )

        self.fact_table = (
            f"{self.project}.{self.gold_ds}.daily_sentiment_fact"
        )

        self.metadata_table = (
            f"{self.project}.{self.gold_ds}.pipeline_metadata"
        )

        self.pipeline_name = "daily_sentiment_pipeline"


    # -------------------------------------------------
    # Create Gold Dataset
    # -------------------------------------------------
    def create_dataset(self):

        dataset_id = f"{self.project}.{self.gold_ds}"

        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "US"

        self.client.create_dataset(dataset, exists_ok=True)

        logger.info(f"✅ Dataset ready: {dataset_id}")


    # -------------------------------------------------
    # Create Metadata Table
    # -------------------------------------------------
    def create_metadata_table(self):

        query = f"""
        CREATE TABLE IF NOT EXISTS `{self.metadata_table}` (
            pipeline_name STRING,
            last_processed TIMESTAMP
        )
        """

        self.client.query(query).result()

        logger.info("✅ Metadata table ready")


    # -------------------------------------------------
    # Create Fact Table
    # -------------------------------------------------
    def create_fact_table(self):

        query = f"""
        CREATE TABLE IF NOT EXISTS `{self.fact_table}` (
            date DATE,
            sentiment_label STRING,
            tweet_count INT64,
            avg_sentiment_score FLOAT64,
            avg_confidence FLOAT64,
            created_at TIMESTAMP
        )
        PARTITION BY date
        """

        self.client.query(query).result()

        logger.info("✅ Fact table ready")


    # -------------------------------------------------
    # Get Watermark
    # -------------------------------------------------
    def get_last_processed(self):

        query = f"""
        SELECT last_processed
        FROM `{self.metadata_table}`
        WHERE pipeline_name = '{self.pipeline_name}'
        """

        rows = list(self.client.query(query))

        if not rows:
            logger.info("ℹ️ First run detected (no watermark)")
            return None

        return rows[0].last_processed


    # -------------------------------------------------
    # Update Watermark
    # -------------------------------------------------
    def update_watermark(self, timestamp):

        query = f"""
        MERGE `{self.metadata_table}` t
        USING (
            SELECT
                '{self.pipeline_name}' AS pipeline_name,
                TIMESTAMP('{timestamp}') AS last_processed
        ) s
        ON t.pipeline_name = s.pipeline_name

        WHEN MATCHED THEN
          UPDATE SET last_processed = s.last_processed

        WHEN NOT MATCHED THEN
          INSERT (pipeline_name, last_processed)
          VALUES (s.pipeline_name, s.last_processed)
        """

        self.client.query(query).result()

        logger.info(f"✅ Watermark updated: {timestamp}")


    # -------------------------------------------------
    # Incremental Load
    # -------------------------------------------------
    def load_incremental_data(self):

        last_ts = self.get_last_processed()

        if last_ts:
            condition = f"processed_at > TIMESTAMP('{last_ts}')"
        else:
            condition = "1=1"

        logger.info(f"📌 Increment condition: {condition}")


        # Insert New Aggregates
        insert_query = f"""
        INSERT INTO `{self.fact_table}`
        SELECT
            DATE(processed_at) AS date,
            sentiment_label,

            COUNT(*) AS tweet_count,

            ROUND(AVG(vader_compound),4) AS avg_sentiment_score,
            ROUND(AVG(confidence_score),4) AS avg_confidence,

            CURRENT_TIMESTAMP() AS created_at

        FROM `{self.silver_table}`

        WHERE {condition}

        GROUP BY date, sentiment_label
        """

        job = self.client.query(insert_query)
        job.result()

        logger.info("✅ New data inserted")


        # Find New Max Timestamp
        max_query = f"""
        SELECT MAX(processed_at) AS max_ts
        FROM `{self.silver_table}`
        WHERE {condition}
        """

        rows = list(self.client.query(max_query))

        max_ts = rows[0].max_ts

        if max_ts:
            self.update_watermark(max_ts)
        else:
            logger.info("ℹ️ No new data found")


    # -------------------------------------------------
    # Run Pipeline
    # -------------------------------------------------
    def run(self):

        logger.info("🚀 Starting Gold Layer Pipeline")
        logger.info("=" * 60)

        self.create_dataset()
        self.create_metadata_table()
        self.create_fact_table()

        self.load_incremental_data()

        logger.info("=" * 60)
        logger.info("🎉 Gold Pipeline Completed Successfully")


# -------------------------------------------------
# Main
# -------------------------------------------------
def main():

    pipeline = GoldLayerPipeline()
    pipeline.run()


if __name__ == "__main__":
    main()
