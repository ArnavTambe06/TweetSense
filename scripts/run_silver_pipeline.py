# scripts/run_silver_pipeline.py
import sys
import os

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from src.processing.silver_pipeline import SilverLayerPipeline

def run_silver_pipeline():
    print("🚀 Running Silver Layer Pipeline...")
    
    try:
        pipeline = SilverLayerPipeline()
        processed_count = pipeline.run_pipeline(limit=20)
        
        if processed_count > 0:
            print(f"✅ Successfully processed {processed_count} tweets")
            print("🎉 Silver layer created!")
        else:
            print("ℹ️ No new tweets to process")
            
    except Exception as e:
        print(f"❌ Pipeline failed: {e}")
        return False

if __name__ == "__main__":
    run_silver_pipeline()