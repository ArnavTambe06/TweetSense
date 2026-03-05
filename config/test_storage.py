# test_storage.py
import os
from google.cloud import storage

# Set credentials
KEY_FILE_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'config', 'GcpKey.json')
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = KEY_FILE_PATH

client = storage.Client()
buckets = list(client.list_buckets())
print("✅ Storage access working! Your buckets:")
for bucket in buckets:
    print(f"   - {bucket.name}")