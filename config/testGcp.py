# test_gcp_auth.py
import os
from google.cloud import pubsub_v1

# Point to your service account key
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'GcpKey.json'

try:
    publisher = pubsub_v1.PublisherClient()
    print(" GCP Authentication SUCCESS!")
except Exception as e:
    print(f" GCP Authentication FAILED: {e}")