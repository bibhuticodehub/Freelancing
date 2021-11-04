import os
# Imports the Google Cloud client library
from google.cloud import storage

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/bapu/Public/GCP_Service_Accounts/bibhuti-exploring-gcp-61202cb20d12.json'

# Instantiates a client
storage_client = storage.Client()

# The name for the new bucket
bucket_id = "bibhuti-gcp-bucket"
bucket = storage_client.get_bucket(bucket_id)

blob = bucket.blob('input_data/metric_data.sql')
blob.upload_from_filename(filename='/home/bapu/Downloads/metric_data.sql')
print("File copied from local to bucket.")