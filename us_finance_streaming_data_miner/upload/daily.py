import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(os.getcwd(), 'credential.json')

from google.cloud import storage
from google.cloud.exceptions import NotFound

_client = None

_BUCKET_NAME = 'stock_daily_data'
_BLOB_NAME = 'us.daily.streaming.csv'

def _get_client():
    global _client
    if _client is None:
        _client = storage.Client()
    return _client

def get_latest_source_filename():
    base_dir = 'data'
    return os.path.join(base_dir, 'daily.csv')

def upload():
    try:
        client = _get_client()
        bucket = client.get_bucket(_BUCKET_NAME)
        blob_name = _BLOB_NAME
        blob = bucket.blob(blob_name)
        source_file = get_latest_source_filename()
        blob.upload_from_filename(source_file)
        print('File {} uploaded to {}.'.format(source_file, blob_name))
    except NotFound:
        print("Sorry, that bucket {} does not exist!".format(_BUCKET_NAME))
