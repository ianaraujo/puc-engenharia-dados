import os

DATABASE = 'databricks'

AWS_CREDENTIALS = {
    'aws_access_key_id': os.environ['AWS_ACCESS_KEY_ID'],
    'aws_secret_access_key': os.environ['AWS_SECRET_ACCESS_KEY'],
    'region_name': 'us-east-2'
}

BUCKET = 'databricks-gold-layer'

S3_OBJECT_KEYS = {
    'sinistralidade': 'sinistralidade.parquet'
}