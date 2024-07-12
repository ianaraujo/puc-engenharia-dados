from os import environ

db_user = environ['POSTGRES_USER']
db_password = environ['POSTGRES_PASSWORD']

DATABASE_URL = f'postgresql://{db_user}:{db_password}@db:5432/postgres'

AWS_CREDENTIALS = {
    'aws_access_key_id': environ['AWS_ACCESS_KEY_ID'],
    'aws_secret_access_key': environ['AWS_SECRET_ACCESS_KEY'],
    'region_name': 'us-east-2'
}

BUCKET = 'databricks-gold-ans'

S3_OBJECT_KEYS = {
    ## set table name: bucket file name
    'sinistralidade': 'sinistralidade.parquet',
    'market_share': 'market_share.parquet',
    'num_beneficiarios': 'num_beneficiarios.parquet',
    'num_operadoras': 'num_operadoras.parquet',
    'custo_beneficiario': 'custo_beneficiario.parquet'
}