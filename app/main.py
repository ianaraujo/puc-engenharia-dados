import os
import shutil
import boto3
import pandas as pd

from sqlalchemy import Engine, create_engine

from settings import (
    DATABASE_URL, AWS_CREDENTIALS, BUCKET, S3_OBJECT_KEYS
)


temp_dir = 'tmp/'

engine = create_engine(DATABASE_URL)

s3 = boto3.client('s3', **AWS_CREDENTIALS)


def read_from_s3(bucket: str, key: str) -> pd.DataFrame:
    """
    Download object from S3 and return a pandas DataFrame.
    """
    temp_path = f'{temp_dir}{key}'

    try:
        s3.download_file(bucket, key, temp_path)
        print(f"File '{key}' download complete!")
    
    except Exception as e:
        print(f"Faied to download '{key}' file: {e}")

    table = pd.read_parquet(temp_path)

    return table


def full_load_database(engine: Engine, objects: dict) -> None:
    """
    Receive a mapping of object sources and load the database.
    """
    for table, key in objects.items():

        data = read_from_s3(BUCKET, key)

        data.to_sql(table, engine, if_exists='replace', index=False)
        
        print(f"Table '{table}' loaded successfully!")


if __name__ == '__main__':    
    
    os.makedirs(temp_dir, exist_ok=True)
    
    full_load_database(engine, objects=S3_OBJECT_KEYS)

    shutil.rmtree(temp_dir)