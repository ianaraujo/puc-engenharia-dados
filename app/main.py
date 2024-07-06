import boto3
import sqlite3
import pandas as pd

import shutil
import os

from settings import (
    DATABASE, AWS_CREDENTIALS, BUCKET, S3_OBJECT_KEYS
)

temp_dir = 'tmp/'

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


def full_load_database(con: sqlite3.Connection, objects: dict) -> None:
    """
    Receive a mapping of object sources and load the database.
    """
    for table, key in objects.items():

        data = read_from_s3(BUCKET, key)
        data['TESTE'] = 'TESTE22222 COLUMN'
        data['TESTE2'] = 'TESTE COLUMN'

        data.to_sql(table, con, if_exists='replace', index=False)
        
        print(f"Table '{table}' loaded successfully!")


if __name__ == '__main__':    
    os.makedirs(temp_dir, exist_ok=True)
    
    with sqlite3.connect(f'{DATABASE}.db') as con:
        full_load_database(con, objects=S3_OBJECT_KEYS)

    shutil.rmtree(temp_dir)