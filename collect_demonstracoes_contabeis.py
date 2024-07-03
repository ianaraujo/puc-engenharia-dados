# Databricks notebook source
# MAGIC %md
# MAGIC ## Importando as bibliotecas necessárias

# COMMAND ----------

import requests
from requests.exceptions import HTTPError

from io import BytesIO
from zipfile import ZipFile

from pyspark.sql import SparkSession, DataFrame

BASE_URL = 'https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline de ingestão dos dados

# COMMAND ----------

class Collector:

    def __init__(self, start_year, end_year):
        self.start_year = start_year
        self.end_year = end_year
        
        self.spark = SparkSession.builder.appName('CollectData').getOrCreate()

    def generate_urls(self) -> list[str]: 
        years = range(self.start_year, self.end_year + 1)
        urls = [BASE_URL + f'{str(year)}/{quarter}T{str(year)}.zip' for year in years for quarter in range(1, 5)]

        # Add specific path name for 3T2017
        alternative_url = BASE_URL + '2017/3-Trimestre.zip'
        urls = [alternative_url if url == BASE_URL + '2017/3T2017.zip' else url for url in urls]

        return urls
    
    def fetch_data(self, target_url: str) -> DataFrame:
        """
        Downloads a zipped file from source, saves as a temp file to be read by Spark and returns a Spark DataFrame.
        """
        try:
            response = requests.get(target_url)
            response.raise_for_status()
        
        except requests.HTTPError as e:
            print(f'HTTP Error: {e}')
            return None
           
        file_content = BytesIO(response.content)

        with ZipFile(file_content, 'r') as zip:
            file = [file for file in zip.namelist() if file.endswith('.csv')]

            if len(file) > 1:
                raise ValueError(f'Expected only one CSV file in {target_url}')
            
            csv_file_name = file[0]

            with zip.open(csv_file_name) as data:
                temp_path = '/tmp/'
                
                with open(temp_path + csv_file_name, 'wb') as temp_file:
                    temp_file.write(data.read())

            df = self.spark.read.csv(
                path=temp_path,
                sep=';',
                encoding='latin1',
                header=True,
                inferSchema=True
            )    
            
        return df

    def ingest_bronze(self) -> None:
        urls = self.generate_urls()

        for url in urls:
            print(f'Collecting data from {url} ...')
            df = self.fetch_data(url)

            if df is None:
                print(f'Failed to dowload data at {url}')
                continue

            table_name = f"bronze.demonstracoes_contabeis.{url.split('/')[-1].replace('.zip', '')}"

            df.write.mode('overwrite').format('delta').saveAsTable(table_name)
        
        print('Bronze data ingested successfully!')

# COMMAND ----------

collector = Collector(start_year=2014, end_year=2023) # 10 anos
collector.ingest_bronze()
