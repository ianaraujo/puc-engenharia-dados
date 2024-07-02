# Databricks notebook source
# MAGIC %md
# MAGIC ## Importando as bibliotecas necessárias

# COMMAND ----------

import requests
from requests.exceptions import HTTPError
from zipfile import ZipFile
from io import BytesIO
from pyspark.sql import SparkSession

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

    def generate_urls(self): 
        years = range(self.start_year, self.end_year + 1)
        urls = [BASE_URL + f'{str(year)}/{quarter}T{str(year)}.zip' for year in years for quarter in range(1, 5)]

        return urls
    
    def fetch_data(self, target_urls):
        spark_dataframes = []

        for url in target_urls:
            try:
                response = requests.get(url)
                response.raise_for_status()
            
            except requests.HTTPError as e:
                print(f'HTTP Error: {e}')
                continue

            file_content = BytesIO(response.content)

            with ZipFile(file_content, 'r') as zip:
                file = [file for file in zip.namelist() if file.endswith('.csv')]

                if len(file) != 1:
                    print(f'Expected only one CSV file in {url}')
                    continue

                with zip.open(file[0]) as data:
                    temp_path = 'dbfs:/tmp/{file[0]}'

                    with open(temp_path, 'wb') as temp_file:
                        temp_file.write(data.read())

                    df = self.spark.read.csv(
                        'file:' + temp_path, 
                        sep=';',
                        encoding='latin1',
                        header=True,
                        inferSchema=True
                    )

                    dbutils.fs.rm(temp_path)
                
                    spark_dataframes.append(df)
        
        if not spark_dataframes:
            return None
        
        full_df = spark_dataframes[0]
        
        for df in spark_dataframes[1:]:
            full_df = full_df.union(df)
            
        return full_df

    def download_data(self):
        urls = self.generate_urls()
        full_df = self.fetch_data(urls)

        return full_df


# COMMAND ----------

collector = Collector(start_year=2014, end_year=2023) # 10 anos

df = collector.download_data()

if df.isEmpty():
    raise ValueError('No valid data found')

df.show(10)
