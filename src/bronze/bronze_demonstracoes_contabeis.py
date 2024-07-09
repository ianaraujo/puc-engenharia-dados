# Databricks notebook source
import requests
from bs4 import BeautifulSoup
from io import BytesIO
from zipfile import ZipFile
import tempfile

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F

# COMMAND ----------

spark = SparkSession.builder.appName('Collector').config('delta.autoOptimize.optimizeWrite', 'true').getOrCreate()

# COMMAND ----------

class Collector:

    def __init__(self, spark: SparkSession, endpoint: str, start_year: int, end_year: int):
        self.start_year = start_year
        self.end_year = end_year
        
        self.base_url = 'https://dadosabertos.ans.gov.br/FTP/PDA'
        self.endpoint = endpoint
        self.url = f'{self.base_url}/{endpoint}/'
        self.volume = f'/Volumes/raw/ans/{endpoint}'

        self.spark = spark

    def _fetch_data(self, url: str) -> requests.Response:
        try:
            response = requests.get(url)
            response.raise_for_status()

            return response
        
        except requests.HTTPError as e:
            print(f'HTTP Error: {e}')
            return None

    def _generate_urls(self) -> list[str]: 
        years = range(self.start_year, self.end_year + 1)
        urls = [self.url + f'{str(year)}/{quarter}T{str(year)}.zip' for year in years for quarter in range(1, 5)]

        # add specific url name for 3T2017
        alternative_url = self.url + '2017/3-Trimestre.zip'
        urls = [alternative_url if url == self.url + '2017/3T2017.zip' else url for url in urls]

        return urls
    
    def _extract_raw_files(self, zip_urls: list[str]) -> None:
        for url in zip_urls:
            content = BytesIO(self._fetch_data(url).content)

            with ZipFile(content, 'r') as zip_file:
                csv_files = [file for file in zip_file.namelist() if file.endswith('.csv')]
                
                if len(csv_files) != 1:
                    raise ValueError(f'Expected exactly one CSV file in {url}, found {len(csv_files)}')
                
                csv_file_name = csv_files[0]
                dest_file = f'{self.volume}/{csv_file_name}' 

                with zip_file.open(csv_file_name) as data:
                    csv_content = data.read()
                    
                with open(dest_file, 'wb') as f:
                    f.write(csv_content)
        
            print(f"Extracted '{csv_file_name}' to {dest_file}")

    def ingest_raw(self) -> None:
        urls = self._generate_urls()
        self._extract_raw_files(urls)
        
        print('Raw data ingested successfully!')

    def ingest_bronze(self, catalog: str, schema: str) -> None:
        df = self.spark.read.csv(
            path=self.volume, 
            sep=';',
            encoding='latin1',
            header=True,
            inferSchema=True
        )

        if df.isEmpty():
            print('No data to ingest!')
            return None

        df.write \
            .format('delta') \
            .mode('overwrite') \
            .saveAsTable(f'{catalog}.{schema}.{self.endpoint}')

    def run(self, catalog: str, schema: str) -> None:
        # self.ingest_raw()
        self.ingest_bronze(catalog, schema)

        print('Data ingestion completed!')

# COMMAND ----------

catalog = 'bronze'
schema = 'ans'
endpoint = 'demonstracoes_contabeis'

pipeline = Collector(spark, endpoint, start_year=2014, end_year=2023)
pipeline.run(catalog, schema)
