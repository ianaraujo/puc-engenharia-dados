# Databricks notebook source
import requests
from bs4 import BeautifulSoup
from io import BytesIO
from zipfile import ZipFile
import tempfile

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F

spark = SparkSession.builder.appName('Beneficarios').config('delta.autoOptimize.optimizeWrite', 'true').getOrCreate()

# COMMAND ----------

class Beneficiarios:

    def __init__(self, spark: SparkSession, endpoint: str, year: str, month: str):

        self.year = year
        self.month = month
        self.spark = spark

        self.base_url = 'https://dadosabertos.ans.gov.br/FTP/PDA'
        self.url = f'{self.base_url}/{endpoint}/{year}{month}'

        self.table_name = f'beneficiarios_{self.year}_{self.month}'

    def _fetch_data(self, url: str) -> requests.Response:
        try:
            response = requests.get(url)
            response.raise_for_status()

            return response
        
        except requests.HTTPError as e:
            print(f'HTTP Error: {e}')
            return None

    def _get_zip_urls(self) -> list[str]:
        soup = BeautifulSoup(self._fetch_data(self.url).content, 'html.parser')
        links = soup.find_all('a')

        zip_urls = [f"{self.url}/{url.get('href')}" for url in links if url.get('href').endswith('.zip')]

        return zip_urls
    
    def _extract_raw_files(self, zip_urls: list[str]) -> None:

        for url in zip_urls:
            content = BytesIO(self._fetch_data(url).content)

            with ZipFile(content, 'r') as zip_file:
                csv_files = [file for file in zip_file.namelist() if file.endswith('.csv')]
                
                if len(csv_files) != 1:
                    raise ValueError(f'Expected exactly one CSV file in {url}, found {len(csv_files)}')
                
                csv_file_name = csv_files[0]
                volume = f'/Volumes/raw/ans/beneficiarios/{csv_file_name}' 

                with zip_file.open(csv_file_name) as data:
                    csv_content = data.read()
                    
                with open(volume, 'wb') as f:
                    f.write(csv_content)
        
            print(f"Extracted '{csv_file_name}' to {volume}")

    def ingest_bronze(self, catalog: str, schema: str) -> None:
        zip_urls = self._get_zip_urls()

        self._extract_raw_files(zip_urls)

        df = spark.read.csv(f'/Volumes/raw/ans/beneficiarios', sep=';', header=True, inferSchema=True)
 
        df.write \
            .format('delta') \
            .mode('overwrite') \
            .saveAsTable(f'{catalog}.{schema}.{self.table_name}')

        print(f"Table '{self.table_name}' loaded successfully!")

    def tranform_silver(self, catalog: str, schema: str) -> None:

        df = spark.sql(f'SELECT * FROM {ingestion_catalog}.{schema}.{self.table_name}')
        
        silver = df \
            .withColumn('CD_OPERADORA', df['CD_OPERADORA'].cast('string'))  \
            .filter(df['COBERTURA_ASSIST_PLAN'] == "Médico-hospitalar") \
            .select(
                F.lit(int(self.year)).alias('ANO'),
                F.lit(int(self.month)).alias('MES'),
                'CD_OPERADORA',
                'NM_RAZAO_SOCIAL',
                'DE_CONTRATACAO_PLANO',
                'QT_BENEFICIARIO_ATIVO'
            )
        silver = silver.coalesce(1)

        silver.write.mode('overwrite').format('delta').saveAsTable(f'{catalog}.{schema}.{self.table_name}')

        print(f"Table transformation completed. Saved to '{self.table_name}' successfully!")

# COMMAND ----------

endpoint = 'informacoes_consolidadas_de_beneficiarios'

pipeline = Beneficiarios(spark, endpoint, year='2024', month='05')

# COMMAND ----------

ingestion_catalog = 'bronze'
catalog = 'silver'
schema = 'ans'


# COMMAND ----------

# pipeline.ingest_bronze(ingestion_catalog, schema)


# COMMAND ----------

pipeline.tranform_silver(catalog, schema)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TEMPORARY VIEW num_beneficiarios AS (
# MAGIC   SELECT CD_OPERADORA, NM_RAZAO_SOCIAL, SUM(QT_BENEFICIARIO_ATIVO) AS NUM_BENEFICIARIOS
# MAGIC   FROM silver.ans.beneficiarios_2024_05
# MAGIC   GROUP BY CD_OPERADORA, NM_RAZAO_SOCIAL
# MAGIC   ORDER BY NUM_BENEFICIARIOS DESC
# MAGIC )
# MAGIC

# COMMAND ----------

num_beneficiarios = spark.read.table('num_beneficiarios')
num_beneficiarios.write.mode('overwrite').format('delta').saveAsTable(f'gold.{schema}.num_{pipeline.table_name}')

print("Table 'num_{pipeline.table_name}' created sucessfully!")
