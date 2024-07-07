# Databricks notebook source
import requests
from io import BytesIO
from zipfile import ZipFile

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F

BASE_URL = 'https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/'

# COMMAND ----------

spark = SparkSession.builder.appName('CollectData').getOrCreate()

class Collector:

    def __init__(self, start_year: int, end_year: int, spark: SparkSession):
        self.start_year = start_year
        self.end_year = end_year
        self.spark = spark

    def generate_urls(self) -> list[str]: 
        years = range(self.start_year, self.end_year + 1)
        urls = [BASE_URL + f'{str(year)}/{quarter}T{str(year)}.zip' for year in years for quarter in range(1, 5)]

        # Add specific path name for 3T2017
        alternative_url = BASE_URL + '2017/3-Trimestre.zip'
        urls = [alternative_url if url == BASE_URL + '2017/3T2017.zip' else url for url in urls]

        return urls
    
    def download_data(self, target_url: str) -> None:
        """
        Download a zipped file from source, extract the CSV, and save to a landing zone.
        """
        try:
            response = requests.get(target_url)
            response.raise_for_status()
        
        except requests.HTTPError as e:
            print(f'HTTP Error: {e}')
            return None

        file_content = BytesIO(response.content)

        try:
            with ZipFile(file_content, 'r') as zip_file:
                csv_files = [file for file in zip_file.namelist() if file.endswith('.csv')]
                
                if len(csv_files) != 1:
                    raise ValueError(f'Expected exactly one CSV file in {target_url}, found {len(csv_files)}')
                
                csv_file_name = csv_files[0]

                with zip_file.open(csv_file_name) as data:
                    path = '/Volumes/raw/ans/demonstracoes_contabeis/'
                    
                    with open(path + csv_file_name, 'wb') as csv_file:
                        csv_file.write(data.read())
                    
        except Exception as e:
            print(f'An error occurred: {e}')
            return None

    def ingest_raw(self) -> None:
        urls = self.generate_urls()

        for url in urls:
            print(f'Collecting data from {url} ...')
            self.download_data(url)
        
        print('Raw data ingested successfully!')

    def ingest_bronze(self) -> None:
        df = self.spark.read.csv(
            path='/Volumes/raw/ans/demonstracoes_contabeis', 
            sep=';',
            encoding='latin1',
            header=True,
            inferSchema=True
        )

        if df.isEmpty():
            print('No data to ingest!')
            return None

        (df.write
            .format('delta')
            .mode('overwrite')
            .saveAsTable('bronze.ans.demonstracoes_contabeis'))

    def run(self) -> None:
        self.ingest_raw()
        self.ingest_bronze()

        print('Data ingestion completed!')


# COMMAND ----------

collector = Collector(start_year=2014, end_year=2023) # 10 anos
collector.run()

# COMMAND ----------

def process_silver(source: str) -> DataFrame: 
    """
    Process data and create silver layer.
    """	
    df = spark.read.format('delta').table(source)

    # fix data types
    df = (df.drop('DESCRICAO', 'VL_SALDO_FINAL')
          .withColumn('VL_SALDO_INICIAL', F.regexp_replace('VL_SALDO_INICIAL', ',', '.').cast('double'))
          .withColumn('DATA', F.to_date(df['DATA'], format='dd/MM/yyyy'))
    )

    # remove null dates
    df = df.filter(df['DATA'].isNotNull())

    # evolve schema
    transformed_df = df.select(
        F.year('DATA').alias('ANO'),
        F.quarter('DATA').alias('TRIMESTRE'),
        'REG_ANS',
        'CD_CONTA_CONTABIL',
        'VL_SALDO_INICIAL'
    )

    return transformed_df

# COMMAND ----------

def sinistralidade_gold(source: str) -> DataFrame:
    """
    Process data and create 'sinistralidade' gold layer.
    """
    contas = {'41': 'DESPESAS', '311': 'RECEITAS'}

    df = spark.read.format('delta').table(source)
    
    filter_df = df.filter(df.CD_CONTA_CONTABIL.isin(list(contas.keys())))

    pivot_df = filter_df \
        .groupBy(['ANO', 'TRIMESTRE']) \
        .pivot('CD_CONTA_CONTABIL') \
        .agg(F.sum('VL_SALDO_INICIAL'))

    pivot_df = pivot_df.withColumnsRenamed(contas)

    formula = (F.col('DESPESAS') / F.col('RECEITAS'))
    
    final = pivot_df \
        .withColumn('SINISTRALIDADE', formula) \
        .select(
            'ANO', 
            'TRIMESTRE', 
            F.round(F.col('SINISTRALIDADE') * 100, 2).alias('SINISTRALIDADE')) \
        .orderBy(['ANO', 'TRIMESTRE'], ascending=True)
    
    return final

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT COUNT(*) AS total FROM bronze.ans.demonstracoes_contabeis

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC WITH total_rows AS (
# MAGIC     SELECT COUNT(*) AS total FROM bronze.ans.demonstracoes_contabeis
# MAGIC )
# MAGIC
# MAGIC SELECT 
# MAGIC     COUNT(CASE WHEN VL_SALDO_INICIAL IS NOT NULL THEN 1 END) AS count_saldo_inicial,
# MAGIC     COUNT(CASE WHEN VL_SALDO_FINAL IS NOT NULL THEN 1 END) AS count_saldo_final,
# MAGIC     COUNT(CASE WHEN VL_SALDO_INICIAL IS NOT NULL THEN 1 END) * 100 / total_rows.total AS percent_saldo_inicial,
# MAGIC     COUNT(CASE WHEN VL_SALDO_FINAL IS NOT NULL THEN 1 END) * 100 / total_rows.total AS percent_saldo_final
# MAGIC FROM bronze.ans.demonstracoes_contabeis, total_rows
# MAGIC GROUP BY total_rows.total
# MAGIC

# COMMAND ----------

table = 'demonstracoes_contabeis'
database = 'ans'

path_name = f'{database}.{table}'

# COMMAND ----------

# silver layer

df_silver = process_silver(source=f'bronze.{path_name}')
df_silver.show()

(df_silver.write
    .format('delta')
    .mode('overwrite')
    .saveAsTable(f'silver.{path_name}')
)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM silver.ans.demonstracoes_contabeis

# COMMAND ----------

sinistralidade = sinistralidade_gold(source=f'silver.{path_name}')
sinistralidade.display()

(sinistralidade.write
    .format('delta')
    .mode('overwrite')
    .saveAsTable(f'gold.{database}.sinistralidade')
)
