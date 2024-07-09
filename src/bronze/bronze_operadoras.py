# Databricks notebook source
import requests
from io import BytesIO

import pyspark.sql.functions as F
from pyspark.sql import SparkSession, DataFrame

# COMMAND ----------

spark = SparkSession.builder.appName('Collect').config('delta.autoOptimize.optimizeWrite', 'true').getOrCreate()

# COMMAND ----------

url_relatorio_cadop = 'https://dadosabertos.ans.gov.br/FTP/PDA/operadoras_de_plano_de_saude_ativas/Relatorio_cadop.csv' 


# COMMAND ----------

def download_data(url: str, dest: str) -> None:
    response = requests.get(url)
    response.raise_for_status()

    with open(dest, 'wb') as f:
        f.write(response.content)
    
    print(f'Downloaded raw data to {dest} sucessfully!')

# COMMAND ----------

raw_path = '/dbfs/tmp/relatorio_cadop.csv'

download_data(url=url_relatorio_cadop, dest=raw_path)

# COMMAND ----------

catalog = 'bronze'
schema = 'ans'
table = 'operadoras'

df = spark.read.csv('dbfs:/tmp/relatorio_cadop.csv', sep=';', header=True, inferSchema=True)

df.display()

# COMMAND ----------

df.write.mode('overwrite').format('delta').saveAsTable(f'{catalog}.{schema}.{table}')

print('Table created succesfully!')
