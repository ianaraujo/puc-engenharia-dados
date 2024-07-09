# Databricks notebook source
import requests
from io import BytesIO

import pyspark.sql.functions as F
from pyspark.sql import SparkSession, DataFrame

spark = SparkSession.builder.appName('Beneficarios').config('delta.autoOptimize.optimizeWrite', 'true').getOrCreate()

# COMMAND ----------

url_relatorio_cadop = 'https://dadosabertos.ans.gov.br/FTP/PDA/operadoras_de_plano_de_saude_ativas/Relatorio_cadop.csv' 

# COMMAND ----------

def download_data(url: str, dest: str) -> None:
    response = requests.get(url)
    response.raise_for_status()

    with open(dest, 'wb') as f:
        f.write(response.content)
    
    print(f'Downloaded raw data to {dest} sucessfully!')


def transform_silver(table: str) -> DataFrame:
    df = spark.sql(f'select * from {table}')

    upper_cols = [col.upper() for col in df.columns]

    df = df.toDF(*upper_cols)

    final_df = df \
        .withColumn('REGISTRO_ANS', df['REGISTRO_ANS'].cast('string')) \
        .withColumn('CNPJ', df['CNPJ'].cast('string')) \
        .select(
            'DATA_REGISTRO_ANS',
            'REGISTRO_ANS',
            'CNPJ',
            'NOME_FANTASIA'
        )

    return final_df
    

# COMMAND ----------

raw_path = '/dbfs/tmp/relatorio_cadop.csv'

download_data(url=url_relatorio_cadop, dest=raw_path)


# COMMAND ----------

operadoras = spark.read.csv('dbfs:/tmp/relatorio_cadop.csv', sep=';', header=True, inferSchema=True)

# COMMAND ----------

catalog = 'bronze'
schema = 'ans'
table = 'operadoras'

# COMMAND ----------

operadoras.write.mode('overwrite').format('delta').saveAsTable(f'{catalog}.{schema}.{table}')
print('Table created succesfully!')

# COMMAND ----------

silver = transform_silver(table=f'{catalog}.{schema}.{table}')

silver.write.mode('overwrite').format('delta').saveAsTable(f'silver.{schema}.{table}')
print('Table created succesfully!')
