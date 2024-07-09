# Databricks notebook source
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# COMMAND ----------

spark = SparkSession.builder.appName('Transform').config('delta.autoOptimize.optimizeWrite', 'true').getOrCreate()

# COMMAND ----------

schema = 'ans'
year = '2024'
month = '05'

table_name = f'beneficiarios_{year}_{month}'

df = spark.sql(f'SELECT * FROM bronze.{schema}.{table_name}')

df.display()

# COMMAND ----------

df = df \
    .withColumn('CD_OPERADORA', df['CD_OPERADORA'].cast('string'))  \
    .filter(df['COBERTURA_ASSIST_PLAN'] == "Médico-hospitalar") \
    .select(
        F.lit(int(year)).alias('ANO'),
        F.lit(int(month)).alias('MES'),
        'CD_OPERADORA',
        'NM_RAZAO_SOCIAL',
        'DE_CONTRATACAO_PLANO',
        'QT_BENEFICIARIO_ATIVO'
    )

df = df.repartition(1)

# COMMAND ----------

df.write.mode('overwrite').format('delta').saveAsTable(f'silver.{schema}.{table_name}')

print(f"Table transformation completed. Saved to 'silver.{schema}.{table_name}' successfully!")
