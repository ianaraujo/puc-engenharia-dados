# Databricks notebook source
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# COMMAND ----------

spark = SparkSession.builder.appName('Transform').config('delta.autoOptimize.optimizeWrite', 'true').getOrCreate()

# COMMAND ----------

schema = 'ans'
table = 'demonstracoes_contabeis'

df = spark.sql(f'SELECT * FROM bronze.{schema}.{table}')

df.display()

# COMMAND ----------

df = df.drop('DESCRICAO', 'VL_SALDO_FINAL') \
    .filter(df['DATA'].isNotNull()) \
    .withColumn('VL_SALDO_INICIAL', F.regexp_replace('VL_SALDO_INICIAL', ',', '.').cast('double')) \
    .withColumn('DATA', F.coalesce(
        F.to_date(df['DATA'], 'dd/MM/yyyy'),
        F.to_date(df['DATA'], 'yyyy-MM-dd')
))

df = df.select(
    F.year('DATA').alias('ANO'),
    F.quarter('DATA').alias('TRIMESTRE'),
        'REG_ANS',
        'CD_CONTA_CONTABIL',
        'VL_SALDO_INICIAL'
    )

df = df.repartition(1)


# COMMAND ----------

df.write.mode('overwrite').format('delta').saveAsTable(f'silver.{schema}.{table}')

print(f"Table transformation completed. Saved to 'silver.{schema}.{table}' successfully!")
