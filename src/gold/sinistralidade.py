# Databricks notebook source
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# COMMAND ----------

spark = SparkSession.builder.appName('Transform').config('delta.autoOptimize.optimizeWrite', 'true').getOrCreate()

# COMMAND ----------

schema = 'ans'
table = 'demonstracoes_contabeis'

df = spark.read.format('delta').table(f'silver.{schema}.{table}')

df.display()

# COMMAND ----------

contas = {
    '41': 'DESPESAS', 
    '311': 'RECEITAS'
}

formula = (F.col('DESPESAS') / F.col('RECEITAS'))

# COMMAND ----------

df = df.filter(df.CD_CONTA_CONTABIL.isin(list(contas.keys())))

pivot = df \
    .groupBy(['ANO', 'TRIMESTRE', 'REG_ANS']) \
    .pivot('CD_CONTA_CONTABIL') \
    .agg(F.sum('VL_SALDO_INICIAL'))

pivot = pivot.withColumnsRenamed(contas)
pivot = pivot.na.drop(how='any')

pivot.display()

# COMMAND ----------

final = pivot \
    .withColumn('SINISTRALIDADE', formula) \
    .select(
        'ANO', 
        'TRIMESTRE', 
        'REG_ANS',
        F.round(F.col('SINISTRALIDADE') * 100, 2).alias('SINISTRALIDADE')) \
    .orderBy(['ANO', 'TRIMESTRE'], ascending=True)

final = final.repartition(1)
final.display()

# COMMAND ----------

final.write.mode('overwrite').format('delta').saveAsTable(f'gold.{schema}.sinistralidade')

print(f"Table transformation completed. Saved to 'gold.{schema}.sinistralidade' successfully!")
