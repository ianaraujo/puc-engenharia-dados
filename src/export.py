# Databricks notebook source
from pyspark.sql import DataFrame

# COMMAND ----------

bucket = 'databricks-gold-ans'
database = 'gold.ans'

tables = spark.sql(f'show tables in {database}')

# COMMAND ----------

def export_parquet(bucket: str, database: str, table: DataFrame) -> None:
    df = spark.read.table(f'{database}.{table.tableName}') 

    save_path = f'/Volumes/gold/ans/{bucket}/{table.tableName}'
    
    df.repartition(1) \
        .write.mode('overwrite') \
        .format('parquet') \
        .option("header","true") \
        .option("inferSchema", "true") \
        .save(save_path)

    parquet_file = [file.name for file in dbutils.fs.ls(save_path) if file.name.startswith('part-')]

    dbutils.fs.mv(save_path + "/" + parquet_file[0], f"{save_path}.parquet")

    for file in dbutils.fs.ls(save_path):
        dbutils.fs.rm(file.path)
    
    dbutils.fs.rm(save_path)
    print(f"Saved '{table.tableName}.parquet' to {bucket}!")

# COMMAND ----------

for table in tables.collect():
    export_parquet(bucket, database, table)
    print("All data exported!")
