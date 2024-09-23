# Databricks notebook source
# MAGIC %md
# MAGIC # Atacadão - Migração do Histórico

# COMMAND ----------

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# COMMAND ----------

from os import path
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Historyzone (HZ)

# COMMAND ----------

# DBTITLE 1,Historyzone Path
historyzone_path = "/mnt/historyzone/Brazil/Directsellout/Atacadao"

# COMMAND ----------

# DBTITLE 1,Lists the contents of a directory
dbutils.fs.ls(historyzone_path)

# COMMAND ----------

# DBTITLE 1,Read Data
df_historyzone = spark.read.format("parquet").load(historyzone_path)

# COMMAND ----------

# DBTITLE 1,Filter Data
#Data de Corte
filter_query = "to_date(data, 'dd/MM/yyyy') < '2024-07-08'"

df_historyzone =   df_historyzone.filter(filter_query)

# COMMAND ----------

(
  df_historyzone.agg(
    min(to_date('data', 'dd/MM/yyyy')).alias("min_date"),
    max(to_date('data', 'dd/MM/yyyy')).alias("max_date"),    
    count("*").alias("count_rows")
  ).display()
)

# COMMAND ----------

# DBTITLE 1,Add partition columns: year, month and day
column_name = col("createdDate")

df_historyzone = (
  df_historyzone
    .withColumn("year", year(column_name))
    .withColumn("month", month(column_name))
    .withColumn("day", dayofmonth(column_name))
)

# COMMAND ----------

# DBTITLE 1,Drop Columns: filename, createdDate, updatedDate e lastReceived
df_historyzone = df_historyzone.drop('filename', 'createdDate', 'updatedDate', 'lastReceived')

# COMMAND ----------

df_historyzone = df_historyzone.toDF(
            *[c.strip().lower() for c in df_historyzone.columns]
        )

# COMMAND ----------

# DBTITLE 1,Print Schema
df_historyzone.printSchema()

# COMMAND ----------

# DBTITLE 1,Write Data in Bronze Layer
# bronze_path = "abfss://bronze@brewdatsazbrzp.dfs.core.windows.net/data/saz/br/sales/atacadao/sales"
# (
#   df_rede_mateus
#     .write
#     .format("delta")
#     .mode("append")
#     .partitionBy("year", "month", "day")
#     .option("partitionOverwriteMode", "dynamic")
#     .save(bronze_path)
# )

# COMMAND ----------

# DBTITLE 1,Write Data in Silver Layer
# silver_path = "abfss://silver@brewdatsazslvp.dfs.core.windows.net/data/saz/br/sales/atacadao/sales"
# (
#   df_hz_rede_mateus
#     .write
#     .format("delta")
#     .mode("append")
#     .partitionBy("year, month, day")
#     .option("partitionOverwriteMode", "dynamic")
#     .save(silver_path)
# )
