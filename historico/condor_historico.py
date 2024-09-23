# Databricks notebook source
# MAGIC %md
# MAGIC # Migração do Histórico

# COMMAND ----------

from os import path
from pyspark.sql.functions import *

# COMMAND ----------

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# COMMAND ----------

# MAGIC %md
# MAGIC # HZ

# COMMAND ----------

# DBTITLE 1,Historyzone Path
layer_mnt = "/mnt/historyzone"
country = "Brazil"
hz = "Sales/Offtrade/Sellout/Condor/"

historyzone_path = path.join(layer_mnt, country, hz)
historyzone_path

# COMMAND ----------

# DBTITLE 1,Lists the contents of a directory
dbutils.fs.ls(historyzone_path)

# COMMAND ----------

# DBTITLE 1,Read Data
df_historyzone = spark.read.format("parquet").load(historyzone_path)
df_historyzone.count()

# COMMAND ----------

df_historyzone.display()

# COMMAND ----------

# DBTITLE 1,Filter Data
filter_query = "to_date('data_venda', 'dd/MM/yyyy') < '20/08/2024'"

df_historyzone =   df_historyzone.filter(filter_query)
df_historyzone.count()

# COMMAND ----------

# DBTITLE 1,Validation Data
(
  df_historyzone.agg(
    min(to_date('data', 'dd/MM/yyyy')).alias("min_date"), 
    max(to_date('data', 'dd/MM/yyyy')).alias("max_date")
).display())

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

# DBTITLE 1,Print Schema
df_historyzone.printSchema()

# COMMAND ----------

# DBTITLE 1,Write Data in Bronze Layer
# bronze_path = "abfss://bronze@brewdatsazbrzp.dfs.core.windows.net/data/saz/br/sales/atacadao/sales"
# (
#   df_historyzone
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
#   df_historyzone
#     .write
#     .format("delta")
#     .mode("append")
#     .partitionBy("year", "month", "day")
#     .option("partitionOverwriteMode", "dynamic")
#     .save(silver_path)
# )
