# Databricks notebook source
# MAGIC %md
# MAGIC # Migração do Histórico

# COMMAND ----------

from os import path
from pyspark.sql.functions import *

# COMMAND ----------

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# COMMAND ----------

# DBTITLE 1,Historico Path
layer_mnt = "/mnt/consumezone"
country = "Brazil"
path_name = "Sales/Offtrade/Sellout/Napp/Pdv"

historico_path = path.join(layer_mnt, country, path_name)
historico_path

# COMMAND ----------

# DBTITLE 1,Lists the contents of a directory
dbutils.fs.ls(historico_path)

# COMMAND ----------

# DBTITLE 1,Read Data
df_historico = spark.read.format("parquet").load(historico_path)

# COMMAND ----------

df_historico.display()

# COMMAND ----------

# DBTITLE 1,Filter Data
filter_query = ""

df_historico = df_historico.filter(filter_query)

# COMMAND ----------

# DBTITLE 1,Validation Data
(
  df_historico.agg(
    min(to_date('data', 'dd/MM/yyyy')).alias("min_date"), 
    max(to_date('data', 'dd/MM/yyyy')).alias("max_date"),
    count('*').alias("count_rows")
).display())

# COMMAND ----------

# DBTITLE 1,Add partition columns: year, month and day
column_name = col("REPLACE_COLUMN_NAME")

df_historico = (
  df_historico
    .withColumn("year", year(column_name))
    .withColumn("month", month(column_name))
    .withColumn("day", dayofmonth(column_name))
)

# COMMAND ----------

# DBTITLE 1,Drop Columns: filename, createdDate, updatedDate e lastReceived
df_historico = df_historico.drop('filename', 'createdDate', 'updatedDate', 'lastReceived')

# COMMAND ----------

# DBTITLE 1,Print Schema
df_historico.printSchema()

# COMMAND ----------

# DBTITLE 1,Write Data in Bronze Layer
# bronze_path = "abfss://bronze@brewdatsazbrzp.dfs.core.windows.net/data/saz/br/sales/atacadao/sales"
# (
#   df_historico
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
#   df_historico
#     .write
#     .format("delta")
#     .mode("append")
#     .partitionBy("year", "month", "day")
#     .option("partitionOverwriteMode", "dynamic")
#     .save(silver_path)
# )
