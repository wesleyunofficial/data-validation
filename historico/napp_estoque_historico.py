# Databricks notebook source
# MAGIC %md
# MAGIC # Napp - Estoque - Migração do Histórico

# COMMAND ----------

# DBTITLE 1,Import Libs
from os import path
from pyspark.sql.functions import *

# COMMAND ----------

# DBTITLE 1,Spark Set Conf
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# COMMAND ----------

# DBTITLE 1,Historico Path
layer_mnt = "/mnt/historyzone"
country = "Brazil"
hz_path = "Sales/Offtrade/Sellout/Napp/Estoque/"

history_zone_path = path.join(layer_mnt, country, hz_path)
history_zone_path

# COMMAND ----------

# DBTITLE 1,Lists the contents of a directory
dbutils.fs.ls(history_zone_path)

# COMMAND ----------

# DBTITLE 1,Read Data
df_historico = spark.read.format("parquet").load(history_zone_path)
df_historico.count()

# COMMAND ----------

# DBTITLE 1,Filter Data
filter_query = "to_date(createdDate, 'yyyy-MM-dd') < '2024-09-19' and updatedDate == ''"

df_historico =   df_historico.filter(filter_query)
df_historico.count()

# COMMAND ----------

# DBTITLE 1,Validation Data
# (
#   df_historyzone.agg(
#     min(to_date('data', 'dd/MM/yyyy')).alias("min_date"), 
#     max(to_date('data', 'dd/MM/yyyy')).alias("max_date"),
#     count("*").alias("count_rows")
#   ).display()
# )

# COMMAND ----------

# DBTITLE 1,Add partition columns: year, month and day
column_name = col("createdDate")

df_historico = (
  df_historico
    .withColumn("year", year(column_name))
    .withColumn("month", month(column_name))
    .withColumn("day", dayofmonth(column_name))
)

# COMMAND ----------

# DBTITLE 1,Drop Columns: filename, createdDate, updatedDate e lastReceived
df_historico = df_historico.drop('DATA', 'filename', 'createdDate', 'updatedDate', 'lastReceived')

# COMMAND ----------

# DBTITLE 1,Rename Columns to Lower Case
df_historico = df_historico.toDF(
            *[c.strip().lower() for c in df_historico.columns]
        )

# COMMAND ----------

# DBTITLE 1,Print Schema
df_historico.printSchema()

# COMMAND ----------

# DBTITLE 1,Write Data in Bronze Layer
# bronze_path = "abfss://bronze@brewdatsazbrzp.dfs.core.windows.net/data/saz/br/sales/nap/stock"
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
# silver_path = "abfss://silver@brewdatsazslvp.dfs.core.windows.net/data/saz/br/sales/nap/stock"
# (
#   df_historico
#     .write
#     .format("delta")
#     .mode("append")
#     .partitionBy("year", "month", "day")
#     .option("partitionOverwriteMode", "dynamic")
#     .save(silver_path)
# )
