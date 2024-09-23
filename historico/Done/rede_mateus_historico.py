# Databricks notebook source
from os import path
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC # PZ

# COMMAND ----------

# DBTITLE 1,Source Path
pz_path = "/mnt/prelandingzone/Brazil/DirectSellout/RedeMateus"

dbutils.fs.ls(pz_path)

# COMMAND ----------

# DBTITLE 1,Read Data
rede_mateus_2021_pz_path = path.join(pz_path, "2021**********")
rede_mateus_2022_pz_path = path.join(pz_path, "2022**********")
rede_mateus_2023_pz_path = path.join(pz_path, "2023**********")
rede_mateus_2024_pz_path = path.join(pz_path, "2024**********")

# COMMAND ----------

df_pz_2021 = spark.read.format("avro").option("mergeSchema", "true").load(rede_mateus_2021_pz_path)
df_pz_2022 = spark.read.format("avro").option("mergeSchema", "true").load(rede_mateus_2022_pz_path)
df_pz_2023 = spark.read.format("avro").option("mergeSchema", "true").load(rede_mateus_2023_pz_path)
df_pz_2024 = spark.read.format("avro").option("mergeSchema", "true").load(rede_mateus_2024_pz_path).where(year(col("DATA")) == 2023)

# COMMAND ----------

# DBTITLE 1,Union All Dataframes
df_rede_mateus = df_pz_2021.union(df_pz_2022).union(df_pz_2023).union(df_pz_2024)

# COMMAND ----------

# DBTITLE 1,Drop Column: filename
df_rede_mateus = df_rede_mateus.drop(col("filename"))

# COMMAND ----------

# DBTITLE 1,Drop Duplicates
df_rede_mateus = df_rede_mateus.dropDuplicates()

# COMMAND ----------

# DBTITLE 1,Add partition columns: year, month and day
df_rede_mateus = ( 
  df_rede_mateus
    .withColumn("year", year(to_date(col("DATA"), "yyyy-MM-dd")))
    .withColumn("month", month(to_date(col("DATA"), "yyyy-MM-dd")))
    .withColumn("day", when(to_date(dayofmonth(col("DATA")) < 16, lit(1)).otherwise(lit(16)))
)

# COMMAND ----------

# DBTITLE 1,Print Schema
df_rede_mateus.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC # HZ

# COMMAND ----------

# DBTITLE 1,Source Path
historyzone_path = '/mnt/historyzone/Brazil/DirectSellout/RedeMateus'
dbutils.fs.ls(historyzone_path)

# COMMAND ----------

# DBTITLE 1,Read Data
df_hz_rede_mateus = spark.read.format("parquet").load(historyzone_path)

# COMMAND ----------

# DBTITLE 1,Filter Year <= 2023
df_hz_rede_mateus = df_hz_rede_mateus.where(year(col("DATA")) <= 2023)

# COMMAND ----------

df_hz_rede_mateus.count()

# COMMAND ----------

# DBTITLE 1,Add partition columns: year, month and day
column_name = "createdDate"

df_hz_rede_mateus = (
  df_hz_rede_mateus
    .withColumn("year", year(df_hz_rede_mateus.createdDate))
    .withColumn("month", month(df_hz_rede_mateus.createdDate))
    .withColumn("day", dayofmonth(df_hz_rede_mateus.createdDate))
)

# COMMAND ----------

df_hz_rede_mateus.display()

# COMMAND ----------

# DBTITLE 1,Drop Columns: filename, createdDate, updatedDate e lastReceived
df_hz_rede_mateus = df_hz_rede_mateus.drop('filename', 'createdDate', 'updatedDate', 'lastReceived')

# COMMAND ----------

df_hz_rede_mateus.printSchema()

# COMMAND ----------

# DBTITLE 1,Write Data in Bronze Layer
# path = "abfss://bronze@brewdatsazbrzp.dfs.core.windows.net/data/saz/br/sales/rede_mateus/sellout"
# (
#   df_rede_mateus
#     .write.format("delta")
#     .mode("overwrite")
#     .save("/mnt/delta/rede_mateus")
#     .partitionBy("year", "month", "day")
#     .option("partitionOverwriteMode", "dynamic")
# )

# COMMAND ----------

# DBTITLE 1,Write Data in Silver Layer
# silver_path = "abfss://silver@brewdatsazslvp.dfs.core.windows.net/data/saz/br/sales/rede_mateus/sellout"
# (
#   df_hz_rede_mateus
#     .write.mode("overwrite")
#     .partitionBy("year, month, day")
#     .option("partitionOverwriteMode", "dynamic")
#     .format("delta").save(silver_path)
# )
