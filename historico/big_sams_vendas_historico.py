# Databricks notebook source
# MAGIC %md
# MAGIC # BigSams - Migração do Histórico

# COMMAND ----------

from os import path
from pyspark.sql.functions import *

# COMMAND ----------

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# COMMAND ----------

# MAGIC %md
# MAGIC # Pré-Sellout

# COMMAND ----------

# DBTITLE 1,Historyzone Path
layer_mnt = "/mnt/consumezone"
country = "Brazil"
cz_path = "Sales/Offtrade/Sellout/PreSellout/BigSams"

consumezone_path = path.join(layer_mnt, country, cz_path)
consumezone_path

# COMMAND ----------

# DBTITLE 1,Lists the contents of a directory
dbutils.fs.ls(consumezone_path)

# COMMAND ----------

# DBTITLE 1,Read Data
df_consumezone = spark.read.format("parquet").load(consumezone_path)

# COMMAND ----------

# DBTITLE 1,Validation Data
(
  df_consumezone.agg(
    min(to_date('data', 'dd/MM/yyyy')).alias("min_date"), 
    max(to_date('data', 'dd/MM/yyyy')).alias("max_date"),
    count('*').alias("count_rows")
).display())

# COMMAND ----------

# DBTITLE 1,Print Schema
df_consumezone.printSchema()

# COMMAND ----------

# DBTITLE 1,Write Data in Gold Layer
#table_name = "brewdat_uc_saz_prod.gld_saz_sales_big_sams.pre_sellout"

#gold_path = "abfss://gold@brewdatsazgldp.dfs.core.windows.net/data/saz/br/sales/sellout/big_sams/gld_saz_sales_big_sams/pre_sellout"

# (
#   df_consumezone
#     .write
#     .format("delta")
#     .mode("overwrite")
#     .option("partitionOverwriteMode", "dynamic")
#     .option("path", gold_path)
#     .partitionBy("ano_partition", "mes_partition", "dia_partition")
#     .saveAsTable(table_name)
# )

# COMMAND ----------

# DBTITLE 1,Validation after data migration
# %sql
# SELECT  min(to_date('data', 'dd/MM/yyyy')) as min_date,
#         max(to_date('data', 'dd/MM/yyyy')) as max_date,
#         count('*') as count_rows
# FROM    brewdat_uc_saz_prod.gld_saz_sales_big_sams.pre_sellout
