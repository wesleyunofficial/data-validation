# Databricks notebook source
# MAGIC %md
# MAGIC # Dunnhumby (Pré-Sellout) - Migração do Histórico

# COMMAND ----------

from os import path
from pyspark.sql.functions import *

# COMMAND ----------

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# COMMAND ----------

# MAGIC %md
# MAGIC # CZ

# COMMAND ----------

# DBTITLE 1,Historyzone Path
layer_mnt = "/mnt/consumezone"
country = "Brazil"
cz = "Sales/Offtrade/Sellout/PreSellout/Dunnhumby"

consume_zone_path = path.join(layer_mnt, country, cz)
consume_zone_path

# COMMAND ----------

# DBTITLE 1,Lists the contents of a directory
dbutils.fs.ls(consume_zone_path)

# COMMAND ----------

# DBTITLE 1,Read Data
df_consume_zone = spark.read.format("parquet").load(consume_zone_path)

# COMMAND ----------

# DBTITLE 1,Validation Data
(
  df_consume_zone.agg(
    min(to_date('data', 'dd/MM/yyyy')).alias("min_date"), 
    max(to_date('data', 'dd/MM/yyyy')).alias("max_date"),
    count('*').alias("count_rows")
).display())

# COMMAND ----------

# DBTITLE 1,Print Schema
df_consume_zone.printSchema()

# COMMAND ----------

# DBTITLE 1,Write Data in Gold Layer
#table_name = brewdat_uc_saz_prod.gld_saz_sales_dunnhumby.pre_sellout

# gold_path = "abfss://gold@brewdatsazgldp.dfs.core.windows.net/data/saz/br/sales/sellout/dunnhumby/gld_saz_sales_dunnhumby/pre_sellout"

# (
#   df_consume_zone
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
# FROM    brewdat_uc_saz_prod.gld_saz_sales_dunnhumby.pre_sellout
