# Databricks notebook source
from os import path
from pyspark.sql.functions import *

# COMMAND ----------

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE brewdat_uc_saz_prod.slv_saz_sales_nap.br_sales

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  min(data) as min_date,
# MAGIC         max(data) as max_date,
# MAGIC         count(*) as count_rows
# MAGIC FROM brewdat_uc_saz_prod.slv_saz_sales_nap.br_sales

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*), year, month, day
# MAGIC FROM brewdat_uc_saz_prod.slv_saz_sales_nap.br_sales
# MAGIC GROUP BY year, month, day

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  count(*), 
# MAGIC         year, 
# MAGIC         month, 
# MAGIC         day
# MAGIC FROM brewdat_uc_saz_prod.slv_saz_sales_nap.br_stock
# MAGIC GROUP BY year, 
# MAGIC         month, 
# MAGIC         day

# COMMAND ----------


