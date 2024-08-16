# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT count(*) 
# MAGIC FROM brewdat_uc_saz_prod.brz_saz_sales_trax.br_product

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) 
# MAGIC FROM brewdat_uc_saz_prod.slv_saz_sales_trax.br_product

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM brewdat_uc_saz_prod.brz_saz_sales_trax.br_product

# COMMAND ----------

spark.read.load
