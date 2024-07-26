# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT  data,
# MAGIC         count(*) row_count
# MAGIC FROM    brewdat_uc_saz_prod.slv_saz_sales_atacadao.br_sales
# MAGIC GROUP BY data
# MAGIC ORDER BY data

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  data,
# MAGIC         count(*) row_count
# MAGIC FROM    brewdat_uc_saz_prod.gld_saz_sales_atacadao.sales
# MAGIC GROUP BY data
# MAGIC ORDER BY data

# COMMAND ----------


