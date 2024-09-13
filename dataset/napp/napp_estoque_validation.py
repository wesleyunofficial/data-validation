# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM brewdat_uc_saz_prod.brz_saz_sales_nap.br_stock

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  DISTINCT 
# MAGIC         to_date(concat_ws('-', year, month, day)) as date,
# MAGIC         COUNT(*) count_rows
# MAGIC FROM brewdat_uc_saz_prod.brz_saz_sales_nap.br_stock
# MAGIC GROUP BY to_date(concat_ws('-', year, month, day))
# MAGIC ORDER BY date

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver
