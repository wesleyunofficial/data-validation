# Databricks notebook source
# MAGIC %md
# MAGIC # Carrefour

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sales

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT year, month, day, count(*)
# MAGIC FROM brewdat_uc_saz_prod.brz_br_carrefour_sales.sales
# MAGIC GROUP BY year, month, day
# MAGIC ORDER BY year, month, day

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DATA, count(*)
# MAGIC FROM brewdat_uc_saz_prod.brz_saz_sales_carrefour.br_sales
# MAGIC GROUP BY DATA
# MAGIC ORDER BY DATA

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED brewdat_uc_saz_prod.brz_saz_sales_carrefour.br_sales

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT year, month, day, count(*)
# MAGIC FROM brewdat_uc_saz_prod.slv_br_carrefour_sales.sales
# MAGIC GROUP BY year, month, day
# MAGIC ORDER BY year, month, day

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stocks

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT year, month, day, count(*)
# MAGIC FROM brewdat_uc_saz_prod.brz_saz_sales_carrefour.br_stocks
# MAGIC GROUP BY year, month, day
# MAGIC ORDER BY year, month, day

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  min(to_date(data, 'yyyy-MM-dd')) min_date,
# MAGIC         max(to_date(data, 'yyyy-MM-dd')) max_date
# MAGIC FROM brewdat_uc_saz_prod.brz_saz_sales_carrefour.br_stocks

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  DISTINCT
# MAGIC         to_date(data, 'yyyy-MM-dd') data
# MAGIC FROM brewdat_uc_saz_prod.brz_saz_sales_carrefour.br_stocks
# MAGIC ORDER BY data

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT year, month, day, count(*)
# MAGIC FROM brewdat_uc_saz_prod.slv_saz_sales_carrefour.br_stocks
# MAGIC GROUP BY year, month, day
# MAGIC ORDER BY year, month, day

# COMMAND ----------


