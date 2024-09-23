# Databricks notebook source
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# COMMAND ----------

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

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  DISTINCT 
# MAGIC         to_date(concat_ws('-', year, month, day)) as date,
# MAGIC         COUNT(*) count_rows
# MAGIC FROM brewdat_uc_saz_prod.slv_saz_sales_nap.br_stock
# MAGIC GROUP BY to_date(concat_ws('-', year, month, day))
# MAGIC ORDER BY date

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT min(data) as min_date,
# MAGIC        max(data) as min_date,
# MAGIC        count(*) as count_rows
# MAGIC FROM brewdat_uc_saz_prod.slv_saz_sales_nap.br_sales

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT data,
# MAGIC        count(*) as count_rows
# MAGIC FROM brewdat_uc_saz_prod.slv_saz_sales_nap.br_sales
# MAGIC GROUP BY data
# MAGIC ORDER BY data

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT to_date(concat_ws('-', year, month, day), 'yyyy-MM-dd') as ingestion_date
# MAGIC FROM brewdat_uc_saz_prod.slv_saz_sales_nap.br_sales
# MAGIC WHERE data = '2021-01-01'
