# Databricks notebook source
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  to_date(min(concat_ws("-", year, month, day)), "yyyy-MM-dd") as min_date,
# MAGIC         to_date(max(concat_ws("-", year, month, day)), "yyyy-MM-dd") as max_date,
# MAGIC         count(*) count_rows
# MAGIC FROM brewdat_uc_saz_prod.brz_saz_sales_scanntech.br_diafull_vta
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  min(to_date(concat_ws("-", year, month, day), "yyyy-MM-dd")) as min_date,
# MAGIC         max(to_date(concat_ws("-", year, month, day), "yyyy-MM-dd")) as max_date,
# MAGIC         count(*) count_rows
# MAGIC FROM brewdat_uc_saz_prod.brz_saz_sales_scanntech.br_diafull_vta

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT year, month, day, 
# MAGIC         to_date(concat_ws("-", year, month, day), "yyyy-MM-dd") as ingestion_date
# MAGIC FROM brewdat_uc_saz_prod.slv_saz_sales_scanntech.br_diafull_vta

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT year, month, day, 
# MAGIC         to_date(concat_ws("-", year, month, day), "yyyy-MM-dd") as ingestion_date
# MAGIC FROM brewdat_uc_saz_prod.slv_saz_sales_scanntech.br_diafull_pdv

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED brewdat_uc_saz_prod.slv_saz_sales_scanntech.br_diafull_prd

# COMMAND ----------


