# Databricks notebook source
# MAGIC %md
# MAGIC # Silver

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*), data_venda, year, month, day 
# MAGIC FROM brewdat_uc_saz_prod.slv_saz_sales_condor.br_sellout 
# MAGIC GROUP BY data_venda, year, month, day

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  DATA_VENDA, count(*) count_rows
# MAGIC FROM brewdat_uc_saz_prod.gld_saz_sales_condor.sellout
# MAGIC WHERE   COMPANY == 'AMBEV'
# MAGIC GROUP BY DATA_VENDA
# MAGIC ORDER BY DATA_VENDA

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DATA, count(*) count_rows
# MAGIC FROM  brewdat_uc_saz_prod.gld_saz_sales_condor.pre_sellout
# MAGIC GROUP BY DATA
# MAGIC ORDER BY DATA

# COMMAND ----------

# MAGIC %md 
# MAGIC # Historico

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  min(to_date(data_venda, 'dd/MM/yyyy')) as min_date,
# MAGIC         max(to_date(data_venda, 'dd/MM/yyyy')) as max_date,
# MAGIC         count(*) count_rows
# MAGIC FROM brewdat_uc_saz_prod.brz_saz_sales_condor.br_sellout
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  DISTINCT to_date(data_venda, 'dd/MM/yyyy') data_venda
# MAGIC FROM    brewdat_uc_saz_prod.brz_saz_sales_condor.br_sellout
# MAGIC ORDER BY data_venda

# COMMAND ----------


