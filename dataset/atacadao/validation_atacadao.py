# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT  data,
# MAGIC         count(*) row_count
# MAGIC FROM    brewdat_uc_saz_prod.slv_saz_sales_atacadao.br_sales
# MAGIC GROUP BY data
# MAGIC ORDER BY data

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  to_date(data, 'dd/MM/yyyy') data,
# MAGIC         count(*) row_count
# MAGIC FROM    brewdat_uc_saz_prod.gld_saz_sales_atacadao.sales
# MAGIC GROUP BY to_date(data, 'dd/MM/yyyy')
# MAGIC ORDER BY data

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DATA, 
# MAGIC        count(*) as COUNT_ROWS, 
# MAGIC        sum(SALES_VOLUME_SELLOUT) as SALES_VOLUME_SELLOUT,
# MAGIC        sum(REVENUE_SELLOUT) as REVENUE_SELLOUT,
# MAGIC        sum(SALES_VOLUME_QUANTITY) as SALES_VOLUME_QUANTITY,
# MAGIC        sum(QUANTITY_STOCK_UNIT) as QUANTITY_STOCK_UNIT
# MAGIC FROM   brewdat_uc_saz_prod.gld_saz_sales_atacadao.pre_sellout
# MAGIC GROUP BY DATA
# MAGIC ORDER BY DATA

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE EXTENDED brewdat_uc_saz_prod.gld_saz_sales_atacadao.pre_sellout

# COMMAND ----------


