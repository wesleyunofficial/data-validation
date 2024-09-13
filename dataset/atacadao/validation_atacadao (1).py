# Databricks notebook source
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  data,
# MAGIC         count(*) row_count
# MAGIC FROM    brewdat_uc_saz_prod.slv_saz_sales_atacadao.br_sales
# MAGIC GROUP BY data
# MAGIC ORDER BY data

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT min(to_date(data, 'dd/MM/yyyy')) min_date, max(to_date(data, 'dd/MM/yyyy')) max_date
# MAGIC FROM  brewdat_uc_saz_prod.slv_saz_sales_atacadao.br_sales

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT
# MAGIC       to_date(data, 'dd/MM/yyyy') data
# MAGIC FROM brewdat_uc_saz_prod.slv_saz_sales_atacadao.br_sales
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

# MAGIC %sql
# MAGIC SELECT count(*) count_rows, ano_partition, mes_partition, dia_partition
# MAGIC FROM brewdat_uc_saz_prod.gld_saz_sales_atacadao.pre_sellout
# MAGIC GROUP BY ano_partition, mes_partition, dia_partition
# MAGIC ORDER BY ano_partition, mes_partition, dia_partition

# COMMAND ----------

# MAGIC %md
# MAGIC # Historico

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  min(to_date(data, 'dd/MM/yyyy')) min_date, 
# MAGIC         max(to_date(data, 'dd/MM/yyyy')) max_date, 
# MAGIC         count(*) count_rows
# MAGIC FROM brewdat_uc_saz_prod.brz_saz_sales_atacadao.br_sales
# MAGIC WHERE 1=1
# MAGIC AND to_date(data, 'dd/MM/yyyy') < '2024-07-08'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  min(to_date(data, 'dd/MM/yyyy')) min_date, 
# MAGIC         max(to_date(data, 'dd/MM/yyyy')) max_date, 
# MAGIC         count(*) count_rows
# MAGIC FROM brewdat_uc_saz_prod.slv_saz_sales_atacadao.br_sales
# MAGIC WHERE 1=1
# MAGIC AND to_date(data, 'dd/MM/yyyy') < '2024-07-08'
