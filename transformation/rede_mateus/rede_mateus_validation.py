# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT  *
# MAGIC FROM  brewdat_uc_saz_prod.slv_saz_sales_rede_mateus.br_sellout
# MAGIC WHERE 1=1
# MAGIC AND DATA == '2024-06-01'
# MAGIC AND CNPJ == '3995515027286'
# MAGIC AND DESCPRODUTO == 'REFRIGERANTES OUTROS 2L'
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  *
# MAGIC FROM  brewdat_uc_saz_prod.gld_saz_sales_rede_mateus.sellout
# MAGIC WHERE 1=1
# MAGIC AND DATA == '2024-06-01'
# MAGIC AND CNPJ == '3995515027286'
# MAGIC AND DESCPRODUTO == 'REFRIGERANTES OUTROS 2L'
# MAGIC AND codigobarras == 0

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  DATA, CNPJ, DESCPRODUTO, CODIGOBARRAS, count(*)
# MAGIC FROM  brewdat_uc_saz_prod.slv_saz_sales_rede_mateus.br_sellout
# MAGIC WHERE   1=1
# MAGIC AND     to_date(DATA, "yyyy-MM-dd") >= '2024-06-01'
# MAGIC GROUP BY DATA, 
# MAGIC         CNPJ,
# MAGIC         DESCPRODUTO, 
# MAGIC         CODIGOBARRAS
# MAGIC HAVING count(*) > 1
# MAGIC ORDER BY DATA

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  DATA, count(*) contador
# MAGIC FROM brewdat_uc_saz_prod.gld_saz_sales_rede_mateus.sellout
# MAGIC WHERE to_date(DATA, 'yyyy-MM-dd') >= '2024-06-01'
# MAGIC GROUP BY DATA
# MAGIC ORDER BY DATA

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  *
# MAGIC FROM brewdat_uc_saz_prod.gld_saz_sales_rede_mateus.sellout
# MAGIC WHERE to_date(DATA, 'yyyy-MM-dd') = '2024-06-01'
# MAGIC ORDER BY CNPJ, DESCPRODUTO, QUANTIDADEVENDA

# COMMAND ----------


