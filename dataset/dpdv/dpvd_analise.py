# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT  year, 
# MAGIC         month, 
# MAGIC         day, 
# MAGIC         count(*) count_rows
# MAGIC FROM    brewdat_uc_saz_prod.slv_saz_sales_input_manual.br_dpdv_direta
# MAGIC GROUP BY  year, month, day

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  year, 
# MAGIC         month, 
# MAGIC         day, 
# MAGIC         count(*) count_rows
# MAGIC FROM    brewdat_uc_saz_prod.slv_saz_sales_input_manual.br_dpdv_lojas_alvo
# MAGIC GROUP BY  year, month, day

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  year, 
# MAGIC         month, 
# MAGIC         day, 
# MAGIC         count(*) count_rows
# MAGIC FROM    brewdat_uc_saz_prod.slv_saz_sales_input_manual.br_dpdv_vizinhanca
# MAGIC GROUP BY  year, month, day

# COMMAND ----------


