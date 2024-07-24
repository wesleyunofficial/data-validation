# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM brewdat_uc_saz_prod.brz_br_carrefour_stocks.stocks

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM brewdat_uc_saz_prod.brz_br_carrefour_sales.sales

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM brewdat_uc_saz_prod.brz_br_carrefour_sales.stocks

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  *
# MAGIC FROM  brewdat_uc_saz_dev.brz_br_brewdat_neogrid.neogrid

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM  brewdat_uc_saz_dev.brz_br_brewdat_scanntech.scanntech

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM    brewdat_uc_saz_dev.brz_br_brewdat_scanntech.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Atacadão

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  count(*)
# MAGIC FROM    brewdat_uc_saz_dev.brz_br_brewdat_atacadao.atacadao

# COMMAND ----------


