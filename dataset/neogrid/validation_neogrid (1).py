# Databricks notebook source
# MAGIC %md
# MAGIC # Silver

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT year, month, day, source_file,  count(*) count_rows
# MAGIC FROM  brewdat_uc_saz_prod.slv_saz_sales_neogrid.br_sellout slv
# MAGIC GROUP BY year, month, day, source_file
# MAGIC ORDER BY year, month, day, source_file
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  dia, 
# MAGIC         sel.codigo_varejo,
# MAGIC         count(*) as count_rows,
# MAGIC         sum(cast(regexp_replace(coalesce(sel.valor_de_venda, 0), ',', '.') as DECIMAL(36, 6))) as valor_de_venda
# MAGIC FROM    brewdat_uc_saz_prod.slv_saz_sales_neogrid.br_sellout sel
# MAGIC GROUP BY dia, sel.codigo_varejo
# MAGIC ORDER BY dia, sel.codigo_varejo
# MAGIC
# MAGIC --regexp_replace(coalesce(df_neogrid.QUANTIDADE_VENDA_UNIDADE,lit('0')),',','.').cast(DecimalType(38,6)).alias('SALES_VOLUME_SELLOUT')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT dia, sel.codigo_varejo FROM brewdat_uc_saz_prod.slv_saz_sales_neogrid.br_sellout sel

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  dia, 
# MAGIC         cast(regexp_replace(coalesce(sel.valor_de_venda, 0), ',', '.') as DECIMAL(36, 6)) valor_de_venda
# MAGIC FROM    brewdat_uc_saz_prod.slv_saz_sales_neogrid.br_sellout sel
# MAGIC LIMIT 100

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DIA,
# MAGIC        count(*) count_rows,
# MAGIC        sum(sel.valor_de_venda) as valor_de_venda
# MAGIC FROM  brewdat_uc_saz_prod.gld_saz_sales_neogrid.sellout sel
# MAGIC GROUP BY DIA
# MAGIC ORDER BY DIA

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE brewdat_uc_saz_prod.gld_saz_sales_neogrid.sellout

# COMMAND ----------

# MAGIC %md 
# MAGIC # Pré-Sellout
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM brewdat_uc_saz_prod.gld_saz_sales_neogrid.pre_sellout
