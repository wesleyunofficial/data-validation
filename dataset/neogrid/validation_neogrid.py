# Databricks notebook source
# MAGIC %md
# MAGIC # Silver

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT year, month, day, source_file,  count(*) count_rows
# MAGIC FROM  brewdat_uc_saz_prod.slv_saz_sales_neogrid.br_sellout slv
# MAGIC WHERE 1=1
# MAGIC --AND year = 2024 and month = 10 and day = 14
# MAGIC GROUP BY year, month, day, source_file
# MAGIC ORDER BY year, month, day, source_file
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT to_date(concat_ws('-', year, month, day)) as ingestion_date, count(*) count_rows
# MAGIC FROM  brewdat_uc_saz_prod.slv_saz_sales_neogrid.br_sellout
# MAGIC GROUP BY to_date(concat_ws('-', year, month, day))

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

# COMMAND ----------

# MAGIC %md 
# MAGIC # Neogrid (Dev)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   count(*) count_rows,
# MAGIC   (
# MAGIC     SELECT
# MAGIC       count(*) count_rows
# MAGIC     FROM
# MAGIC       brewdat_uc_saz_dev.slv_saz_sales_neogrid.br_sellout
# MAGIC   ) count_rows_total,
# MAGIC   source_file
# MAGIC FROM
# MAGIC   brewdat_uc_saz_dev.slv_saz_sales_neogrid.br_sellout
# MAGIC GROUP BY
# MAGIC   source_file

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT unb, start_date, end_date
# MAGIC FROM brewdat_uc_saz_prod.gld_saz_sales_promax.nota_fiscal_cdd_union

# COMMAND ----------

dbutils.fs.ls("abfss://gold@brewdatsazgldp.dfs.core.windows.net/data/saz/br/sales/sellout/neogrid/gld_saz_sales_neogrid/sellout/DIA=2024-06-12")

# COMMAND ----------


