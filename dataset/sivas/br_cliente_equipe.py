# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT count(*) count_rows, year, month, day
# MAGIC FROM  brewdat_uc_saz_prod.brz_saz_sales_sivas.br_cliente_equipe
# MAGIC GROUP BY year, month, day

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE EXTENDED brewdat_uc_saz_prod.brz_saz_sales_sivas.br_cliente_equipe

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*)
# MAGIC FROM  brewdat_uc_saz_prod.slv_saz_sales_sivas.br_cliente_equipe

# COMMAND ----------

df = spark.sql("""
               SELECT * FROM  brewdat_uc_saz_prod.slv_saz_sales_sivas.br_cliente_equipe
               """)




# COMMAND ----------

df = df.dropDuplicates()

# COMMAND ----------

df.createOrReplaceTempView("br_cliente_equipe")

# COMMAND ----------

spark.sql("""
SELECT cod_cliente, 
       cod_dv_cliente, 
       cod_equipe, 
       cod_usu_inc, 
       dat_inc, 
       cod_usu_alt, 
       dat_alt, 
       dat_ini_vigencia, 
       dat_fim_vigencia, 
       ind_loja_as_rota,
       count(*)
FROM br_cliente_equipe
GROUP BY cod_cliente, 
       cod_dv_cliente, 
       cod_equipe, 
       cod_usu_inc, 
       dat_inc, 
       cod_usu_alt, 
       dat_alt, 
       dat_ini_vigencia, 
       dat_fim_vigencia, 
       ind_loja_as_rota
HAVING count(*) > 1
""").display()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT * except(year, month, day)
# MAGIC FROM br_cliente_equipe
# MAGIC WHERE cod_cliente = '16856831'
# MAGIC AND cod_equipe = '127725'

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE EXTENDED brewdat_uc_saz_prod.slv_saz_sales_sivas.br_cliente_equipe

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE EXTENDED brewdat_uc_saz_prod.brz_saz_sales_sivas.br_cliente_equipe

# COMMAND ----------

Afrom delta.tables import DeltaTable

# COMMAND ----------

# Create a DeltaTable object
delta_table = DeltaTable.forPath(spark, "abfss://bronze@brewdatsazbrzp.dfs.core.windows.net/Br/sales/sivas/cliente_equipe")

# COMMAND ----------

# Get the metadata of the Delta Table
metadata = delta_table.detail().select("partitionColumns").collect()

# Display partition columns
partition_columns = metadata[0]['partitionColumns']
print(partition_columns)

# COMMAND ----------


