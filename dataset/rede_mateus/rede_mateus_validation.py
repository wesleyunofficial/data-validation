# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  *
# MAGIC FROM  brewdat_uc_saz_prod.brz_saz_sales_rede_mateus.br_sellout
# MAGIC WHERE 1=1
# MAGIC AND DATA == '2024-06-01'
# MAGIC AND CNPJ == '3995515027286'
# MAGIC AND DESCPRODUTO == 'REFRIGERANTES OUTROS 2L'

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  descproduto, codigobarras
# MAGIC FROM  brewdat_uc_saz_prod.slv_saz_sales_rede_mateus.br_sellout
# MAGIC WHERE 1=1
# MAGIC AND DATA == '2024-06-01'
# MAGIC AND CNPJ == '3995515027286'
# MAGIC -- AND DESCPRODUTO == 'REFRIGERANTES OUTROS 2L'
# MAGIC AND codigobarras != '0'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  *
# MAGIC FROM  brewdat_uc_saz_prod.slv_saz_sales_rede_mateus.br_sellout
# MAGIC WHERE 1=1
# MAGIC AND DATA == '2024-06-01'
# MAGIC AND CNPJ == '3995515027286'
# MAGIC AND DESCPRODUTO == 'REFRIGERANTES OUTROS 2L'

# COMMAND ----------

df_filtered = spark.sql("""
          SELECT  *
FROM  brewdat_uc_saz_prod.slv_saz_sales_rede_mateus.br_sellout
WHERE 1=1
AND DATA == '2024-06-01'
AND CNPJ == '3995515027286'
AND DESCPRODUTO == 'REFRIGERANTES OUTROS 2L'          
          """)

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql.functions import *

window = Window.partitionBy(["CNPJ", "CODIGOBARRAS", "DESCPRODUTO", "DATA"]).orderBy(col("DATA").desc())

df_filtered = df_filtered.withColumn("row_number", row_number().over(window))

df_filtered.where("row_number == 1").display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  to_date(DATA, 'yyyy-MM-dd') as DATA,
# MAGIC         count(*) count_rows,
# MAGIC         sum(QUANTIDADEVENDA) as QUANTIDADEVENDA,
# MAGIC         sum(QUANTIDADEESTOQUE) as QUANTIDADEESTOQUE
# MAGIC FROM  brewdat_uc_saz_prod.slv_saz_sales_rede_mateus.br_sellout
# MAGIC WHERE 1=1
# MAGIC AND DATA == '2024-06-01'
# MAGIC -- AND CNPJ == '3995515027286'
# MAGIC --AND DESCPRODUTO == 'REFRIGERANTES OUTROS 2L'
# MAGIC AND codigobarras <> 0
# MAGIC GROUP BY to_date(DATA, 'yyyy-MM-dd')
# MAGIC ORDER BY to_date(DATA, 'yyyy-MM-dd')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  to_date(DATA, 'yyyy-MM-dd') as DATA,
# MAGIC         count(*) count_rows,
# MAGIC         sum(QUANTIDADEVENDA) as QUANTIDADEVENDA,
# MAGIC         sum(QUANTIDADEESTOQUE) as QUANTIDADEESTOQUE
# MAGIC FROM  brewdat_uc_saz_prod.gld_saz_sales_rede_mateus.sellout
# MAGIC WHERE 1=1
# MAGIC AND DATA >= '2024-06-01'
# MAGIC --AND CNPJ == '3995515027286'
# MAGIC --AND DESCPRODUTO == 'REFRIGERANTES OUTROS 2L'
# MAGIC AND codigobarras <> '0'
# MAGIC GROUP BY to_date(DATA, 'yyyy-MM-dd')
# MAGIC ORDER BY to_date(DATA, 'yyyy-MM-dd')

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
# MAGIC SELECT  COUNT(*)
# MAGIC FROM brewdat_uc_saz_prod.gld_saz_sales_rede_mateus.sellout
# MAGIC WHERE to_date(DATA, 'yyyy-MM-dd') = '2024-06-01'
# MAGIC AND CNPJ == '3995515027286'
# MAGIC -- AND DESCPRODUTO == 'REFRIGERANTES OUTROS 2L'
# MAGIC AND CODIGOBARRAS <> '0'
# MAGIC -- ORDER BY CNPJ, DESCPRODUTO, QUANTIDADEVENDA

# COMMAND ----------


