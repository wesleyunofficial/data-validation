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

# DBTITLE 1,Migração Historico
# MAGIC %sql
# MAGIC SELECT  min(data) min_date,
# MAGIC         max(data) max_date,
# MAGIC         count(*) count_rows
# MAGIC FROM    brewdat_uc_saz_prod.brz_saz_sales_rede_mateus.br_sellout
# MAGIC WHERE   1=1
# MAGIC AND     year(data) < '2024'

# COMMAND ----------

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  min(data) min_date,
# MAGIC         max(data) max_date,
# MAGIC         count(*) count_rows
# MAGIC FROM    brewdat_uc_saz_prod.slv_saz_sales_rede_mateus.br_sellout
# MAGIC WHERE   1=1
# MAGIC AND     to_date(data,'yyyy-MM-dd') < '2024-01-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC brewdat_uc_saz_prod.brz_saz_sales_rede_mateus.br_sellout

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

# DBTITLE 1,Min e Max Data
# MAGIC %sql
# MAGIC SELECT  min(to_date(DATA, 'yyyy-MM-dd')) min_data, 
# MAGIC         max(to_date(DATA, 'yyyy-MM-dd')) max_data,
# MAGIC         count(*) count_rows
# MAGIC FROM    brewdat_uc_saz_prod.gld_saz_sales_rede_mateus.sellout sel

# COMMAND ----------

# DBTITLE 1,OnePlatform - Export for Data Validation
# MAGIC %sql
# MAGIC SELECT  DATA,
# MAGIC         count(*) COUNT_ROWS,
# MAGIC         sum(QUANTIDADEVENDA) as QUANTIDADEVENDA,
# MAGIC         sum(QUANTIDADEESTOQUE) as QUANTIDADEESTOQUE,
# MAGIC         sum(VALORVENDA) as VALORVENDA
# MAGIC FROM    brewdat_uc_saz_prod.gld_saz_sales_rede_mateus.sellout sel
# MAGIC WHERE   1=1
# MAGIC AND     DATA >= '2024-01-01'
# MAGIC AND     DATA <= '2024-08-15'
# MAGIC AND     sel.CODIGOBARRAS <> '0'
# MAGIC GROUP BY sel.data
# MAGIC ORDER BY sel.data

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE brewdat_uc_saz_prod.gld_saz_sales_rede_mateus.sellout 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  to_date(DATA, 'yyyy-MM-dd') as DATA,
# MAGIC         count(*) count_rows,
# MAGIC         sum(QUANTIDADEVENDA) as QUANTIDADEVENDA,
# MAGIC         sum(QUANTIDADEESTOQUE) as QUANTIDADEESTOQUE,
# MAGIC         sum(VALORVENDA) as VALORVENDA
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

# MAGIC %md
# MAGIC # Gold - Pré-Sellout
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC |current_date|min_date  | max_date  |count_rows|
# MAGIC |---|---|---|---|
# MAGIC |2024-08-21| 2024-06-22 |2024-08-19 |460.922   |
# MAGIC |2024-08-22| 2024-06-22 |2024-08-20 |468.080   |
# MAGIC |2024-08-23|2024-06-22  |2024-08-21 |475.686   |
# MAGIC

# COMMAND ----------

# DBTITLE 1,Pré-Sellout
# MAGIC %sql
# MAGIC SELECT  min(data) min_date, 
# MAGIC         max(data) max_date, 
# MAGIC         count(*) count_rows
# MAGIC FROM  brewdat_uc_saz_prod.gld_saz_sales_rede_mateus.pre_sellout
# MAGIC WHERE   1=1
# MAGIC AND     PRODUCT_SOURCE_CODE <> '0'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  DATA, count(*) COUNT_ROWS
# MAGIC FROM    brewdat_uc_saz_prod.gld_saz_sales_rede_mateus.pre_sellout
# MAGIC WHERE   1=1
# MAGIC --AND     to_date(DATA, 'yyyy-MM-dd') >= '2024-06-22'
# MAGIC --AND     to_date(DATA, 'yyyy-MM-dd') <= '2024-08-19'
# MAGIC AND     PRODUCT_SOURCE_CODE <> '0'
# MAGIC GROUP BY DATA
# MAGIC ORDER BY DATA

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  DISTINCT *
# MAGIC FROM  brewdat_uc_saz_prod.gld_saz_sales_rede_mateus.pre_sellout
# MAGIC WHERE 1=1
# MAGIC AND   DATA = '2024-08-01'
# MAGIC AND   PRODUCT_SOURCE_CODE <> '0'

# COMMAND ----------

from datetime import date, timedelta

start_date = date.today() - timedelta(days=60)
end_date = date.today()

print(f"start_date: {start_date} | end_date: {end_date}")

# COMMAND ----------


