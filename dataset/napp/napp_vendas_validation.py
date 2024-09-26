# Databricks notebook source
from os import path
from pyspark.sql.functions import *

# COMMAND ----------

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE brewdat_uc_saz_prod.slv_saz_sales_nap.br_sales

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  min(data) as min_date,
# MAGIC         max(data) as max_date,
# MAGIC         count(*) as count_rows
# MAGIC FROM brewdat_uc_saz_prod.slv_saz_sales_nap.br_sales

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*), year, month, day
# MAGIC FROM brewdat_uc_saz_prod.slv_saz_sales_nap.br_sales
# MAGIC GROUP BY year, month, day

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  count(*), 
# MAGIC         year, 
# MAGIC         month, 
# MAGIC         day
# MAGIC FROM brewdat_uc_saz_prod.slv_saz_sales_nap.br_stock
# MAGIC GROUP BY year, 
# MAGIC         month, 
# MAGIC         day

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM  brewdat_uc_saz_prod.gld_saz_sales_distinct_products.distinct_products

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM brewdat_uc_saz_prod.gld_saz_sales_master_table.br_master_table

# COMMAND ----------

# importando a biblioteca pandas
import pandas as pd

# Criando dataframe
dados = {
    'Data': ['01/01/2024', '02/01/2024', '03/01/2024', '04/01/2024'],
    'Demanda': [100, 120, 130, 140],
    'Dias de Estoque': [3, 2, 1, 0]
}

df = pd.DataFrame(data=dados)

# Verificando o tipo do dataframe
print(type(df))

# Exibe as primeiras 5 linhas da base de dados
df.head()



# COMMAND ----------

import pyspark.pandas as ps


# Criando o mesmo dataframe, agora utilizando a biblioteca pyspark.pandas

df2 = ps.DataFrame(data=dados)


# Verificando o tipo do dataframe

print(type(df2))

# COMMAND ----------

df2.head()

# COMMAND ----------


