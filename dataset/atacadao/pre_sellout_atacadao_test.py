# Databricks notebook source
# Importação das bibliotecas no contexto do notebook:
from datetime import datetime, timedelta, date
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType, DecimalType, LongType, DateType
import time

# COMMAND ----------

df_atacadao =  spark.read.table('brewdat_uc_saz_prod.gld_saz_sales_atacadao.sales')

# COMMAND ----------

df_atacadao_ajuste = df_atacadao.withColumn('CXA', split(df_atacadao['MULT'], ' X ').getItem(0)) \
                                .withColumn('PACK', split(df_atacadao['MULT'], ' X ').getItem(1))

# COMMAND ----------

# Aplica as transformações e padronizações no dataframe
df_atacadao_select = df_atacadao_ajuste.select(coalesce(trim(df_atacadao_ajuste.CNPJ),lit('0')).cast(LongType()).alias('CNPJ_PDV'),
                                               to_date(coalesce(to_date((df_atacadao_ajuste.DATA),'dd/MM/yyyy'),lit('19000101')),'yyyy-MM-dd').alias('DATA'),
                                               translate(coalesce(df_atacadao_ajuste.QTDE_VENDA_UNIDADE, lit('0')), ',','.').cast(DecimalType(38,6)).alias('SALES_VOLUME_SELLOUT'),
                                               translate(coalesce(df_atacadao_ajuste.QTDE_VENDA_UNIDADE, lit('0')), ',','.').cast(DecimalType(38,6)).alias('SALES_VOLUME_QUANTITY'),
                                               lit('0').cast(DecimalType(38,6)).alias('REVENUE_SELLOUT'),
                                               lit('NAO INFORMADO').alias('CATEGORY'),
                                               upper(coalesce(df_atacadao_ajuste.DESCRICAO, lit('NAO INFORMADO'))).alias('PRODUCT_SOURCE_NAME'),
                                               lit('NAO INFORMADO').alias('BRAND'),
                                               coalesce(df_atacadao_ajuste.CODIGO_BARRA.cast(LongType()), lit(0)).alias('PRODUCT_SOURCE_CODE'),
                                               coalesce(col('CODIGO_BARRA'), lit('0')).cast(LongType()).alias('EAN_SOURCE_CODE'),
                                               lit('NAO INFORMADO').alias('PDV_COMPANY_NAME'),
                                               lit('NAO INFORMADO').alias('PDV_TYPE'),
                                               lit('NAO INFORMADO').alias('ID_PDV_ECOMMERCE'), 
                                               translate(coalesce(df_atacadao_ajuste.ESTOQUE_BRUTO, lit('0')), ',','.').cast(DecimalType(38,6)).alias('QUANTITY_STOCK_UNIT'),
                                               df_atacadao_ajuste.CXA.cast(DecimalType(38,6)),
                                               df_atacadao_ajuste.PACK.cast(DecimalType(38,6)))

df_atacadao_volume = df_atacadao_select.withColumn('SALES_VOLUME_SELLOUT', df_atacadao_select.SALES_VOLUME_SELLOUT/(df_atacadao_select.CXA*df_atacadao_select.PACK))
df_atacadao_volume = df_atacadao_volume.withColumn('QUANTITY_STOCK_UNIT', df_atacadao_volume.QUANTITY_STOCK_UNIT/(df_atacadao_volume.CXA*df_atacadao_volume.PACK))

# COMMAND ----------

df_atacadao_volume = df_atacadao_volume.filter('DATA is not null')

# COMMAND ----------

# v_data_inicio = '2024-06-01'
# v_data_fim    = '2024-08-07'
# v_option_mode = "static"

# if v_data_inicio.replace(' ',''):
#   v_option_mode = "dynamic"
#   df_atacadao_volume = df_atacadao_volume.filter(col('DATA').between(v_data_inicio,v_data_fim))

# COMMAND ----------

# Aplica a agregação dos valores de vendas
df_atacadao_group = df_atacadao_volume.groupBy("CATEGORY","BRAND","PRODUCT_SOURCE_CODE", "EAN_SOURCE_CODE","PRODUCT_SOURCE_NAME","DATA","PDV_COMPANY_NAME","CNPJ_PDV","PDV_TYPE","ID_PDV_ECOMMERCE","CXA","PACK").agg(sum("SALES_VOLUME_SELLOUT").alias('SALES_VOLUME_SELLOUT'),sum("REVENUE_SELLOUT").alias('REVENUE_SELLOUT'),sum("SALES_VOLUME_QUANTITY").alias('SALES_VOLUME_QUANTITY'),sum("QUANTITY_STOCK_UNIT").alias('QUANTITY_STOCK_UNIT'))

# COMMAND ----------


