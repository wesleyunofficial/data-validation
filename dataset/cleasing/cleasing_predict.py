# Databricks notebook source
# Databricks notebook source
import json

# from engineeringstore.core.transformation.task.task_entrypoint import TaskEntryPoint
# from engineeringstore.core.transformation.transformation import Transformation

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.types import *
from pyspark.sql.types import StringType
from pyspark.sql.functions import (col, concat, explode, lit, pandas_udf)

import difflib

from pyspark.sql import SparkSession
import pandas as pd

from pyiris.ingestion.extract import FileReader
from pyiris.infrastructure import Spark as pyiris_spark
from pyiris.ingestion.config.file_system_config import FileSystemConfig
from pyiris.ingestion.load import FileWriter

spark.conf.set('spark.sql.execution.arrow.enabled', 'false')

# Main class for your transformation
class CleasingPredict(): #Transformation
    def __init__(self):
        super().__init__(
            dependencies = [
            ]
        )

    # This method is mandatory and the final transformation of your dataframe must be returned here
    def definitions(self):
        # Apply your transformation and return your final dataframe
        df_example = self.get_table("my_example")
        return df_example

# COMMAND ----------

# MAGIC %md
# MAGIC # Functions

# COMMAND ----------

# DBTITLE 1,Drop Columns
def drop_columns(df, list_columns):
  return df.drop(*list_columns)

# COMMAND ----------

# MAGIC %md
# MAGIC # MasterTable

# COMMAND ----------

# DBTITLE 1,Process MasterTable
def process_master_table():

  # table_name = ""
  # df_master_table = self.get_table(table_name)

  # read
  master_table_path = "SellOut/Manual/MasterTable2SellOutOffTrade"
  # Master original
  
  dl_reader = FileReader(
    table_id = 'masteroriginal',
    format = 'parquet',
    mount_name = 'consumezone',
    country = 'Brazil',
    path = master_table_path
  )  
  
  df_master_table = dl_reader.consume(spark = pyiris_spark())

  # transform

  # drop unnecessary columns
  df_master_table = drop_columns(df_master_table, ['createdDate', 'updatedDate', 'lastReceived', 'ID', 'FRIENDLY_NAME'])

  # rename column source name
  df_master_table = df_master_table.withColumnRenamed('PRODUCT_SOURCE_NAME', 
                                                      'PRODUCT_SOURCE_NAME_MASTER')
  
  # explode column EAN
  df_master_table_explode = df_master_table.select(explode(col('EAN')).alias('EAN_EX'), '*')

  return df_master_table_explode

# COMMAND ----------

# DBTITLE 1,Test Master Table
df_master_table = process_master_table()

df_master_table.count()

# COMMAND ----------

# MAGIC %md 
# MAGIC # MasterTable Generic

# COMMAND ----------

def process_master_table_generic():

  # table_name = ""  
  # df_master_table_generic = self.get_table(table_name)

  master_table_generic_path =  'SellOut/Manual/MasterTable2GenericSellOutOffTrade'
  
  # Master generic
  dl_reader = FileReader(
    table_id = 'mastergeneric',
    format = 'parquet',
    mount_name = 'consumezone',
    country = 'Brazil',
    path = master_table_generic_path
  )

  df_master_table_generic = dl_reader.consume(spark = pyiris_spark())

  schema_sellin = schema = StructType([
    StructField("COD_PROD", IntegerType(), True),
    StructField("COD_ABREV_PROD", IntegerType(), True),
    StructField("NOM_PROD", StringType(), False),
    StructField("CESTA_OFICIAL", IntegerType(), False),
    StructField("EAN_PACK", LongType(), False),
    StructField("EAN_UNIT", LongType(), False),
    StructField("INNOVATION", StringType(), False),
    StructField("FUTURE_BEVS", StringType(), False),
    StructField("COD_PROD_GERENCIAL_VENDAS", IntegerType(), False),
    StructField("NOM_PROD_GERENCIAL_VENDAS", StringType(), False),
  ])

  # drop unnecessary columns
  df_master_table_generic = drop_columns(df_master_table_generic, ['createdDate', 'updatedDate', 'lastReceived', 'ID', 'FRIENDLY_NAME'])

  # add columns
  df_master_table_generic = (
    df_master_table_generic
      .withColumn('AUTO_FILL_SELLIN', lit(None))
      .withColumn('MATCH_INTERNAL_PRODUCT', lit(None).cast(ArrayType(schema_sellin)))
    )
  
  # rename column from PRODUCT_SOURCE_NAME to PRODUCT_SOURCE_NAME_MASTER
  df_master_table_generic = df_master_table_generic.withColumnRenamed('PRODUCT_SOURCE_NAME', 
                                                                      'PRODUCT_SOURCE_NAME_MASTER')
  
  # explode column EAN
  df_master_table_generic_explode = df_master_table_generic.select(explode(col('EAN')).alias('EAN_EX'), '*')

  return df_master_table_generic_explode

# COMMAND ----------

# DBTITLE 1,Test MasterTable Generic
df_master_table_generic = process_master_table_generic()

df_master_table_generic.count()

# COMMAND ----------

# MAGIC %md
# MAGIC # Distinct Products

# COMMAND ----------

def process_distinct_products():
  
  # table_name = ""  
  # df_distinct_products = self.get_table(table_name)

  distinct_products_path = 'Sales/Offtrade/Sellout/DistinctProducts'

  dl_reader = FileReader(
    table_id = 'distinctoriginal',
    format = 'parquet',
    mount_name = 'consumezone',
    country = 'Brazil',
    path = distinct_products_path
  )

  df_distinct_products = dl_reader.consume(spark = pyiris_spark())

  return df_distinct_products


# COMMAND ----------

df_distinct_products = process_distinct_products()

df_distinct_products.count()

# COMMAND ----------

# MAGIC %md
# MAGIC # Distinct Treated

# COMMAND ----------

def process_distinct_treated():

  # table_name = ""  
  # df_distinct_products = self.get_table(table_name)

  distinct_treated_path = 'Sales/Offtrade/Cleansing2/WorkData/Origins'

  # Distinct tratada
  dl_reader = FileReader(
    table_id = 'distinct',
    format = 'parquet',
    mount_name = 'consumezone',
    country = 'Brazil',
    path = distinct_treated_path
  )

  df_distinct_treated = dl_reader.consume(spark = pyiris_spark())

  return df_distinct_treated

# COMMAND ----------

df_distinct_treated = process_distinct_treated()

df_distinct_treated.count()

# COMMAND ----------

# MAGIC %md
# MAGIC # MasterTreated

# COMMAND ----------

def process_master_treated():

  # table_name = ""  
  # df_master_treated = self.get_table(table_name)

  master_treated_path = 'Sales/Offtrade/Cleansing2/WorkData/MasterTreated'

  # Master tratada
  dl_reader = FileReader(
    table_id = 'master',
    format = 'parquet',
    mount_name = 'consumezone',
    country = 'Brazil',
    path = master_treated_path
  )

  df_master_treated = dl_reader.consume(spark = pyiris_spark())

  return df_master_treated

# COMMAND ----------

df_master_treated = process_master_treated()

df_master_treated.count()

# COMMAND ----------


