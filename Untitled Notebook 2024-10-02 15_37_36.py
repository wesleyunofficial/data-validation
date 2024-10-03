# Databricks notebook source
from pyspark.sql.types import StringType
from pyspark.sql.functions import col, trim, regexp_replace

# COMMAND ----------

data = [['I like        finish'], 
         ['         zombies']]



# COMMAND ----------

data

# COMMAND ----------

columns = ['words']

df = spark.createDataFrame(data, columns)

# COMMAND ----------

df.display()

# COMMAND ----------

def single_space(col):
    return trim(regexp_replace(col, " +", " "))

# COMMAND ----------

df.withColumn("clean_words", single_space("words")).display()
