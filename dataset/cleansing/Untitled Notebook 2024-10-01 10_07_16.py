# Databricks notebook source
from pyspark.sql.types import StringType

# COMMAND ----------

@udf(StringType())
def unique_word_string(text):
    ulist = []  # Cria uma lista vazia para armazenar palavras únicas
    [ulist.append(x) for x in text.split(" ") if x not in ulist]  # Itera sobre as palavras e adiciona na lista se não estiver presente
    return " ".join(ulist)  # Retorna as palavras únicas como uma string, unidas por espaços


# COMMAND ----------

texto = "gato cachorro gato pássaro cachorro"

text_ = unique_word_string(texto)

# COMMAND ----------

print(text_)

# COMMAND ----------


