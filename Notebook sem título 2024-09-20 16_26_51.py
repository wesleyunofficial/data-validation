# Databricks notebook source
prices = [300, 1000, 450, 150]

taxes = list(map(lambda x: x * 0.08, prices))

# COMMAND ----------

print(taxes)

# COMMAND ----------


