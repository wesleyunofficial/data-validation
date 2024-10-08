# Databricks notebook source
# MAGIC %sh pip install delta-sharing

# COMMAND ----------

import delta_sharing

profile_client = f"/dbfs/FileStore/delta_sharing/one_platform/config.share"

client = delta_sharing.SharingClient(profile=profile_client)

client.list_all_tables()

# COMMAND ----------

delta_sharing

# COMMAND ----------

share_name = "brewdat_uc_saz_scus_iris_sales_ecommerce_prod_ds"

schema_name = "gld_saz_sales_distinct_products"

table_name = "distinct_products"

# COMMAND ----------

url = profile_client + "#" + share_name + "." + schema_name + "." + table_name
url

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM DELTA.`/dbfs/FileStore/delta_sharing/one_platform/config.share#brewdat_uc_saz_scus_iris_sales_ecommerce_prod_ds.gld_saz_sales_distinct_products.distinct_products`

# COMMAND ----------

# (
#   spark.read
#     .format("deltaSharing")
#     .option("readChangeFeed", "true")
#     .option("startingVersion", 0)
#     .option("endingVersion", 10)
#     .load(url)
# )

# COMMAND ----------

df = delta_sharing.load_as_spark(url=url)

# COMMAND ----------

df.createOrReplaceTempView("view_distinct_products")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) count_rows
# MAGIC FROM view_distinct_products

# COMMAND ----------

delta_sharing.load_as_pandas(url)

# COMMAND ----------

df = spark.read.format("deltaSharing").load(url)

# COMMAND ----------

df.show()

# COMMAND ----------


