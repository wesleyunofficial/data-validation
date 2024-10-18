# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from pyspark.sql.functions import col, sum

# COMMAND ----------

# Inicializar a Spark session
spark = SparkSession.builder.appName("Product Sales Calculation").getOrCreate()

# Definindo o schema do DataFrame 'products'
products_schema = StructType([
    StructField("ProductID", IntegerType(), True),
    StructField("ProductName", StringType(), True),
    StructField("Category", StringType(), True),
    StructField("Price", FloatType(), True)
])

# Criando dados do DataFrame 'products'
products_data = [
    (1, "Laptop", "Electronics", 1500.00),
    (2, "Smartphone", "Electronics", 800.00),
    (3, "Desk", "Furniture", 250.00),
    (4, "Chair", "Furniture", 150.00),
    (5, "Headphones", "Electronics", 100.00)
]

# Criando o DataFrame 'products'
products_df = spark.createDataFrame(products_data, schema=products_schema)

# Mostrar os DataFrames criados
products_df.show()

# COMMAND ----------

# Definindo o schema do DataFrame 'order_details'
order_details_schema = StructType([
    StructField("OrderID", IntegerType(), True),
    StructField("ProductID", IntegerType(), True),
    StructField("Quantity", IntegerType(), True),
    StructField("Discount", FloatType(), True)
])

# Criando dados do DataFrame 'order_details'
order_details_data = [
    (101, 1, 2, 0.10),
    (101, 5, 1, 0.05),
    (102, 3, 4, 0.00),
    (103, 4, 2, 0.00),
    (104, 1, 1, 0.15),
    (105, 2, 3, 0.05)
]

# Criando o DataFrame 'order_details'
order_details_df = spark.createDataFrame(order_details_data, schema=order_details_schema)

order_details_df.show()

# COMMAND ----------

from pyspark.sql.functions import col, sum

# join the dataframes producs and order_details
df_sales = (
    products_df.join(order_details_df, 'ProductID', 'inner')
)

# calcule the sales amount, discount value and total sales
df_sales = (
    df_sales
        .withColumn('SalesAmount', col('Price') * col('Quantity'))
        .withColumn('DiscountValue', col('Price') * col('Quantity') * col('Discount'))
        .withColumn('TotalSales', col('SalesAmount') - col('DiscountValue'))
    )

# group by total sales per product name and sort by total sales descending
(
    df_sales
        .groupBy(col("ProductName"))
        .agg(sum("TotalSales").alias("TotalSales"))
        .sort(col("TotalSales"), ascending=False)
        .display()
)


# COMMAND ----------

df_sales = (
    df_sales
        .withColumn('SalesAmount', col('Price') * col('Quantity'))
        .withColumn('DiscountValue', col('Price') * col('Quantity') * col('Discount'))
        .withColumn('TotalSales', col('SalesAmount') - col('DiscountValue'))
    )



# COMMAND ----------


