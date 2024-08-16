# Databricks notebook source
# Databricks notebook source
import json
from datetime import timedelta, date

from engineeringstore.core.transformation.task.task_entrypoint import TaskEntryPoint
from engineeringstore.core.transformation.transformation import Transformation
from pyspark.sql import DataFrame
from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import (
    coalesce,
    col,
    dayofmonth,
    lit,
    month,
    regexp_replace,
    split,
    sum,
    to_date,
    translate,
    trim,
    upper,
    year,   
    when

)
from pyspark.sql.types import IntegerType, DecimalType, LongType


# Main class for your transformation
class PreSellout(Transformation):
    def __init__(self):
        super().__init__(
            dependencies = [
            ]
        )

    def get_delta_table_unity_catalog(self, table_name: str) -> DataFrame:
        try:
            # Attempt to read the table from the Unity Catalog
            df_rede_mateus_sellout = spark.table(table_name)

            # Check if the table has any records
            if df_rede_mateus_sellout.count() > 0:
                return df_rede_mateus_sellout
            else:
                # Raise an error if the table exists but is empty
                raise ValueError("Delta table exists, but is empty.")
                # dbutils.notebook.exit("Delta table exists, but is empty.")
        except AnalysisException as e:
            # Handle the case where the table is not found
            if "Table or view not found" in str(e):
                raise FileNotFoundError(
                    f"Delta table {table_name} was not found in the Unity Catalog."
                )
                dbutils.notebook.exit(
                    f"Delta table {table_name} was not found in the Unity Catalog."
                )
    

    # This method is mandatory and the final transformation of your dataframe must be returned here
    def definitions(self):
        # Apply your transformation and return your final dataframe
        df_example = self.get_delta_table_unity_catalog("brewdat_uc_saz_prod.gld_saz_sales_rede_mateus.sellout")
        return df_example


# COMMAND ----------

pre_sellout = PreSellout()

df_rede_mateus = pre_sellout.definitions()

# COMMAND ----------

df_rede_mateus.printSchema()

# COMMAND ----------

df_rede_mateus = (
    df_rede_mateus.withColumn(
        "ID_PDV_ECOMMERCE", 
        when(
            upper(col("VENDAVIRTUAL")) == "SIM", "S"
        ).otherwise("N")
    ).withColumn(
        "COMPANY",
        when(
            upper(df_rede_mateus.TIPO) == "CONCORRENTE", lit("OUTRO FABRICANTE")
        ).otherwise(upper(df_rede_mateus.TIPO)),
    )
)

# COMMAND ----------

df_rede_mateus_transformed = df_rede_mateus.select(
    coalesce(df_rede_mateus.CNPJ.cast(LongType()), lit(0)).alias("CNPJ_PDV"),
    to_date(coalesce(trim(regexp_replace(df_rede_mateus.DATA, "-", "")), lit("19000101")), "yyyyMMdd").alias("DATA"),
    regexp_replace(coalesce(df_rede_mateus.QUANTIDADEVENDA, lit(0)), ",", ".").cast(DecimalType(38, 6)).alias("SALES_VOLUME_SELLOUT"),
    regexp_replace(coalesce(df_rede_mateus.QUANTIDADEVENDA, lit(0)), ",", ".").cast(DecimalType(38, 6)).alias("SALES_VOLUME_QUANTITY"),
    regexp_replace(coalesce(df_rede_mateus.VALORVENDA, lit(0)), ",", ".").cast(DecimalType(38, 6)).alias("REVENUE_SELLOUT"),
    upper(coalesce(df_rede_mateus.SECAO, lit("NAO INFORMADO"))).alias("CATEGORY"),
    upper(coalesce(df_rede_mateus.DESCPRODUTO, lit("NAO INFORMADO"))).alias("PRODUCT_SOURCE_NAME"), 
    lit("NAO INFORMADO").alias("BRAND"),
    coalesce(df_rede_mateus.CODIGOBARRAS.cast(LongType()), lit(0)).alias("PRODUCT_SOURCE_CODE"),
    lit("NAO INFORMADO").alias("PDV_COMPANY_NAME"),
    lit("NAO INFORMADO").alias("PDV_TYPE"),
    col("ID_PDV_ECOMMERCE"),
    col("COMPANY"),
    regexp_replace(coalesce(df_rede_mateus.QUANTIDADEESTOQUE, lit(0)), ",", ".").cast(DecimalType(38, 6)).alias("QUANTITY_STOCK_UNIT"),
)

# COMMAND ----------

df_rede_mateus_transformed = df_rede_mateus_transformed.filter("DATA is not null")

# COMMAND ----------

# set date range last 60 day
start_date = date.today() - timedelta(days=60)
end_date = date.today()

# set first day of month
first_day_of_the_month = date.today().replace(day=1)

# if today is first day of the month
# Filter the DataFrame to include only rows where 'DATA' is between start_date and end_date
if date.today() != first_day_of_the_month:
    df_rede_mateus_transformed = df_rede_mateus_transformed.filter(
        col("DATA").between(start_date, end_date)
    )


# COMMAND ----------

spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

# COMMAND ----------

df_rede_mateus_transformed_group = (
    df_rede_mateus_transformed.groupBy(
        "CATEGORY",
        "COMPANY",
        "BRAND",
        "PRODUCT_SOURCE_CODE",
        "PRODUCT_SOURCE_NAME",
        "DATA",
        "PDV_COMPANY_NAME",
        "CNPJ_PDV",
        "PDV_TYPE",
        "ID_PDV_ECOMMERCE",
    )
    .agg(
        sum("SALES_VOLUME_SELLOUT").alias("SALES_VOLUME_SELLOUT"),
        sum("REVENUE_SELLOUT").alias("REVENUE_SELLOUT"),
        sum("SALES_VOLUME_QUANTITY").alias("SALES_VOLUME_QUANTITY"),
        sum("QUANTITY_STOCK_UNIT").alias("QUANTITY_STOCK_UNIT"),
    )
    .select(
        "CATEGORY",
        "COMPANY",
        "BRAND",
        "PRODUCT_SOURCE_CODE",
        "PRODUCT_SOURCE_NAME",
        "DATA",
        lit("DIRECTSELLOUT").alias("SOURCE_NAME"),
        lit("REDE_MATEUS").alias("SUBSOURCE_NAME"),
        "CNPJ_PDV",
        lit("0").cast(IntegerType()).alias("CHECKOUT"),
        "SALES_VOLUME_SELLOUT",
        "SALES_VOLUME_QUANTITY",
        "REVENUE_SELLOUT",
        "QUANTITY_STOCK_UNIT",
        "PDV_COMPANY_NAME",
        "ID_PDV_ECOMMERCE",
        "PDV_TYPE",
    )
)

# COMMAND ----------

df_rede_mateus_transformed_group.display()

# COMMAND ----------


