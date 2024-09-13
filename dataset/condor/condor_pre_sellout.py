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
    sum,
    to_date,
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
        """
            Read a table from the Unity Catalog.

        Args:
            table_name (str): name of the table to return

        Raises:
            ValueError: _description_
            FileNotFoundError: _description_

        Returns:
            DataFrame: _description_
        """
        try:
            df_condor_sellout = spark.table(table_name)
        
            if df_condor_sellout.count() > 0:
                return df_condor_sellout
            else:
                raise ValueError("Delta table exists, but is empty.")
            dbutils.notebook.exit("Delta table exists, but is empty.")
        except AnalysisException as e:
            if "Table or view not found" in str(e):
                raise FileNotFoundError(
                    f"Delta table {table_name} was not found in the Unity Catalog."
                )
                dbutils.notebook.exit(
                    f"Delta table {table_name} was not found in the Unity Catalog."
                )

    def transformation_condor_pre_sellout(self, df_condor:DataFrame) -> DataFrame:
        """
            Apply transformations:
            - Add column
            - Alter data type
        Args:
            df_condor (DataFrame): _description_

        Returns:
            DataFrame: _description_
        """  
        
        df_condor_transformed = df_condor.select(
            upper(coalesce(df_condor.CATEGORY, lit("NAO INFORMADO"))).alias("CATEGORY"),
            upper(coalesce(df_condor.COMPANY, lit("NAO INFORMADO"))).alias("COMPANY"),
            upper(coalesce(df_condor.BRAND_ID, lit("NAO INFORMADO"))).alias("BRAND"),
            coalesce((df_condor.EAN.cast(LongType())), lit(0)).alias("PRODUCT_SOURCE_CODE"),
            upper(coalesce(df_condor.PRODUCT_SOURCE_NAME, lit("NAO INFORMADO"))).alias(
                "PRODUCT_SOURCE_NAME"
            ),
            to_date(
                coalesce(trim(df_condor.DATA_VENDA), lit("01/01/1900")), "dd/MM/yyyy"
            ).alias("DATA"),
            lit("CONDOR").alias("SOURCE_NAME"),
            lit("CONDOR").alias("SUBSOURCE_NAME"),
            coalesce(df_condor.CNPJ_LOJA.cast(LongType()), lit(0)).alias("CNPJ_PDV"),
            lit("0").cast(IntegerType()).alias("CHECKOUT"),
            regexp_replace(coalesce(df_condor.VOLUME_UNIT, lit("0")), ",", ".")
            .cast(DecimalType(38, 6))
            .alias("SALES_VOLUME_SELLOUT"),
            regexp_replace(coalesce(df_condor.PACK_QUANTITY, lit("0")), ",", ".")
            .cast(DecimalType(38, 6))
            .alias("SALES_VOLUME_QUANTITY"),
            regexp_replace(coalesce(df_condor.FATURAMENTO, lit("0")), ",", ".")
            .cast(DecimalType(38, 6))
            .alias("REVENUE_SELLOUT"),
            lit("0").cast(DecimalType(38, 6)).alias("QUANTITY_STOCK_UNIT"),
            upper(coalesce(df_condor.COMPANY, lit("NAO INFORMADO"))).alias("PDV_COMPANY_NAME"),
            lit("N").alias("ID_PDV_ECOMMERCE"),
            lit("NAO INFORMADO").alias("PDV_TYPE"),
            lit(None).cast(DecimalType(38, 6)).alias("CXA"),
            lit(None).cast(DecimalType(38, 6)).alias("PACK"),
        )

        df_condor_transformed = df_condor_transformed.filter(
            "DATA is not null AND PDV_COMPANY_NAME == 'AMBEV'"
        )

        # set date range last 60 days
        start_date = date.today() - timedelta(days=60)
        end_date = date.today()

        # set the first day of month
        first_day_of_the_month = date.today().replace(day=1)

        # if today isn't the first day of the month then
        # filter the DataFrame to include only rows where 'DATA' is between start_date and end_date
        if date.today() != first_day_of_the_month:
            df_condor_transformed = df_condor_transformed.filter(
                col("DATA").between(start_date, end_date)
            )

        df_condor_transformed = (
            df_condor_transformed
                .withColumn("ano_partition", year("DATA"))
                .withColumn("mes_partition", month("DATA"))
                .withColumn("dia_partition", dayofmonth("DATA"))
        )

        return df_condor_transformed
    
    # This method is mandatory and the final transformation of your dataframe must be returned here
    def definitions(self):
        table_name = "brewdat_uc_saz_prod.gld_saz_sales_condor.sellout"
        
        # Apply your transformation and return your final dataframe
        df_condor_sellout = self.get_delta_table_unity_catalog(table_name)

        df_condor_pre_sellout = self.transformation_condor_pre_sellout(df_condor_sellout)
        
        return df_condor_pre_sellout

# COMMAND ----------

pre_sellout_task = PreSellout()

df_condor = pre_sellout_task.definitions()

# COMMAND ----------

df_condor.count()

# COMMAND ----------

df_condor.select("PDV_COMPANY_NAME").distinct().display()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df_condor.agg(min(col('DATA')), max(col('DATA'))).display() 

# COMMAND ----------




# COMMAND ----------

df_condor_transformed.display()

# COMMAND ----------


