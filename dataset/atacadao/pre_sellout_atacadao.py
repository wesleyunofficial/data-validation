# Databricks notebook source
# Importação das bibliotecas no contexto do notebook:
from datetime import datetime, timedelta, date
from pyspark.sql.functions import (coalesce,
                                   col,
                                   lit,
                                   split,
                                   to_date,
                                   sum,
                                   translate,
                                   trim,
                                   upper)
from pyspark.sql.types import IntegerType, DecimalType, LongType, DateType
import time

# COMMAND ----------

# Databricks notebook source
import json

from engineeringstore.core.transformation.task.task_entrypoint import TaskEntryPoint
from engineeringstore.core.transformation.transformation import Transformation
from pyspark.sql import DataFrame
from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType, DecimalType, LongType, DateType, DoubleType
from datetime import datetime, timedelta, date


# Main class for your transformation
class PreSellout(Transformation):
    def __init__(self):
        super().__init__(dependencies=[])

    def get_delta_table_unity_catalog(self, table_name: str) -> DataFrame:
        try:
            # Attempt to read the table from the Unity Catalog
            df_atacadao_sales = spark.table(table_name)

            # Check if the table has any records
            if df_atacadao_sales.count() > 0:
                return df_atacadao_sales
            else:
                # Raise an error if the table exists but is empty
                raise ValueError("Delta table exists, but is empty.")
                dbutils.notebook.exit("Delta table exists, but is empty.")
        except AnalysisException as e:
            # Handle the case where the table is not found
            if "Table or view not found" in str(e):
                raise FileNotFoundError(
                    f"Delta table {table_name} was not found in the Unity Catalog."
                )
                dbutils.notebook.exit(
                    f"Delta table {table_name} was not found in the Unity Catalog."
                )
            else:
                # Handle any other exceptions
                raise dbutils.notebook.exit(e)

    def transformation_atacadao_presellout(self, df_atacadao_sales: DataFrame) -> DataFrame:

        # Split the column 'MULT' into two new columns 'CXA' and 'PACK' using " X " as the delimiter
        df_atacadao_transformed = (
            df_atacadao_sales
                .withColumn(
                    "CXA", 
                    split(df_atacadao_sales["MULT"], " X ").getItem(0))
                .withColumn(
                    "PACK", 
                    split(df_atacadao_sales["MULT"], " X ").getItem(1))
            )

        # Apply transformations and standardizations to the DataFrame
        df_atacadao_transformed = (
            df_atacadao_transformed.select(
                coalesce(trim(df_atacadao_transformed.CNPJ), lit('0')).cast(LongType()).alias('CNPJ_PDV'),
                to_date(coalesce(to_date((df_atacadao_transformed.DATA), 'dd/MM/yyyy'), lit('19000101')), 'yyyy-MM-dd').alias('DATA'),
                translate(coalesce(df_atacadao_transformed.QTDE_VENDA_UNIDADE, lit('0')), ',', '.').cast(DecimalType(38,6)).alias('SALES_VOLUME_SELLOUT'),
                translate(coalesce(df_atacadao_transformed.QTDE_VENDA_UNIDADE, lit('0')), ',', '.').cast(DecimalType(38,6)).alias('SALES_VOLUME_QUANTITY'),
                lit('0').cast(DecimalType(38,6)).alias('REVENUE_SELLOUT'),
                lit('NAO INFORMADO').alias('CATEGORY'),
                upper(coalesce(df_atacadao_transformed.DESCRICAO, lit('NAO INFORMADO'))).alias('PRODUCT_SOURCE_NAME'),
                lit('NAO INFORMADO').alias('BRAND'),
                coalesce(df_atacadao_transformed.CODIGO_BARRA.cast(LongType()), lit(0)).alias('PRODUCT_SOURCE_CODE'),
                coalesce(col('CODIGO_BARRA'), lit('0')).cast(LongType()).alias('EAN_SOURCE_CODE'),
                lit('NAO INFORMADO').alias('PDV_COMPANY_NAME'),
                lit('NAO INFORMADO').alias('PDV_TYPE'),
                lit('NAO INFORMADO').alias('ID_PDV_ECOMMERCE'), 
                translate(coalesce(df_atacadao_transformed.ESTOQUE_BRUTO, lit('0')), ',', '.').cast(DecimalType(38,6)).alias('QUANTITY_STOCK_UNIT'),
                df_atacadao_transformed.CXA.cast(DecimalType(38,6)),
                df_atacadao_transformed.PACK.cast(DecimalType(38,6))
            )
        )

        # Calculate the SALES_VOLUME_SELLOUT and QUANTITY_STOCK_UNIT by dividing them by the product of 'CXA' and 'PACK'
        df_atacadao_transformed = (
            df_atacadao_transformed.withColumn('SALES_VOLUME_SELLOUT', 
                                        df_atacadao_transformed.SALES_VOLUME_SELLOUT /
                                        (df_atacadao_transformed.CXA * df_atacadao_transformed.PACK))
                                .withColumn('QUANTITY_STOCK_UNIT', 
                                            df_atacadao_transformed.QUANTITY_STOCK_UNIT /
                                            (df_atacadao_transformed.CXA * df_atacadao_transformed.PACK))
                )

        # Filter the DataFrame to include only rows where 'DATA' is not null
        df_atacadao_transformed = df_atacadao_transformed.filter('DATA is not null')

        # set date range last 60 day
        start_date = date.today() - timedelta(days=60)
        end_date = date.today()

        # set first day of month
        first_day_of_the_month = date.today().replace(day=1)

        # if today is first day of the month
        # Filter the DataFrame to include only rows where 'DATA' is between start_date and end_date
        if date.today() != first_day_of_the_month:
            df_atacadao_transformed = (
                df_atacadao_transformed.filter(col("DATA").between(start_date, end_date))
            )

        # Aggregate the data by the specified columns and sum the relevant metrics
        df_atacadao_transformed_group = (
            df_atacadao_transformed
            .groupBy(
                "CATEGORY",
                "BRAND",
                "PRODUCT_SOURCE_CODE",
                "EAN_SOURCE_CODE",
                "PRODUCT_SOURCE_NAME",
                "DATA",
                "PDV_COMPANY_NAME",
                "CNPJ_PDV",
                "PDV_TYPE",
                "ID_PDV_ECOMMERCE",
                "CXA",
                "PACK")
            .agg(
                sum("SALES_VOLUME_SELLOUT").alias('SALES_VOLUME_SELLOUT'),
                sum("REVENUE_SELLOUT").alias('REVENUE_SELLOUT'),
                sum("SALES_VOLUME_QUANTITY").alias('SALES_VOLUME_QUANTITY'),
                sum("QUANTITY_STOCK_UNIT").alias('QUANTITY_STOCK_UNIT')
                )
            )
        
        return df_atacadao_transformed_group
    
    def select_columns_atacadao_presellout(self, df_atacadao_sales: DataFrame) -> DataFrame:

        # Select specific columns from the DataFrame and add new columns with constant values
        df_atacadao_sales_selected = (
            df_atacadao_sales.select(
                df_atacadao_sales.CATEGORY, 
                lit('AMBEV').alias('COMPANY'),  # Add a new column 'COMPANY' with a constant value 'AMBEV'
                df_atacadao_sales.BRAND,
                df_atacadao_sales.PRODUCT_SOURCE_CODE,
                df_atacadao_sales.PRODUCT_SOURCE_NAME,
                df_atacadao_sales.DATA,                  
                lit('ATACADAO').alias('SOURCE_NAME'),  # Add a new column 'SOURCE_NAME' with a constant value 'ATACADAO'
                lit('ATACADAO').alias('SUBSOURCE_NAME'),  # Add a new column 'SUBSOURCE_NAME' with a constant value 'ATACADAO'
                df_atacadao_sales.CNPJ_PDV.alias('CNPJ_PDV'),
                lit('0').cast(IntegerType()).alias('CHECKOUT'),  # Add a new column 'CHECKOUT' with a constant value '0'
                df_atacadao_sales.SALES_VOLUME_SELLOUT,
                df_atacadao_sales.SALES_VOLUME_QUANTITY,
                df_atacadao_sales.REVENUE_SELLOUT,
                df_atacadao_sales.QUANTITY_STOCK_UNIT,  
                df_atacadao_sales.PDV_COMPANY_NAME,
                df_atacadao_sales.ID_PDV_ECOMMERCE,
                df_atacadao_sales.PDV_TYPE,
                df_atacadao_sales.CXA,
                df_atacadao_sales.PACK)
            )
        
        # Add partitioning columns based on the 'DATA' column
        df_atacadao_sales_selected = (
            df_atacadao_sales_selected
                .withColumn('ano_partition', year('DATA'))  # Extract the year from 'DATA' and add it as 'ano_partition'
                .withColumn('mes_partition', month('DATA'))  # Extract the month from 'DATA' and add it as 'mes_partition'
                .withColumn('dia_partition', dayofmonth('DATA'))  # Extract the day of the month from 'DATA' and add it as 'dia_partition'
        )

        return df_atacadao_sales_selected


    def extract_gold_atacadao_sales(self) -> DataFrame:
        try:
            df_gold_atacadao_sales = self.get_table(
                "brewdat_uc_saz_prod.gld_saz_sales_atacadao.sales"
            )
        except:
            dbutils.notebook.exit("Não há dados nos arquivos fontes")

        return df_gold_atacadao_sales

    # This method is mandatory and the final transformation of your dataframe must be returned here
    def definitions(self):
        table_name = "brewdat_uc_saz_prod.gld_saz_sales_atacadao.sales"

        df_atacadao_sales = self.get_delta_table_unity_catalog(table_name)

        df_atacadao_transformed = self.transformation_atacadao_presellout(df_atacadao_sales)

        df_atacadao_pre_sellout = self.select_columns_atacadao_presellout(df_atacadao_transformed)

        return df_atacadao_pre_sellout

# COMMAND ----------

pre_sellout = PreSellout()

df_atacadao_sales = pre_sellout.definitions()

# COMMAND ----------

df_atacadao_sales.diplay()

# COMMAND ----------

df_atacadao_sales.agg(min('DATA'), max('DATA'), count("*"), sum('SALES_VOLUME_SELLOUT')).display()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * EXCEPT (year, month, day) FROM brewdat_uc_saz_prod.slv_saz_sales_atacadao.br_sales

# COMMAND ----------

spark.sql("SELECT * EXCEPT (year, month, day) FROM brewdat_uc_saz_prod.slv_saz_sales_atacadao.br_sales").display()

# COMMAND ----------

import json
def return_metadata(dataframe):
  schema_json = dataframe.schema.json()
 
  schemas = json.loads(schema_json)

  for schema in schemas['fields']:
    print(f"- name: '{schema['name']}'\n  description: ''\n  type: '{schema['type']}'")

# COMMAND ----------

return_metadata(df_atacadao_sales)

# COMMAND ----------

# set date range last 60 day
start_date = date.today() - timedelta(days=60)
end_date = date.today()

# set first day of month
first_day_of_month = date.today().replace(day=1)

if date.today() != first_day_of_month:

#     df_atacadao_transformed = (
#         df_atacadao_transformed.filter(col("DATA").between(start_date, end_date))
#     )

# COMMAND ----------

v_option_mode = "static"

if start_date.replace(' ',''):
    v_option_mode = "dynamic"
  # df_atacadao_volume = df_atacadao_volume.filter(col('DATA').between(v_data_inicio,v_data_fim))

# COMMAND ----------

v_string = ' '

# COMMAND ----------



if v_string.replace('',''):
    print(True)
else:
    print(False)

# COMMAND ----------


from pyspark.sql.functions import col, column

# COMMAND ----------

data = [["apple", 10.0], ["orange", 20.0], ["banana", 30.0]]

df = spark.createDataFrame(data, ["name", "price"])

# COMMAND ----------

df.select(column("name"), col("price")).display()

# COMMAND ----------

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "DYNAMIC")

# Check the mode for Partition Overwrite
spark.conf.get("spark.sql.sources.partitionOverwriteMode")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT min(to_date(data, 'dd/MM/yyyy')), 
# MAGIC        max(to_date(data, 'dd/MM/yyyy')),
# MAGIC        count(*)
# MAGIC FROM brewdat_uc_saz_prod.slv_saz_sales_atacadao.br_sales

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT concat_ws("-", year, month, day),
# MAGIC        to_date(data, 'dd/MM/yyyy'),
# MAGIC
# MAGIC        count(*)
# MAGIC FROM brewdat_uc_saz_prod.slv_saz_sales_atacadao.br_sales
# MAGIC GROUP BY concat_ws("-", year, month, day), to_date(data, 'dd/MM/yyyy')
# MAGIC ORDER BY concat_ws("-", year, month, day), to_date(data, 'dd/MM/yyyy')

# COMMAND ----------

df_ingestion = spark.sql(
"""
    SELECT  *
    FROM    brewdat_uc_saz_prod.slv_saz_sales_atacadao.br_sales slv 
    JOIN
    (
        SELECT  DISTINCT         
                cast(concat_ws("-", slv.year, slv.month, slv.day) as date) AS ingestion_date
        FROM    brewdat_uc_saz_prod.slv_saz_sales_atacadao.br_sales slv
        ORDER BY ingestion_date DESC
        LIMIT 4
    ) AS tmp
    ON cast(concat_ws("-", slv.year, slv.month, slv.day) as date) = tmp.ingestion_date
"""
)

df_ingestion.groupBy("ingestion_date").count().display()

# COMMAND ----------

df_ingestion.groupBy("ingestion_date").count().display()

# COMMAND ----------


