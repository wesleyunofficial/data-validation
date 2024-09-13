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
            # Attempt to read the table from the Unity Catalog
            df_rede_mateus_sellout = spark.table(table_name)

            # Check if the table has any records
            if df_rede_mateus_sellout.count() > 0:
                return df_rede_mateus_sellout
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
    def transformation_neogrid_presellout(
        self, df_neogrid: DataFrame
    ) -> DataFrame:
        """
            Apply transformations:
            - Add column
            - Alter data type
            - Group values

        Args:
            df_rede_mateus (DataFrame): _description_

        Returns:
            DataFrame: _description_
        """
        
        # Add column ID PDV E COMMERCE based on DESCRICAO LOCAL and BANDEIRA columns
        df_neogrid = df_neogrid.withColumn(
            "ID_PDV_ECOMMERCE", 
            when(((upper(df_neogrid.DESCRICAO_LOCAL)).like("%COMMERC%")) | (upper(df_neogrid.BANDEIRA) == "E-COMMERCE"), "S")
            .otherwise("N")
        )

        # Transform columns to the correct data type and add new columns
        df_neogrid_transformed = df_neogrid.select(
            coalesce(df_neogrid.CNPJ.cast(LongType()), lit(0)).alias("CNPJ_PDV"),
            to_date(coalesce(trim(regexp_replace(df_neogrid.DIA, "-", "")), lit("19000101")),"yyyyMMdd",).alias("DATA"),
            regexp_replace(coalesce(df_neogrid.QUANTIDADE_VENDA_UNIDADE, lit("0")), ",", ".").cast(DecimalType(38, 6)).alias("SALES_VOLUME_SELLOUT"),
            regexp_replace(coalesce(df_neogrid.QUANTIDADE_VENDA_UNIDADE, lit("0")), ",", ".").cast(DecimalType(38, 6)).alias("SALES_VOLUME_QUANTITY"),
            regexp_replace(coalesce(df_neogrid.VALOR_DE_VENDA, lit("0")), ",", ".").cast(DecimalType(38, 6)).alias("REVENUE_SELLOUT"),
            upper(coalesce(df_neogrid.CATEGORIA_NIELSEN, lit("NAO INFORMADO"))).alias("CATEGORY" ),
            upper(coalesce(df_neogrid.DESCRICAO_PRODUTO_VAREJO, lit("NAO INFORMADO"))).alias("PRODUCT_SOURCE_NAME"),
            upper(coalesce(df_neogrid.MARCA_NIELSEN, lit("NAO INFORMADO"))).alias("BRAND"),
            coalesce((df_neogrid.EAN_PRODUTO_VAREJO.cast(LongType())), lit(0)).alias("PRODUCT_SOURCE_CODE"),
            upper(coalesce(df_neogrid.BANDEIRA, lit("NAO INFORMADO"))).alias("PDV_COMPANY_NAME"),
            upper(coalesce(df_neogrid.TIPO_LOCAL, lit("NAO INFORMADO"))).alias("PDV_TYPE"),
            df_neogrid.ID_PDV_ECOMMERCE,
            regexp_replace(coalesce(df_neogrid.QUANTIDADE_ESTOQUE_UNIDADE, lit("0")), ",", ".").cast(DecimalType(38, 6)).alias("QUANTITY_STOCK_UNIT"),
        )

        # Filter the DataFrame to include only rows where 'DATA' is not null
        df_neogrid_transformed = df_neogrid_transformed.filter('DATA is not null')

        # set date range last 60 day
        start_date = date.today() - timedelta(days=60)
        end_date = date.today()

        # set first day of month
        first_day_of_the_month = date.today().replace(day=1)

        # if today is first day of the month
        # Filter the DataFrame to include only rows where 'DATA' is between start_date and end_date
        if date.today() != first_day_of_the_month:
            df_rede_mateus_transformed = df_neogrid_transformed.filter(
                col("DATA").between(start_date, end_date)
            )

        df_neogrid_transformed_group = df_neogrid_transformed.groupBy(
            "CATEGORY",
            "BRAND",
            "PRODUCT_SOURCE_CODE",
            "PRODUCT_SOURCE_NAME",
            "DATA",
            "PDV_COMPANY_NAME",
            "CNPJ_PDV",
            "PDV_TYPE",
            "ID_PDV_ECOMMERCE",
        ).agg(
            sum("SALES_VOLUME_SELLOUT").alias("SALES_VOLUME_SELLOUT"),
            sum("SALES_VOLUME_QUANTITY").alias("SALES_VOLUME_QUANTITY"),
            sum("REVENUE_SELLOUT").alias("REVENUE_SELLOUT"),
            sum("QUANTITY_STOCK_UNIT").alias("QUANTITY_STOCK_UNIT"),
        )
        
        return df_neogrid_transformed_group
    
    def select_columns_neogrid_presellout(
        self, df_neogrid: DataFrame
    ) -> DataFrame:
        """
            Select columns to be use in the final table

        Args:
            df_neogrid (DataFrame): _description_

        Returns:
            DataFrame: _description_
        """

        # Select columns to be use in the final table and add columns COMPANY, SOURCE_NAME and SUBSOURCE_NAME  
        df_neogrid_selected = df_neogrid.select(
            df_neogrid.CATEGORY,
            lit("AMBEV").alias("COMPANY"),
            df_neogrid.BRAND,
            df_neogrid.PRODUCT_SOURCE_CODE,
            df_neogrid.PRODUCT_SOURCE_NAME,
            df_neogrid.DATA,
            lit("NEOGRID DIRETA").alias("SOURCE_NAME"),
            lit("NEOGRID DIRETA").alias("SUBSOURCE_NAME"),
            df_neogrid.CNPJ_PDV.alias("CNPJ_PDV"),
            lit("0").cast(IntegerType()).alias("CHECKOUT"),
            df_neogrid.SALES_VOLUME_SELLOUT,
            df_neogrid.SALES_VOLUME_QUANTITY,
            df_neogrid.REVENUE_SELLOUT,
            df_neogrid.QUANTITY_STOCK_UNIT,
            df_neogrid.PDV_COMPANY_NAME,
            df_neogrid.ID_PDV_ECOMMERCE,
            df_neogrid.PDV_TYPE,
        )

        # Add columns to be used in the partition year, month and day
        df_neogrid_selected = (
            df_neogrid_selected
                .withColumn("CXA", lit(None).cast(DecimalType(38, 6)))
                .withColumn("PACK", lit(None).cast(DecimalType(38, 6)))
                .withColumn("ano_partition", year("DATA"))
                .withColumn("mes_partition", month("DATA"))
                .withColumn("dia_partition", dayofmonth("DATA"))    
        )

        return df_neogrid_selected

    # This method is mandatory and the final transformation of your dataframe must be returned here
    def definitions(self):
        table_name = "brewdat_uc_saz_prod.gld_saz_sales_neogrid.sellout" 

        # Apply your transformation and return your final dataframe
        df_neogrid_sellout = self.get_delta_table_unity_catalog(table_name)

        df_neogrid_transformed = self.transformation_neogrid_presellout(df_neogrid_sellout)

        df_neogrid_pre_sellout = self.select_columns_neogrid_presellout(df_neogrid_transformed)
        
        return df_neogrid_pre_sellout

# COMMAND ----------

pre_sellout = PreSellout()

df_neogrid = pre_sellout.definitions()

# COMMAND ----------

df_neogrid.display()


# COMMAND ----------

# Select columns to be use in the final table and add columns COMPANY, SOURCE_NAME and SUBSOURCE_NAME  
df_neogrid_selected = df_neogrid.select(
    df_neogrid.CATEGORY,
    lit("AMBEV").alias("COMPANY"),
    df_neogrid.BRAND,
    df_neogrid.PRODUCT_SOURCE_CODE,
    df_neogrid.PRODUCT_SOURCE_NAME,
    df_neogrid.DATA,
    lit("NEOGRID DIRETA").alias("SOURCE_NAME"),
    lit("NEOGRID DIRETA").alias("SUBSOURCE_NAME"),
    df_neogrid.CNPJ_PDV.alias("CNPJ_PDV"),
    lit("0").cast(IntegerType()).alias("CHECKOUT"),
    df_neogrid.SALES_VOLUME_SELLOUT,
    df_neogrid.SALES_VOLUME_QUANTITY,
    df_neogrid.REVENUE_SELLOUT,
    df_neogrid.QUANTITY_STOCK_UNIT,
    df_neogrid.PDV_COMPANY_NAME,
    df_neogrid.ID_PDV_ECOMMERCE,
    df_neogrid.PDV_TYPE,
)

# Add columns to be used in the partition year, month and day
df_neogrid_selected = (
    df_neogrid_selected
        .withColumn("CXA", lit(None).cast(DecimalType(38, 6)))
        .withColumn("PACK", lit(None).cast(DecimalType(38, 6)))
        .withColumn("ano_partition", year("DATA"))
        .withColumn("mes_partition", month("DATA"))
        .withColumn("dia_partition", dayofmonth("DATA"))    
)

# COMMAND ----------


