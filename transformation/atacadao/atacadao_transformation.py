# Databricks notebook source
# Databricks notebook source
import json

from engineeringstore.core.transformation.task.task_entrypoint import TaskEntryPoint
from engineeringstore.core.transformation.transformation import Transformation
from datetime import date, datetime, timedelta
from pytz import timezone
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import (col,
                                   concat_ws,
                                   count,
                                   max,
                                   row_number,
                                   when)


# Main class for your transformation
class Sales(Transformation):
    def __init__(self):
        super().__init__(
            dependencies = [
            ]
        )

    def get_datetime(self) -> datetime:
        """
            Get datetime
        Returns:
           current_datetime (datetime): current datetime.
        """
        time_zone = timezone("America/Sao_Paulo")
        current_datetime = datetime.now(tz=time_zone)
        return current_datetime
    
    def is_first_execution_by_date(self, current_datetime) -> bool:
        """
            Check if is the first_execution verifing if dates are equals.
        Returnes:
            _type_: bool
        """
        dates = [date(2024, 7, 18), 
                 date(2024, 7, 19)]

        if current_datetime.date() in dates:
            return True
        else:
            return False

    # Function to check for NaN and replace with None if NaN
    def check_nan(self, column):
        return when(col(column) != 'NaN', col(column)).otherwise(None)

    def extract_silver_atacadao_sales(
        self, current_datetime, is_first_execution
        ) -> DataFrame:
        
        current_date = current_datetime.date() - timedelta(weeks=1)        

        df_atacadao_sales = spark.table("brewdat_uc_saz_prod.brz_saz_sales_atacadao.br_sales")    

        # Add new column ingestion date with the year, month and day
        df_atacadao_sales = (
            df_atacadao_sales
                .withColumn("ingestion_date", concat_ws("-", 
                                                        col("year"), 
                                                        col("month"), 
                                                        col("day")).cast("date"),
                            )
                        )

        # Verify if is the first execution then return the whole dataframe else filter by ingestion_date
        if is_first_execution:
            return df_atacadao_sales
        else:
            return df_atacadao_sales.where(
                df_atacadao_sales.ingestion_date >= current_date
            )
            
    def select_columns_atacada_sales(self, df_atacadao_sales) -> DataFrame:

        df_atacadao_sales_columns = df_atacadao_sales.select("*")

        # Apply the function to each column in the DataFrame 
        for column in df_atacadao_sales_columns.columns:
            if column not in ["year", "month", "day", "ingestion_date"]:
                df_atacadao_sales_columns = df_atacadao_sales_columns.withColumn(column, self.check_nan(column))

        df_atacadao_sales_columns = df_atacadao_sales_columns.drop("year", "month", "day")
        
        # Rename columns name for uppercase
        df_atacadao_sales_columns = df_atacadao_sales_columns.toDF(
            *[c.strip().upper() for c in df_atacadao_sales_columns.columns]
        )            

        return df_atacadao_sales_columns
    
    def transformation_in_source(self, df_atacadao_sales) -> DataFrame:
        
        window = (
                Window.partitionBy([
                col("DATA"), 
                col("CNPJ"), 
                col("CODIGO_BARRA"), 
                col("DESCRICAO")
            ]).orderBy(col("ingestion_date").desc())
        )

        df_atacadao_sales_transformed = df_atacadao_sales.withColumn(
            "max_ingestion_date", max(col("ingestion_date")).over(window)
        )
        
        df_atacadao_sales_transformed = df_atacadao_sales_transformed.where(
            (col("ingestion_date") == col("max_ingestion_date"))
        )

        df_atacadao_sales_transformed = (
            df_atacadao_sales_transformed.drop(col("max_ingestion_date"))
        )
        
        df_atacadao_sales_transformed = (
            df_atacadao_sales_transformed.dropDuplicates()
        )

        return df_atacadao_sales_transformed
    
    def extract_gold_atacadao_sales(self) -> DataFrame:

        df_gold_atacadao_sales = self.get_table(
            "brewdat_uc_saz_prod.gld_br_sales_sellout_atacadao.sales"
        )

        return df_gold_atacadao_sales
    
    def merge_dataframes(self, df_gold_atacadao_sales, df_silver_atacadao_sales) -> DataFrame:
        
        df_atacadao_sales_merged = df_gold_atacadao_sales.union(df_silver_atacadao_sales)
        
        return df_atacadao_sales_merged
    
    def transformation_in_destination(self, df_atacadao_sales_merged) -> DataFrame:
        df_atacadao_sales_transformed = df_atacadao_sales_merged.dropDuplicates()

        return df_atacadao_sales_transformed


    # This method is mandatory and the final transformation of your dataframe must be returned here
    def definitions(self):
        # Apply your transformation and return your final dataframe
        #df_example = self.extract_silver_atacadao_sales()
        current_datetime = self.get_datetime()

        is_first_execution = self.is_first_execution_by_date(current_datetime)

        if is_first_execution:

            # extract
            df_extracted_atacadao_sales = self.extract_silver_atacadao_sales(
                current_datetime, True
            )

            # select columns, verify nulls and rename columns
            df_selected_atacadao_sales = self.select_columns_atacada_sales(df_extracted_atacadao_sales)

            #transformation
            final_dataframe = self.transformation_in_source(df_selected_atacadao_sales)
        else:            

            df_gold_atacadao_sales = self.extract_gold_atacadao_sales()

            df_extracted_atacadao_sales = self.extract_silver_atacadao_sales(current_datetime, False)

            # select columns, verify nulls and rename columns
            df_selected_atacadao_sales = self.select_columns_atacada_sales(df_extracted_atacadao_sales)

            # merge dataframes silver source and gold destination
            df_merged_atacadao_sales = self.merge_dataframes(df_gold_atacadao_sales, df_extracted_atacadao_sales)

            # apply transformation in merged dataframe
            final_dataframe = self.transformation_in_source(df_merged_atacadao_sales)

        return final_dataframe

# COMMAND ----------

sales_task = Sales()

df = sales_task.definitions()

# COMMAND ----------

df_transform.groupBy("DATA").count().display()

# COMMAND ----------

(
    df_select.groupBy(
    col("DATA"), col("CNPJ"), col("CODIGO_BARRA")).agg(count("DATA").alias("contador"))
    .where("contador > 1")
    .orderBy(col("DATA")).display()
)

# COMMAND ----------

df_select.where("""DATA == '08/07/2024' AND 
                CNPJ == '75315333000370' AND 
                CODIGO_BARRA == '7892840808495' AND 
                DESCRICAO == 'ISOTO.GATORADE TANGERINA PET'""").display()

# COMMAND ----------

window = Window.partitionBy([
    col("DATA"), col("CNPJ"), col("CODIGO_BARRA"), col("DESCRICAO")
]).orderBy(col("ingestion_date").desc())

# COMMAND ----------

df_select = df_select.withColumn("row_number", row_number().over(window))

# COMMAND ----------

df_select.where("row_number == 1").limit(100).display()

# COMMAND ----------

df_transform.createOrReplaceTempView("df_transform")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  DATA, count(*)
# MAGIC FROM    df_transform
# MAGIC GROUP BY DATA

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   FILIAL,
# MAGIC   CNPJ,
# MAGIC   ATIVIDADE,
# MAGIC   UF,
# MAGIC   MERCADORIA,
# MAGIC   CODIGO_BARRA,
# MAGIC   DESCRICAO,
# MAGIC   SIGLA,
# MAGIC   MULT,
# MAGIC   ESTOQUE_BRUTO,
# MAGIC   QTDE_VENDA_UNIDADE,
# MAGIC   DATA,
# MAGIC   max(ingestion_date) max_data
# MAGIC FROM
# MAGIC   df_transform
# MAGIC GROUP BY FILIAL,
# MAGIC   CNPJ,
# MAGIC   ATIVIDADE,
# MAGIC   UF,
# MAGIC   MERCADORIA,
# MAGIC   CODIGO_BARRA,
# MAGIC   DESCRICAO,
# MAGIC   SIGLA,
# MAGIC   MULT,
# MAGIC   ESTOQUE_BRUTO,
# MAGIC   QTDE_VENDA_UNIDADE,
# MAGIC   DATA

# COMMAND ----------

(df_transform.groupBy(
    col("DATA")).agg(count("DATA").alias("contador"))
    .where("contador > 1")
    .orderBy(col("DATA")).display()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC brewdat_uc_saz_prod.slv_saz_sales_atacadao.br_sales

# COMMAND ----------

df_duplicates = (df_select.groupBy(
    col("DATA"), col("CNPJ"), col("DESCRICAO")).agg(count("DATA").alias("contador"))
    .where("contador > 1")
    .orderBy(col("DATA"))
)


# COMMAND ----------

join_df = ((df_select.DATA == df_duplicates.DATA) & (df_select.DESCRICAO == df_duplicates.DESCRICAO) & (df_select.CNPJ == df_duplicates.CNPJ))

# COMMAND ----------

df_select.join(df_duplicates, join_df, "inner").display()

# COMMAND ----------

df_transform.groupBy(
    col("DATA")).count().orderBy(col("DATA")).display()

# COMMAND ----------

df.display()

# COMMAND ----------

# df = (
#     df
#     .withColumn(
#             "FILIAL", 
#             when(col("FILIAL") != "NaN", col("FILIAL")).otherwise(None)
#         )
#     .withColumn(
#             "CNPJ", 
#             when(col("CNPJ") != "NaN", col("CNPJ")).otherwise(None)
#         )
#     .withColumn(
#             "ATIVIDADE", 
#             when(col("ATIVIDADE") != "NaN", col("ATIVIDADE")).otherwise(None)
#         )
#     .withColumn(
#             "UF",
#             when(col("UF") != "NaN", col("UF")).otherwise(None)
#         )
#     .withColumn(
#             "MERCADORIA",
#             when(col("MERCADORIA") != "NaN", col("MERCADORIA")).otherwise(None),
#         )
#     .withColumn(
#             "CODIGO_BARRA",
#             when(col("CODIGO_BARRA") != "NaN", col("CODIGO_BARRA")).otherwise(None),
#         )
#     .withColumn(
#             "DESCRICAO", 
#             when(col("DESCRICAO") != "NaN", col("DESCRICAO")).otherwise(None)
#         )
#     .withColumn(
#             "SIGLA", 
#             when(col("SIGLA") != "NaN", col("SIGLA")).otherwise(None)
#         )
#     .withColumn(
#             "MULT", 
#             when(col("MULT") != "NaN", col("MULT")).otherwise(None)
#         )
#     .withColumn(
#             "ESTOQUE_BRUTO",
#             when(col("ESTOQUE_BRUTO") != "NaN", col("ESTOQUE_BRUTO")).otherwise(None),
#         )
#     .withColumn(
#             "QTDE_VENDA_UNIDADE",
#             when(col("QTDE_VENDA_UNIDADE") != "NaN", col("QTDE_VENDA_UNIDADE")).otherwise(None),
#         )
#     .withColumn(
#             "DATA", 
#             when(col("DATA") != "NaN", col("DATA")).otherwise(None)
#         )
# )

# COMMAND ----------

df.groupBy(
    col("DATA"), col("year"), col("month"), col("day")).count().orderBy(col("DATA")).display()

# COMMAND ----------

# Sample data
data = [(1.0, float('nan')), (2.0, 3.0), (float('nan'), 4.0)]
columns = ["col1", "col2"]

# Create DataFrame
df = spark.createDataFrame(data, columns)

# Function to check for NaN and replace with None if NaN
def check_nan(column):
    return when(col(column) != 'NaN', col(column)).otherwise(None)

# Apply the function to each column in the DataFrame
for column in df.columns:
    #df = df.withColumn(column, check_nan(column))
    print(column)

# Show the result
df.show()

# COMMAND ----------

# Sample data
data = [(1.0, float('nan')), (2.0, 3.0), (float('nan'), 4.0)]
columns = ["col1", "col2"]

# Create DataFrame
df = spark.createDataFrame(data, columns)

df.show()

# Apply the function to each column in the DataFrame
for column in df.columns:
    df = df.withColumn(column, sales_task.check_nan(column))



# COMMAND ----------


