# Databricks notebook source
# Databricks notebook source
import json

from engineeringstore.core.transformation.task.task_entrypoint import TaskEntryPoint
from engineeringstore.core.transformation.transformation import Transformation
from pyspark.sql.window import Window
from pyspark.sql.functions import col, concat_ws, max, row_number
from datetime import date, datetime, timedelta
from pytz import timezone
from pyspark.sql import DataFrame


# Main class for your transformation
class SelloutTask(Transformation):
    def __init__(self):
        super().__init__(dependencies=[])

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
        dates = [date(2024, 9, 9), date(2024, 9, 9)]

        if self.reference_date.date() in dates:
            return True
        else:
            return False

    def extract_silver_rede_mateus_sellout(
        self, current_datetime, is_first_load
    ) -> DataFrame:
        """
            Extract data from table brewdat_uc_saz_prod.slv_saz_sales_rede_mateus.br_sellout
        Returns:
            _type_: dataframe spark
        """

        df_rede_mateus_sellout = self.get_table(
            "brewdat_uc_saz_prod.slv_saz_sales_rede_mateus.br_sellout"
        )

        # Create a new column ingestion date with the year, month and day
        df_rede_mateus_sellout = df_rede_mateus_sellout.withColumn(
            "ingestion_date",
            concat_ws("-", col("year"), col("month"), col("day")).cast("date"),
        )

        if is_first_load:            
            return df_rede_mateus_sellout        
        else:
            current_date = current_datetime.date() - timedelta(days=7)

            df_rede_mateus_sellout = df_rede_mateus_sellout.filter(
                df_rede_mateus_sellout.ingestion_date >= current_date
            )
            
            return df_rede_mateus_sellout

    def select_columns_rede_mateus_sellout(self, df_rede_mateus_sellout) -> DataFrame:
        """
            - select only the required columns
            - rename them if necessary
            - add ingestion date column using columns year, month and day
        Returns:
            df_rede_mateus_sellout (spark.DataFrame): Transformed dataframe.
        """

        df_rede_mateus_selected = df_rede_mateus_sellout.select(
            "data",
            "cnpj",
            "desc",
            "secao",
            "descproduto",
            "codigobarras",
            "quantidadevenda",
            "valorvenda",
            "vendavirtual",
            "quantidadeestoque",
            "tipo",
            "ingestion_date",
        )

        # Rename columns for uppercase
        df_rede_mateus_selected = df_rede_mateus_selected.toDF(
            *[c.strip().upper() for c in df_rede_mateus_selected.columns]
        )

        return df_rede_mateus_selected

    def apply_transformation_in_source(self, df_rede_mateus_sellout) -> DataFrame:
        """
        Retrieve the first row number for CNPJ, CODIGOBARRAS, DESCPRODUTO, DATA 
        and filter only row number 1

        Args:
            df_rede_mateus_sellout (spark.DataFrame) : Selected dataframe.

        Returns:
            df_rede_mateus_sellout_transformed (spark.DataFrame): Transformed dataframe.
        """
        # Creating Windows Partition to get most recent date from Sellout Data
        window = Window.partitionBy(
            ["CNPJ", "CODIGOBARRAS", "DESCPRODUTO", "DATA"]
        ).orderBy(col("DATA").desc())

        # Creating filter column row_number based on window partition
        df_rede_mateus_sellout_transformed = df_rede_mateus_sellout.withColumn(
            "row_number", row_number().over(window)
        )

        # Filtering the rows with row_number = 1
        df_rede_mateus_sellout_transformed = df_rede_mateus_sellout_transformed.filter(
            df_rede_mateus_sellout_transformed.row_number == 1
        )
        # Drop column row_number
        df_rede_mateus_sellout_transformed = df_rede_mateus_sellout_transformed.drop(
            "row_number"
        )

        return df_rede_mateus_sellout_transformed

    def extract_gold_rede_mateus_sellout(self) -> DataFrame:
        """
            Method used to load Rede Mateus Sellout SCD type 1
        Returns:
            df_gold_rede_mateus_sellout (spark.DataFrame): Dataframe destination.
        """

        df_gold_rede_mateus_sellout = self.get_table(
            "brewdat_uc_saz_prod.gld_saz_sales_rede_mateus.sellout"
        )

        return df_gold_rede_mateus_sellout

    def merge_dataframes(
        self, df_gold_rede_mateus_sellout, df_rede_mateus_sellout
    ) -> DataFrame:
        """
            Merge source and destination dataframes and return for next step
        Args:
            df_gold_rede_mateus_sellout (spark.DataFrame): Destination dataframe
            df_rede_mateus_sellout (spark.DataFrame): Source dataframe

        Returns:
            DataFrame (spark.DataFrame): Merged dataframe
        """

        return df_gold_rede_mateus_sellout.union(df_rede_mateus_sellout)

    def apply_transformation_in_destination(self, df_rede_mateus_sellout) -> DataFrame:
        """_summary_

        Args:
            df_rede_mateus_sellout (_type_): _description_

        Returns:
            DataFrame (spark.DataFrame): Transformed dataframe
        """

        window = Window.partitionBy(
            ["CNPJ", "CODIGOBARRAS", "DESCPRODUTO", "DATA"]
        ).orderBy(col("DATA").desc())

        # Creating filter column max_dat based on window partition
        df_rede_mateus_sellout_transformed = df_rede_mateus_sellout.withColumn(
            "max_ingestion_date", max(col("ingestion_date")).over(window)
        )

        # Filtering most recent data based on ingestion date
        df_rede_mateus_sellout_transformed = df_rede_mateus_sellout_transformed.where(
            (col("ingestion_date") == col("max_ingestion_date"))
        )

        # Drop duplicates if exists
        df_rede_mateus_sellout_transformed = (
            df_rede_mateus_sellout_transformed.dropDuplicates()
        )

        # Drop column max_ingestion_date
        df_rede_mateus_sellout_transformed = df_rede_mateus_sellout_transformed.drop(
            "max_ingestion_date"
        )

        return df_rede_mateus_sellout_transformed

    # This method is mandatory and the final transformation of your dataframe must be returned here
    def definitions(self):
        """
            Data transformation pipeline
        Returns:
            final_dataframe (spark.DataFrame): Transformed dataframe.
        """
        current_datetime = self.get_datetime()

        is_first_load = self.is_first_execution_by_date(current_datetime)

        if is_first_load:
            df_silver_rede_mateus_sellout = self.extract_silver_rede_mateus_sellout(
                current_datetime, True
            )

            df_rede_mateus_selected = self.select_columns_rede_mateus_sellout(
                df_silver_rede_mateus_sellout
            )

            df_rede_mateus_final = self.apply_transformation_in_source(
                df_rede_mateus_selected
            )
        else:
            df_gold_rede_mateus_sellout = (
                self.extract_gold_rede_mateus_sellout()
            )

            df_silver_rede_mateus_sellout = self.extract_silver_rede_mateus_sellout(
                current_datetime, False
            )

            df_rede_mateus_selected = self.select_columns_rede_mateus_sellout(
                df_silver_rede_mateus_sellout
            )

            df_rede_mateus_transformed = self.apply_transformation_in_source(
                df_rede_mateus_selected
            )

            df_rede_mateus_merged = self.merge_dataframes(
                df_gold_rede_mateus_sellout, df_rede_mateus_transformed
            )

            df_rede_mateus_final = self.apply_transformation_in_destination(
                df_rede_mateus_merged
            )

        return df_rede_mateus_final


# COMMAND ----------

sellout_task = SelloutTask()
df__ = sellout_task.definitions()

# COMMAND ----------

sellout_task.get_datetime()

# COMMAND ----------

sellout_task.is_first_execution_by_date(datetime.now())

# COMMAND ----------

df_silver = sellout_task.extract_silver_rede_mateus_sellout(datetime.now(), False)

# COMMAND ----------

df_silver.display()

# COMMAND ----------

df__.filter(col("INGESTION_DATE") == "2024-07-16").display()

# COMMAND ----------

df_silver.groupBy("INGESTION_DATE").count().display()

# COMMAND ----------


df = sellout_task.load_silver_rede_mateus_sellout(datetime.now(), True)

# COMMAND ----------

df.where("""
           DATA == '2024-06-01' 
            AND DESCPRODUTO == 'OUTROS 1L'
            AND CNPJ == '3995515017647'           
           """).display()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  *
# MAGIC FROM  brewdat_uc_saz_prod.gld_saz_sales_rede_mateus.sellout
# MAGIC WHERE   1=1
# MAGIC AND DATA == '2024-06-01' 
# MAGIC AND DESCPRODUTO == 'OUTROS 1L'
# MAGIC AND CNPJ == '3995515017647'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  *
# MAGIC FROM  brewdat_uc_saz_prod.slv_saz_sales_rede_mateus.br_sellout
# MAGIC WHERE   1=1
# MAGIC AND DATA == '2024-06-01' 
# MAGIC AND DESCPRODUTO == 'OUTROS 1L'
# MAGIC AND CNPJ == '3995515017647'

# COMMAND ----------

# MAGIC %sh curl -v telnet://ns-mdm-adapters-dev-scus-001.servicebus.windows.net

# COMMAND ----------


