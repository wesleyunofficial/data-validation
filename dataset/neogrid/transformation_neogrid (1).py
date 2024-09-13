# Databricks notebook source
# MAGIC %pip install --quiet pendulum

# COMMAND ----------

# Databricks notebook source
import json

from engineeringstore.core.transformation.task.task_entrypoint import TaskEntryPoint
from engineeringstore.core.transformation.transformation import Transformation
from datetime import date, datetime, timedelta
from pytz import timezone
from pyspark.sql.window import Window
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    concat_ws,
    from_unixtime,
    lit,
    max,
    to_date,
    unix_timestamp,
    when,
)


# Main class for your transformation
class Sellout(Transformation):
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
        dates = [date(2024, 7, 29), date(2024, 7, 30), date(2024, 7, 31)]

        if current_datetime.date() in dates:
            return True
        else:
            return False

    def extract_silver_neogrid_sellout(
        self, current_datetime, is_first_execution
    ) -> DataFrame:
        """Return PySpark DataFrame with sellout data from the brewdat_uc_saz_prod.slv_saz_sales_neogrid.br_sellout.

        Args:
            current_datetime (datetime): Current datetime.
            is_first_execution (bool): Flag indicating if it's the first execution.

        Returns:
            DataFrame: PySpark DataFrame.
        """

        # Calculate the date for the previous day
        current_date = current_datetime.date() - timedelta(days=1)

        # Get the DataFrame from the specified table
        df_neogrid_sellout = self.get_table(
            "brewdat_uc_saz_prod.slv_saz_sales_neogrid.br_sellout"
        )

        # Add new column ingestion date with the year, month and day
        df_neogrid_sellout = df_neogrid_sellout.withColumn(
            "ingestion_date",
            concat_ws("-", col("year"), col("month"), col("day")).cast("date"),
        )

        # If it's the first execution, return the whole DataFrame
        if is_first_execution:
            return df_neogrid_sellout
        else:
            # Otherwise, filter the DataFrame by 'ingestion_date'
            df_neogrid_sellout = df_neogrid_sellout.where(
                df_neogrid_sellout.ingestion_date == current_date
            )

            return df_neogrid_sellout

    def extract_gold_neogrid_sellout(self) -> DataFrame:
        """Return PySpark DataFrame with sellout data from the brewdat_uc_saz_prod.gld_saz_sales_neogrid.sellout

        Returns:
            DataFrame: PySpark DataFrame.
        """

        df_gold_neogrid_sellout = self.get_table(
            "brewdat_uc_saz_prod.gld_saz_sales_neogrid.sellout"
        )

        return df_gold_neogrid_sellout

    def select_columns_neogrid_sellout(self, df_neogrid_sellout) -> DataFrame:
        """
        Processes the given DataFrame by renaming columns to uppercase,
        filtering out specific rows, and formatting the 'DIA' column as a date.

        Args:
            df_neogrid_sellout (DataFrame): Input DataFrame containing sellout data.

        Returns:
            DataFrame: Processed DataFrame with renamed columns, filtered rows,
                    and formatted 'DIA' column.
        """

        # Drop Columns year, month and day
        df_neogrid_sellout = df_neogrid_sellout.drop("year", "month", "day")

        # Rename all columns to upper case
        for column in df_neogrid_sellout.columns:
            df_neogrid_sellout = df_neogrid_sellout.withColumnRenamed(
                column, column.upper()
            )

        # Filter out rows where the 'DIA' column has the value 'Dia'
        df_neogrid_sellout = df_neogrid_sellout.where(col("DIA") != "Dia")

        # Convert 'DIA' column to a date format, handling different input formats
        df_neogrid_sellout = df_neogrid_sellout.withColumn(
            "DIA",
            when(
                to_date(
                    from_unixtime(unix_timestamp(df_neogrid_sellout.DIA, "yyyy-MM-dd"))
                ).isNotNull(),
                to_date(
                    from_unixtime(unix_timestamp(df_neogrid_sellout.DIA, "yyyy-MM-dd"))
                ),
            )
            .when(
                to_date(
                    from_unixtime(unix_timestamp(df_neogrid_sellout.DIA, "dd/MM/yyyy"))
                ).isNotNull(),
                to_date(
                    from_unixtime(unix_timestamp(df_neogrid_sellout.DIA, "dd/MM/yyyy"))
                ),
            )
            .otherwise("1900-01-01"),
        )

        return df_neogrid_sellout

    def apply_transformation(self, df_neogrid_sellout) -> DataFrame:
        """
        Apply various transformations to the input DataFrame.
        - drop duplicates values
        - filter rows with Sale Quantity Unit and Stock Quantity Unit
        - filter max ingestion date

        Args:
            df_neogrid_sellout (DataFrame): Input PySpark DataFrame to be transformed.

        Returns:
            DataFrame: Transformed PySpark DataFrame.
        """

        # Drop duplicate rows from the DataFrame
        df_transformed_neogrid_sellout = df_neogrid_sellout.dropDuplicates()

        # Filter rows where either QUANTIDADE_VENDA_UNIDADE is greater than '0,00'
        # or QUANTIDADE_ESTOQUE_UNIDADE is greater than or equal to '0,00'
        df_transformed_neogrid_sellout = df_transformed_neogrid_sellout.filter(
            (df_transformed_neogrid_sellout.QUANTIDADE_VENDA_UNIDADE > lit("0,00"))
            | (df_transformed_neogrid_sellout.QUANTIDADE_ESTOQUE_UNIDADE >= lit("0,00"))
        )
        # Define a window specification for partitioning and ordering the ingestion_date
        window = Window.partitionBy(
            [
                col("CODIGO_VAREJO"),
                col("DIA"),
                col("CNPJ"),
                col("EAN_PRODUTO_VAREJO"),
                col("DESCRICAO_PRODUTO_VAREJO"),
            ]
        ).orderBy(col("ingestion_date").desc())

        # Add new column with the max ingestion date
        df_transformed_neogrid_sellout = df_transformed_neogrid_sellout.withColumn(
            "max_ingestion_date", max(col("ingestion_date")).over(window)
        )

        # Filter by max ingestion date
        df_transformed_neogrid_sellout = df_transformed_neogrid_sellout.where(
            col("ingestion_date") == col("max_ingestion_date")
        )

        # Drop column max ingestion date
        df_transformed_neogrid_sellout = df_transformed_neogrid_sellout.drop(
            col("max_ingestion_date")
        )

        return df_transformed_neogrid_sellout

    def merge_dataframes(
        self, df_gold_neogrid_sellout, df_silver_neogrid_sellout
    ) -> DataFrame:
        """
        Merges two PySpark DataFrames containing Neogrid sellout data.

        This function takes two DataFrames as input and merges them using the `union` operation.
        The resulting DataFrame contains all rows from both input DataFrames.

        Args:
            df_gold_neogrid_sellout (DataFrame): The first DataFrame to be merged.
            df_silver_neogrid_sellout (DataFrame): The second DataFrame to be merged.

        Returns:
            DataFrame: A merged PySpark DataFrame containing all rows from both input DataFrames.
        """
        df_merged_neogrid_sellout = df_gold_neogrid_sellout.union(
            df_silver_neogrid_sellout
        )

        return df_merged_neogrid_sellout

    # This method is mandatory and the final transformation of your dataframe must be returned here
    def definitions(self):

        current_datetime = self.get_datetime()

        is_first_execution = self.is_first_execution_by_date(current_datetime)

        if is_first_execution:

            # Extract data from source (silver layer)
            df_extracted_neogrid_sellout = self.extract_silver_neogrid_sellout(
                current_datetime, True
            )

            # Select columns, verify null values and rename columns
            df_selected_neogrid_sellout = self.select_columns_neogrid_sellout(
                df_extracted_neogrid_sellout
            )

            # Apply transformation (drop duplicates)
            final_dataframe = self.apply_transformation(df_selected_neogrid_sellout)
        else:
            # Extract data from destination (gold layer)
            df_gold_neogrid_sellout = self.extract_gold_neogrid_sellout()

            # Extract data from source (silver layer)
            df_extracted_neogrid_sellout = self.extract_silver_neogrid_sellout(
                current_datetime, False
            )

            # Select columns, verify null values and rename columns
            df_selected_neogrid_sellout = self.select_columns_neogrid_sellout(
                df_extracted_neogrid_sellout
            )

            # Merge dataframes silver (source) and gold (destination)
            df_merged_neogrid_sellout = self.merge_dataframes(
                df_gold_neogrid_sellout, df_selected_neogrid_sellout
            )

            # Apply transformation (drop duplicates)
            final_dataframe = self.apply_transformation(df_merged_neogrid_sellout)

        return final_dataframe


# # Call brewdat TaskEntryPoint to handle transformation and load the data
# context = json.loads(dbutils.widgets.get("context"))
# task_entry_point = TaskEntryPoint(context=context, transformation_object=Sellout())
# task = task_entry_point.handle()
# task.run()



# COMMAND ----------

sellout_task = Sellout()

# COMMAND ----------

df_transformed = sellout_task.definitions()

# COMMAND ----------

df_transformed.count()

# COMMAND ----------



# COMMAND ----------


df_transformed.groupBy("DIA").agg(count("*").alias("contador")).display()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM brewdat_uc_saz_prod.gld_saz_sales_neogrid.sellout
# MAGIC
# MAGIC `brewdat_uc_saz_prod`.`gld_br_sales_sellout_neogrid`.`sellout`

# COMMAND ----------

df_selected = df_selected.withColumn("row_number", row_number().over(window))

# COMMAND ----------

df_selected.groupBy(col("CODIGO_VAREJO"), 
            col("DIA"), 
            col("CNPJ"), 
            col("EAN_PRODUTO_VAREJO"), 
            col("DESCRICAO_PRODUTO_VAREJO"))\
            .agg(count("*").alias("contador"))\
            .where(col("contador") > 1).display()

# COMMAND ----------

df_row1 = df_transformed.where("""
                  CODIGO_VAREJO == 864 AND 
                  DIA == '2024-06-28' AND 
                  CNPJ == '05789313000780' AND 
                  EAN_PRODUTO_VAREJO == '7891991298476'
                  AND CAUSA_RAIZ_OSA == "PBE"
                  """)

# COMMAND ----------

df_row2 = df_transformed.where("""
                  CODIGO_VAREJO == 864 AND 
                  DIA == '2024-06-28' AND 
                  CNPJ == '05789313000780' AND 
                  EAN_PRODUTO_VAREJO == '7891991298476'
                  AND CAUSA_RAIZ_OSA IS NULL
                  """)

# COMMAND ----------

for c in columns:
    comparisons = comparisons.withColumn(
        f"{c}_equal", 
        col(f"1_{c}") == col(f"2_{c}")
    )

# COMMAND ----------

comparisons.display()

# COMMAND ----------

df.where(df.dia != 'Dia').count()

# COMMAND ----------

df = df.withColumn('dia', \
                    when(to_date(from_unixtime(unix_timestamp(df.dia, 'yyyy-MM-dd'))).isNotNull(), \
                        to_date(from_unixtime(unix_timestamp(df.dia, 'yyyy-MM-dd')))) \
                    .when(to_date(from_unixtime(unix_timestamp(df.dia, 'dd/MM/yyyy'))).isNotNull(), \
                        to_date(from_unixtime(unix_timestamp(df.dia, 'dd/MM/yyyy')))) \
                    .otherwise('1900-01-01'))

# COMMAND ----------

df.select("dia").distinct().display()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT 
# MAGIC        year, 
# MAGIC        month, 
# MAGIC        day
# MAGIC FROM   brewdat_uc_saz_prod.slv_saz_sales_neogrid.br_sellout

# COMMAND ----------


year = date.today().year
month = date.today().month
day = date.today().day

# COMMAND ----------


df_neogrid_sellout = ( 
            spark.sql(f"""
                      SELECT DISTINCT dia, codigo_varejo
                      FROM brewdat_uc_saz_prod.slv_saz_sales_neogrid.br_sellout
                      WHERE 1=1
                      """)
        ).collect()

# COMMAND ----------

type(df_neogrid_sellout)

# COMMAND ----------

dia = ""
codigo_varejo = ""
for row in df_neogrid_sellout.sort(key=0, reverse=False):
    dia = row['dia']
    codigo_varejo = row["codigo_varejo"]

    print(f"dia={dia}, codigo_varejo={codigo_varejo}")



# COMMAND ----------

spark.sql(f"""
            SELECT DISTINCT day, month, year
            FROM brewdat_uc_saz_prod.slv_saz_sales_neogrid.br_sellout
            WHERE 1=1
            """).display()

# COMMAND ----------


