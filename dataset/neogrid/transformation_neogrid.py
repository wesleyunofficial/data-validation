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
    count,
    from_unixtime,
    lit,
    max,
    row_number,
    to_date,    
    unix_timestamp,
    when
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
        dates = [date(2024, 7, 25), date(2024, 7, 26)]

        if current_datetime.date() in dates:
            return True
        else:
            return False        

    def extract_silver_neogrid_sales(
        self, current_datetime, is_first_execution
    ) -> DataFrame:

        current_date = current_datetime.date() - timedelta(days=1)

        df_neogrid_sellout = self.get_table(
            "brewdat_uc_saz_prod.slv_saz_sales_neogrid.br_sellout"
        )

        # Add new column ingestion date with the year, month and day
        df_neogrid_sellout = df_neogrid_sellout.withColumn(
            "ingestion_date",
            concat_ws("-", col("year"), col("month"), col("day")).cast("date"),
        )

        # Verify if is the first execution then return the whole dataframe else filter by ingestion_date
        if is_first_execution:
            return df_neogrid_sellout
        else:
            df_neogrid_sellout = df_neogrid_sellout.where(
                df_neogrid_sellout.ingestion_date == current_date
            )

            return df_neogrid_sellout

    def select_columns_neogrid_sales(self, df_neogrid_sellout) -> DataFrame:

        """ 
        """

        # rename all columns to upper case
        for column in df_neogrid_sellout.columns:
            df_neogrid_sellout = df_neogrid_sellout.withColumnRenamed(column, column.upper())

        df_neogrid_sellout = df_neogrid_sellout.where(col("DIA") != 'Dia')

        df_neogrid_sellout = df_neogrid_sellout.withColumn(
            "DIA",
            when(
                to_date(
                    from_unixtime(unix_timestamp(df_neogrid_sellout.DIA, "yyyy-MM-dd"))
                ).isNotNull(),
                to_date(from_unixtime(unix_timestamp(df_neogrid_sellout.DIA, "yyyy-MM-dd"))),
            )
            .when(
                to_date(
                    from_unixtime(unix_timestamp(df_neogrid_sellout.DIA, "dd/MM/yyyy"))
                ).isNotNull(),
                to_date(from_unixtime(unix_timestamp(df_neogrid_sellout.DIA, "dd/MM/yyyy"))),
            )
            .otherwise("1900-01-01"),
        )

        return df_neogrid_sellout
    
    def transformation_in_source(self, df_neogrid_sellout) -> DataFrame:

        df_transformed_neogrid_sellout = df_neogrid_sellout.dropDuplicates()

        df_transformed_neogrid_sellout = (
                df_transformed_neogrid_sellout.filter(
                        (df_transformed_neogrid_sellout.QUANTIDADE_VENDA_UNIDADE > lit('0,00')) |
                        (df_transformed_neogrid_sellout.QUANTIDADE_ESTOQUE_UNIDADE >= lit('0,00'))
                    )
                )
        
        window = (
        Window.partitionBy([
                col("CODIGO_VAREJO"), 
                col("DIA"), 
                col("CNPJ"), 
                col("EAN_PRODUTO_VAREJO"), 
                col("DESCRICAO_PRODUTO_VAREJO")
            ]).orderBy(col("ingestion_date").desc())
        )

        df_transformed_neogrid_sellout = df_transformed_neogrid_sellout.withColumn(
            "max_ingestion_date", max(col("ingestion_date")).over(window)
        )

        df_transformed_neogrid_sellout = df_transformed_neogrid_sellout.where(
            col("ingestion_date") == col("ingestion_date_max")
        )

        df_transformed_neogrid_sellout = df_transformed_neogrid_sellout.drop(
            col("max_ingestion_date")
        )

        return df_transformed_neogrid_sellout

    # This method is mandatory and the final transformation of your dataframe must be returned here
    def definitions(self):

        current_datetime = self.get_datetime()

        is_first_execution = self.is_first_execution_by_date(current_datetime)

        if is_first_execution:

            # extract
            df_extracted_atacadao_sales = self.extract_silver_neogrid_sales(
                current_datetime, True
            )

            df_selected_atacadao_sales = self.select_columns_neogrid_sales(
                df_extracted_atacadao_sales)

        # Apply your transformation and return your final dataframe
        return df_extracted_atacadao_sales, df_selected_atacadao_sales

# COMMAND ----------

sellout_task = Sellout()

# COMMAND ----------

df_extracted, df_selected = sellout_task.definitions()

# COMMAND ----------

df_selected.count()

# COMMAND ----------



# COMMAND ----------

df_selected.count()

# COMMAND ----------



# COMMAND ----------





# COMMAND ----------




# COMMAND ----------



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

df_selected.where("""
                  CODIGO_VAREJO == 864 AND 
                  DIA == '2024-06-28' AND 
                  CNPJ == '05789313000780' AND 
                  EAN_PRODUTO_VAREJO == '7891991298476'
                  """).display()

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


