# Databricks notebook source
# Databricks notebook source
import json

from engineeringstore.core.transformation.task.task_entrypoint import TaskEntryPoint
from engineeringstore.core.transformation.transformation import Transformation
from pyspark.sql.window import Window
from pyspark.sql.functions import col, concat_ws, max, regexp_replace, row_number
from datetime import date, datetime, timedelta
from pytz import timezone
from pyspark.sql import DataFrame
from pyspark.sql.types import FloatType

# Main class for your transformation
class Sellout(Transformation):
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
        dates = [date(2024, 8, 26), date(2024, 8, 27), date(2024, 8, 28)]

        if current_datetime.date() in dates:
            return True
        else:
            return False
        
    def extract_silver_condor_sellout(
        self, current_datetime:datetime, is_first_load:bool
    ) -> DataFrame:
        """
            Extract data from table brewdat_uc_saz_prod.slv_saz_sales_condor.br_sellout
        Returns:
            _type_: dataframe spark
        """

        current_date = current_datetime.date() - timedelta(days=3)

        df_condor_sellout = self.get_table(
            "brewdat_uc_saz_prod.slv_saz_sales_condor.br_sellout"
        )

        # Create a new column ingestion date with the year, month and day
        df_condor_sellout = df_condor_sellout.withColumn(
            "ingestion_date",
            concat_ws("-", col("year"), col("month"), col("day")).cast("date"),
        )

        if is_first_load:
            return df_condor_sellout
        else:
            df_condor_sellout = df_condor_sellout.where(
                df_condor_sellout.ingestion_date >= (current_date - timedelta(weeks=1))
            )

            return df_condor_sellout
        
    def extract_gold_condor_sellout(self) -> DataFrame:

        df_condor_sellout = self.get_table("brewdat_uc_saz_prod.gld_saz_sales_condor.sellout")

        return df_condor_sellout
    
    def merge_dataframes(
        self, df_gold_condor_sellout: DataFrame, df_silver_condor_sellout: DataFrame
    ) -> DataFrame:
        
        df_condor_sellout_merged = df_gold_condor_sellout.union(
            df_silver_condor_sellout
        )

        return df_condor_sellout_merged

    
    def select_columns_condor_sellout(self, df_condor_sellout: DataFrame) -> DataFrame:
        """
            - select only the required columns
            - rename them if necessary
            - add ingestion date column using columns year, month and day
        Returns:
            df_condor_sellout (spark.DataFrame): Transformed dataframe.
        """

        df_condor_sellout_selected = (
            df_condor_sellout.withColumn('VOLUME_UNIT', regexp_replace('VOLUME_UNIT', ',', '.').cast(FloatType()))
                             .withColumn('PACK_QUANTITY', regexp_replace('PACK_QUANTITY', ',', '.').cast(FloatType()))
                             .withColumn('FATURAMENTO', regexp_replace('FATURAMENTO', ',', '.').cast(FloatType()))
        )

        # Drop columns year, month and day
        df_condor_sellout_selected = df_condor_sellout_selected.drop(
            "year", "month", "day"
        )

        # Rename columns for uppercase
        df_condor_sellout_selected = df_condor_sellout_selected.toDF(
            *[c.strip().upper() for c in df_condor_sellout_selected.columns]
        )
        
        return df_condor_sellout_selected

    def apply_transformation_in_source(self, df_condor_sellout: DataFrame) -> DataFrame:
        """
        """
        
        window = Window.partitionBy(
            ['DATA_VENDA','CNPJ_LOJA','EAN', 'PRODUCT_SOURCE_NAME', 'VOLUME_UNIT', 'MEA_UNIT', 'PACK_QUANTITY']
        ).orderBy(col("DATA_VENDA").desc())

        df_condor_sellout = df_condor_sellout.withColumn(
            "row_number", row_number().over(window)
        )

        df_condor_sellout = df_condor_sellout.filter(
            col("row_number") == 1
        )

        df_condor_sellout = df_condor_sellout.drop("row_number")

        return df_condor_sellout
    
    def apply_transformation_in_destination(df_condor_sellout: DataFrame) -> DataFrame:

        window = Window.partitionBy(
            ['DATA_VENDA','CNPJ_LOJA','EAN', 'PRODUCT_SOURCE_NAME', 'VOLUME_UNIT', 'MEA_UNIT', 'PACK_QUANTITY']
        ).orderBy(col("INGESTION_DATE").desc())

        df_condor_sellout_transformed = df_condor_sellout.withColumn(
            "max_ingestion_date", max(col("INGESTION_DATE")).over(window)
        )

        df_condor_sellout_transformed = df_condor_sellout_transformed.filter(
            col("INGESTION_DATE") == col("max_ingestion_date")
        )

        df_condor_sellout_transformed = df_condor_sellout_transformed.drop("max_ingestion_date")

        df_condor_sellout_transformed = df_condor_sellout_transformed.dropDuplicates()

        return df_condor_sellout_transformed

    # This method is mandatory and the final transformation of your dataframe must be returned here
    def definitions(self):
        # Apply your transformation and return your final dataframe
        current_datetime = self.get_datetime()

        is_first_load = self.is_first_execution_by_date(current_datetime)

        if is_first_load:

            # extract silver table
            df_condor_sellout_extracted = self.extract_silver_condor_sellout(
                current_datetime, True
            )

            # select columns from silver table
            df_condor_sellout_selected = self.select_columns_condor_sellout(
                df_condor_sellout_extracted
            )

            # apply transformation in source
            df_condor_sellout_final = self.apply_transformation_in_source(
                df_condor_sellout_selected
            )
        else:            
            
            # extract gold table
            df_gold_condor_sellout = self.extract_gold_condor_sellout()

            # extract silver table
            df_condor_sellout_extracted = self.extract_silver_condor_sellout(
                current_datetime, False
            )

            # select columns from silver table
            df_condor_sellout_selected = self.select_columns_condor_sellout(
                df_condor_sellout_extracted
            )

            # apply transformation in source
            df_condor_sellout_transformed = self.apply_transformation_in_source(
                df_condor_sellout_selected
            )

            # merge gold and silver dataframes
            df_condor_sellout_merged = self.merge_dataframes(
                df_gold_condor_sellout, df_condor_sellout_transformed
            )

            # apply transformation in destination
            df_condor_sellout_final = self.apply_transformation_in_destination(
                df_condor_sellout_merged
            )

        return df_condor_sellout_final

# COMMAND ----------

sellout = Sellout()

df_condor = sellout.definitions()

# COMMAND ----------

import json
def return_metadata(dataframe):
  schema_json = dataframe.schema.json()
 
  schemas = json.loads(schema_json)

  for schema in schemas['fields']:
    print(f"- name: '{schema['name']}'\n  description: ''\n  type: '{schema['type']}'")

# COMMAND ----------

return_metadata(df_condor)

# COMMAND ----------


