# Databricks notebook source
# Databricks notebook source
import json

from engineeringstore.core.transformation.task.task_entrypoint import TaskEntryPoint
from engineeringstore.core.transformation.transformation import Transformation


# Main class for your transformation
class SelloutTestData(Transformation):
    def __init__(self):
        super().__init__(
            dependencies = [
            ]
        )

    def extract_date(self):
        return self.get_table("brewdat_uc_saz_prod.slv_saz_sales_rede_mateus.br_sellout")
    
    def transform(self, df):
        return df
    
    def load(self, df):
        return df

    # This method is mandatory and the final transformation of your dataframe must be returned here
    def definitions(self):
        # Apply your transformation and return your final dataframe
        df_extracted = self.extract_date()

        df_transformed = self.transform(df_extracted)

        df_loaded = self.load(df_transformed)
       
        return df_loaded

# COMMAND ----------

sellout_test_date = SelloutTestData()

# COMMAND ----------

df = sellout_test_date.definitions()

# COMMAND ----------


