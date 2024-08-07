# Databricks notebook source
# Databricks notebook source
import json

from engineeringstore.core.transformation.task.task_entrypoint import TaskEntryPoint
from engineeringstore.core.transformation.transformation import Transformation


# Main class for your transformation
class Mastertable(Transformation):
    def __init__(self):
        super().__init__(
            dependencies = [
            ]
        )

    def get_silver_table(self):
        return self.get_table("brewdat_uc_saz_prod.brz_saz_sales_manual_mastertable.br_master_table")

    # This method is mandatory and the final transformation of your dataframe must be returned here
    def definitions(self):
        # Apply your transformation and return your final dataframe
        df_example = self.get_silver_table()
        return df_example
