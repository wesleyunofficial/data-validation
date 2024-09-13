# Databricks notebook source
# Databricks notebook source
import json

from engineeringstore.core.transformation.task.task_entrypoint import TaskEntryPoint
from engineeringstore.core.transformation.transformation import Transformation


# Main class for your transformation
class RedeMateusOver(Transformation):
    def __init__(self):
        super().__init__(
            dependencies = [
            ]
        )

    # This method is mandatory and the final transformation of your dataframe must be returned here
    def definitions(self):
        # Apply your transformation and return your final dataframe
        df_example = self.reference_date # self.get_table("my_example")
        return df_example

# COMMAND ----------

objeto = RedeMateusOver()

# COMMAND ----------

objeto.definitions()

# COMMAND ----------


