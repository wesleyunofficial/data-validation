# Databricks notebook source
# Databricks notebook source
import json

from engineeringstore.core.transformation.task.task_entrypoint import TaskEntryPoint
from engineeringstore.core.transformation.transformation import Transformation
from engineeringstore.infrastructure.common.secrets_manager import Secret

from pyspark.sql.functions import when, lit, lower, trim, col, udf, length, concat, upper, regexp_replace, collect_list
from pyspark.sql import functions as f
from pyspark.sql import types as t
from pyspark.sql.window import Window
from dateutil import relativedelta
import datetime

from functools import reduce

# from pyiris.ingestion.extract import FileReader
# from pyiris.infrastructure import Spark as pyiris_spark
# from pyiris.infrastructure.common.config import get_key
# from pyiris.ingestion.config.file_system_config import FileSystemConfig
# from pyiris.ingestion.load import FileWriter
from pyspark.sql.types import StringType, StructType, StructField
import os

import numpy as np
import json

import pandas as pd
import time

import re
import requests
import sys

from sklearn.feature_extraction.text import TfidfTransformer, TfidfVectorizer
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.neighbors import NearestNeighbors


# Main class for your transformation
class CleasingGpt(Transformation):
    def __init__(self):
        super().__init__(dependencies=[])

    # This method is mandatory and the final transformation of your dataframe must be returned here
    def definitions(self):
        # Apply your transformation and return your final dataframe
        df_example = self.get_table("my_example")
        return df_example

# COMMAND ----------

from engineeringstore.infrastructure.common.secrets_manager import Secret

iris_devops_token = Secret("iris-azure-devops-project-teams-token").value

%pip install asimov-genai --extra-index-url https://ambevtech-corporate-python:$iris_devops_token@pkgs.dev.azure.com/AMBEV-SA/_packaging/ambevtech-corporate-python/pypi/simple

dbutils.library.restartPython()

# COMMAND ----------



# COMMAND ----------


