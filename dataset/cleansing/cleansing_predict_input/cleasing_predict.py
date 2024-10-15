# Databricks notebook source
# Databricks notebook source
import json

from engineeringstore.core.transformation.task.task_entrypoint import TaskEntryPoint
from engineeringstore.core.transformation.transformation import Transformation

from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    LongType,
    StringType,
    StructType,
    StructField,
)
from pyspark.sql.functions import (
    col,
    concat,
    explode,
    lit,
    round,
    udf,
    when,
)
import re
from pyspark.sql import DataFrame

import difflib

spark.conf.set("spark.sql.execution.arrow.enabled", "false")


# Main class for your transformation
class CleansingPredictInput(Transformation):  #
    def __init__(self):
        super().__init__(dependencies=[])

    def drop_columns(self, df, list_columns):
        """
        Remove specified columns from a DataFrame.

        This function takes a DataFrame and a list of columns, returning a new
        version of the DataFrame without the specified columns.

        Parameters:
        -----------
        df : DataFrame
            The DataFrame from which columns will be removed.
        list_columns : list
            List of columns to be dropped from the DataFrame.

        Returns:
        --------
        DataFrame
            A new version of the DataFrame without the specified columns.
        """
        return df.drop(*list_columns)

    def explode_column(self, df_to_explode, column_to_explode, alias_column):
        """
        Explodes a specified column in a DataFrame and assigns an alias to the resulting column.

        This function takes a DataFrame, explodes the specified column, and renames
        the exploded column with the given alias. It retains all other columns in the DataFrame.

        Parameters:
        -----------
        df_to_explode : DataFrame
            The DataFrame that contains the column to be exploded.
        column_to_explode : str
            The name of the column to be exploded. The column must contain an array or map.
        alias_column : str
            The alias for the new exploded column.

        Returns:
        --------
        DataFrame
            A new DataFrame with the exploded column, along with all other columns.
        """
        return df_to_explode.select(
            explode(col(column_to_explode)).alias(alias_column), "*"
        )

    def process_master_table(self) -> DataFrame:
        """
        Processes the master sales table by applying transformations.

        This function reads the historical sales master table from a specified location,
        performs data transformations such as dropping unnecessary columns and renaming
        a column, and returns the processed DataFrame.

        Steps performed:
        ----------------
        1. Reads the master table from the specified source.
        2. Drops the columns 'year', 'month', 'day', 'ID', and 'FRIENDLY_NAME'.
        3. Renames the column 'PRODUCT_SOURCE_NAME' to 'PRODUCT_SOURCE_NAME_MASTER'.

        Returns:
        --------
        DataFrame
            The transformed DataFrame after dropping columns and renaming.
        """
        # read
        table_name = "brewdat_uc_saz_prod.br_historical_sales.cz_mastertable"
        df_master_table = self.get_table(table_name)

        # transform

        # drop unnecessary columns
        df_master_table = self.drop_columns(
            df_master_table, ["year", "month", "day", "ID", "FRIENDLY_NAME"]
        )

        # rename column source name
        df_master_table = df_master_table.withColumnRenamed(
            "PRODUCT_SOURCE_NAME", "PRODUCT_SOURCE_NAME_MASTER"
        )

        return df_master_table

    def process_master_table_generic(self) -> DataFrame:
        """
        Processes the master generic sales table by applying transformations.

        This function reads the historical sales master table from a specified location,
        performs data transformations such as dropping unnecessary columns and renaming
        a column, and returns the processed DataFrame.

        Steps performed:
        ----------------
        1. Reads the master table from the specified source.
        2. Drops the columns 'year', 'month', 'day', 'ID', and 'FRIENDLY_NAME'.
        3. Renames the column 'PRODUCT_SOURCE_NAME' to 'PRODUCT_SOURCE_NAME_MASTER'.

        Returns:
        --------
        DataFrame
            The transformed DataFrame after dropping columns and renaming.
        """

        table_name = "brewdat_uc_saz_prod.br_historical_sales.cz_mastertable_generic"
        df_master_table_generic = self.get_table(table_name)

        # transform
        schema_sellin = StructType(
            [
                StructField("COD_PROD", IntegerType(), True),
                StructField("COD_ABREV_PROD", IntegerType(), True),
                StructField("NOM_PROD", StringType(), False),
                StructField("CESTA_OFICIAL", IntegerType(), False),
                StructField("EAN_PACK", LongType(), False),
                StructField("EAN_UNIT", LongType(), False),
                StructField("INNOVATION", StringType(), False),
                StructField("FUTURE_BEVS", StringType(), False),
                StructField("COD_PROD_GERENCIAL_VENDAS", IntegerType(), False),
                StructField("NOM_PROD_GERENCIAL_VENDAS", StringType(), False),
            ]
        )

        # drop unnecessary columns
        df_master_table_generic = self.drop_columns(
            df_master_table_generic, ["year", "month", "day", "ID", "FRIENDLY_NAME"]
        )

        # add columns
        df_master_table_generic = df_master_table_generic.withColumn(
            "AUTO_FILL_SELLIN", lit(None)
        ).withColumn("MATCH_INTERNAL_PRODUCT", lit(None).cast(ArrayType(schema_sellin)))

        # rename column from PRODUCT_SOURCE_NAME to PRODUCT_SOURCE_NAME_MASTER
        df_master_table_generic = df_master_table_generic.withColumnRenamed(
            "PRODUCT_SOURCE_NAME", "PRODUCT_SOURCE_NAME_MASTER"
        )

        return df_master_table_generic

    def process_distinct_products(self) -> DataFrame:
        """
        Processes the distinct products table by applying transformations.

        This function reads the distinct products table from the specified source and performs
        data transformations. It fills null values in the 'UNIDADES_CONTENIDO' and 'MED_CANT_CONTENIDO'
        columns with a default value of 1, under specific conditions where 'SOURCE_NAME' is 'SCANNTECH'
        and 'SUBSOURCE_NAME' is 'DIAFULL'.

        Steps performed:
        ----------------
        1. Reads the distinct products table from the specified source.
        2. Fills null values in 'UNIDADES_CONTENIDO' where 'SOURCE_NAME' is 'SCANNTECH' and
        'SUBSOURCE_NAME' is 'DIAFULL' with 1.
        3. Fills null values in 'MED_CANT_CONTENIDO' under the same conditions with 1.

        Returns:
        --------
        DataFrame
            The transformed DataFrame after applying the null value replacements.
        """
        # read
        table_name = (
            "brewdat_uc_saz_prod.gld_saz_sales_distinct_products.distinct_products"
        )
        df_distinct_products = self.get_table(table_name)
        # end read

        # transform
        df_distinct_products = df_distinct_products.withColumn(
            "UNIDADES_CONTENIDO",
            when(
                (col("SOURCE_NAME") == "SCANNTECH")
                & (col("SUBSOURCE_NAME") == "DIAFULL")
                & col("UNIDADES_CONTENIDO").isNull(),
                1,
            ).otherwise(col("UNIDADES_CONTENIDO")),
        ).withColumn(
            "MED_CANT_CONTENIDO",
            when(
                (col("SOURCE_NAME") == "SCANNTECH")
                & (col("SUBSOURCE_NAME") == "DIAFULL")
                & col("MED_CANT_CONTENIDO").isNull(),
                1,
            ).otherwise(col("MED_CANT_CONTENIDO")),
        )
        # end transform

        return df_distinct_products

    def process_distinct_treated(self) -> DataFrame:
        """
        Retrieves the distinct treated products table.

        This function reads the distinct treated products table from the specified source
        and returns it as a DataFrame without applying any transformations.

        Steps performed:
        ----------------
        1. Reads the distinct treated products table from the specified source.

        Returns:
        --------
        DataFrame
            The distinct treated products DataFrame.
        """
        table_name = "brewdat_uc_saz_prod.gld_saz_sales_distinct_products.distinct_products_treated"
        df_distinct_treated = self.get_table(table_name)

        return df_distinct_treated

    def process_master_treated(self) -> DataFrame:
        """
        Processes the treated master table by dropping unnecessary columns.

        This function reads the treated master table from the specified source, removes
        the columns 'brand_description' and 'number_description', and returns the processed DataFrame.

        Steps performed:
        ----------------
        1. Reads the treated master table from the specified source.
        2. Drops the columns 'brand_description' and 'number_description'.

        Returns:
        --------
        DataFrame
            The treated master DataFrame after dropping the specified columns.
        """
        table_name = (
            "brewdat_uc_saz_mlp_featurestore_prod.sales.cleansing_master_treated"
        )
        df_master_treated = self.get_table(table_name)

        df_master_treated = self.drop_columns(
            df_master_treated, ["brand_description", "number_description"]
        )

        return df_master_treated

    def match_by_product(
        self, df_master_table, df_distinct_products, is_match_by_ean
    ) -> DataFrame:
        """
        Matches products between the master table and distinct products table based on EAN or description.

        This function matches products from the master table to the distinct products table
        either by using the EAN code or by matching product names, depending on the value of
        `is_match_by_ean`. The function performs an inner join and processes the matched records,
        adding a new column indicating the match type and removing duplicates.

        Parameters:
        -----------
        df_master_table : DataFrame
            The master table DataFrame containing product data.
        df_distinct_products : DataFrame
            The distinct products DataFrame containing the list of unique products.
        is_match_by_ean : bool
            If True, the match is performed by EAN (using 'PRODUCT_SOURCE_CODE').
            If False, the match is performed by product name (using 'PRODUCT_SOURCE_NAME').

        Returns:
        --------
        DataFrame
            A DataFrame with matched products, including a 'PRODUCT_MATCH_TYPE' column
            indicating whether the match was made by EAN or description. Duplicates are removed.
        """
        df_match = df_distinct_products.select(
            "PRODUCT_ID",
            "SUBSOURCE_NAME",
            "SOURCE_NAME",
            "BRAND",
            "PRODUCT_SOURCE_NAME",
            "PRODUCT_SOURCE_CODE",
            "MED_CANT_CONTENIDO",
            "UNIDADES_CONTENIDO",
        )

        if is_match_by_ean:
            join_keys = df_master_table.EAN_EX == df_match.PRODUCT_SOURCE_CODE
            df_match = df_match.filter("PRODUCT_SOURCE_CODE != 0")
        else:
            join_keys = df_master_table.SOURCE_EX == df_match.PRODUCT_SOURCE_NAME

        df_match = df_match.join(df_master_table, join_keys, "inner")

        if is_match_by_ean:
            # Add 'PRODUCT_MATCH_TYPE' column
            df_match = df_match.withColumn("PRODUCT_MATCH_TYPE", lit("EAN"))

            # Remove duplicates and drop 'EAN_EX' column
            df_match = df_match.dropDuplicates(subset=["PRODUCT_ID"]).drop("EAN_EX")
        else:
            # Add 'PRODUCT_MATCH_TYPE' column
            df_match = df_match.withColumn("PRODUCT_MATCH_TYPE", lit("DESCRICAO"))

            # Remove duplicates and drop 'SOURCE_EX' column
            df_match = df_match.dropDuplicates(subset=["PRODUCT_ID"]).drop("SOURCE_EX")

        return df_match

    def match_by_ean(self, df_master_table, df_distinct_products) -> DataFrame:
        """
        Matches products between the master table and distinct products table based on EAN

        This function matches products from the master table to the distinct products table
        either by using the EAN code. The function performs an inner join and processes the matched records,
        adding a new column indicating the match type and removing duplicates.

        Parameters:
        -----------
        df_master_table : DataFrame
            The master table DataFrame containing product data.
        df_distinct_products : DataFrame
            The distinct products DataFrame containing the list of unique products.

        Returns:
        --------
        DataFrame
            A DataFrame with matched products, including a 'PRODUCT_MATCH_TYPE' column
            indicating whether the match was made by EAN. Duplicates are removed.
        """

        # Seleciona as colunas e faz o filtro
        df_ean_match = (
            df_distinct_products.alias("DistinctInput")
            .select(
                "PRODUCT_ID",
                "SUBSOURCE_NAME",
                "SOURCE_NAME",
                "BRAND",
                "PRODUCT_SOURCE_NAME",
                "PRODUCT_SOURCE_CODE",
                "MED_CANT_CONTENIDO",
                "UNIDADES_CONTENIDO",
            )
            .filter("PRODUCT_SOURCE_CODE != 0")
        )

        join_keys = df_master_table.EAN_EX == df_ean_match.PRODUCT_SOURCE_CODE

        # Faz o join com MasterOriginalExplode
        df_ean_match = df_ean_match.join(df_master_table, join_keys, "inner")

        # Adiciona a coluna 'PRODUCT_MATCH_TYPE'
        df_ean_match = df_ean_match.withColumn("PRODUCT_MATCH_TYPE", lit("EAN"))

        # Remove duplicatas e a coluna 'EAN_EX'
        df_ean_match = df_ean_match.dropDuplicates(subset=["PRODUCT_ID"]).drop("EAN_EX")

        return df_ean_match

    def match_by_description(self, df_master_table, df_distinct_products) -> DataFrame:
        """
        Matches products between the master table and distinct products table based on description.

        This function matches products from the master table to the distinct products table
        either by matching product names.
        The function performs an inner join and processes the matched records,
        adding a new column indicating the match type and removing duplicates.

        Parameters:
        -----------
        df_master_table : DataFrame
            The master table DataFrame containing product data.
        df_distinct_products : DataFrame
            The distinct products DataFrame containing the list of unique products.

        Returns:
        --------
        DataFrame
            A DataFrame with matched products, including a 'PRODUCT_MATCH_TYPE' column
            indicating whether the match was made by description. Duplicates are removed.
        """

        # Seleciona as colunas relevantes e realiza o join
        df_description_match = (
            df_distinct_products.alias("DistinctInput")
            .select(
                "PRODUCT_ID",
                "SUBSOURCE_NAME",
                "SOURCE_NAME",
                "BRAND",
                "PRODUCT_SOURCE_NAME",
                "PRODUCT_SOURCE_CODE",
                "MED_CANT_CONTENIDO",
                "UNIDADES_CONTENIDO",
            )
            .join(
                df_master_table.alias("MasterOriginalExplodeSourceName"),
                col("MasterOriginalExplodeSourceName.SOURCE_EX")
                == col("DistinctInput.PRODUCT_SOURCE_NAME"),
                "inner",
            )
        )

        # Adiciona a coluna 'PRODUCT_MATCH_TYPE'
        df_description_match = df_description_match.withColumn(
            "PRODUCT_MATCH_TYPE", lit("DESCRICAO")
        )

        # Remove duplicatas e a coluna 'SOURCE_EX'
        df_description_match = df_description_match.dropDuplicates(
            subset=["PRODUCT_ID"]
        ).drop("SOURCE_EX")

        return df_description_match

    def update_match_columns(self, df_match) -> DataFrame:
        """
        Atualiza e transforma várias colunas no DataFrame 'EanMatch' de acordo com as condições sobre 'SOURCE_NAME' e 'SUBSOURCE_NAME'.
        Faz operações de atualização condicional, conversões de tipo e remoção de colunas.

        :param df: DataFrame PySpark contendo as colunas a serem atualizadas.
        :return: DataFrame transformado com colunas modificadas e removidas.
        """
        # Atualiza a coluna 'VOLUME_TOTAL'
        df_match = df_match.withColumn(
            "VOLUME_TOTAL",
            when(
                (col("SOURCE_NAME") == "SCANNTECH")
                & (col("SUBSOURCE_NAME") == "DIAFULL"),
                col("MED_CANT_CONTENIDO"),
            ).otherwise(col("VOLUME_TOTAL")),
        )

        # Atualiza a coluna 'PACK_QUANTITY'
        df_match = df_match.withColumn(
            "PACK_QUANTITY",
            when(
                (col("SOURCE_NAME") == "SCANNTECH")
                & (col("SUBSOURCE_NAME") == "DIAFULL"),
                col("UNIDADES_CONTENIDO"),
            ).otherwise(col("PACK_QUANTITY")),
        )

        # Converte 'MED_CANT_CONTENIDO' e 'UNIDADES_CONTENIDO' para int
        df_match = df_match.withColumn(
            "MED_CANT_CONTENIDO", col("MED_CANT_CONTENIDO").cast("int")
        ).withColumn("UNIDADES_CONTENIDO", col("UNIDADES_CONTENIDO").cast("int"))

        # Cria a coluna 'VOLUME_CONTENIDO' arredondada
        df_match = df_match.withColumn(
            "VOLUME_CONTENIDO",
            round(col("MED_CANT_CONTENIDO") / col("UNIDADES_CONTENIDO"), 0),
        )

        # Converte 'VOLUME_CONTENIDO' para long
        df_match = df_match.withColumn(
            "VOLUME_CONTENIDO", col("VOLUME_CONTENIDO").cast("long")
        )

        # Atualiza a coluna 'VOLUME_UNIT'
        df_match = df_match.withColumn(
            "VOLUME_UNIT",
            when(
                (col("SOURCE_NAME") == "SCANNTECH")
                & (col("SUBSOURCE_NAME") == "DIAFULL"),
                col("VOLUME_CONTENIDO"),
            ).otherwise(col("VOLUME_UNIT")),
        )

        # Remove as colunas desnecessárias
        df_match = df_match.drop(
            "MED_CANT_CONTENIDO", "VOLUME_CONTENIDO", "UNIDADES_CONTENIDO"
        )

        return df_match

    def remove_matching_rows(
        self, df_distinct_products, df_match, select_columns, join_keys
    ) -> DataFrame:
        """
        Removes rows from the distinct products table that match rows in another DataFrame.

        This function performs a left anti join between the `df_distinct_products` DataFrame
        and the `df_match` DataFrame. It removes rows from `df_distinct_products` where the
        specified join keys match the selected columns in `df_match`.

        Parameters:
        -----------
        df_distinct_products : DataFrame
            The DataFrame containing distinct products.
        df_match : DataFrame
            The DataFrame containing matched products to be excluded from `df_distinct_products`.
        select_columns : list
            List of columns from `df_match` to use in the join for filtering.
        join_keys : list
            List of keys to perform the join on between the two DataFrames.

        Returns:
        --------
        DataFrame
            A DataFrame containing only rows from `df_distinct_products` that do not match
            rows in `df_match` based on the specified join keys.
        """
        return df_distinct_products.join(
            df_match.select(select_columns), join_keys, how="left_anti"
        )

    def treated_match_by_description(
        self,
        df_distinct_treated,
        df_master_treated,
        df_master_table,
        df_ean_match,
        df_ean_match_generic,
        df_description_match,
        df_description_match_generic,
    ) -> DataFrame:
        """
        Matches products by treated description and removes previously matched rows.

        This function matches products from the treated distinct products DataFrame (`df_distinct_treated`)
        to the treated master products DataFrame (`df_master_treated`) using a treated product description.
        After the match, it renames columns, adds a match type column, and joins the result with the original
        master table. It then removes rows that have already been matched by EAN or description, including generic matches.

        Parameters:
        -----------
        df_distinct_treated : DataFrame
            The DataFrame containing treated distinct products.
        df_master_treated : DataFrame
            The DataFrame containing treated master products for matching.
        df_master_table : DataFrame
            The master table DataFrame for joining after the match.
        df_ean_match : DataFrame
            DataFrame with products already matched by EAN.
        df_ean_match_generic : DataFrame
            DataFrame with products already matched by generic EAN.
        df_description_match : DataFrame
            DataFrame with products already matched by description.
        df_description_match_generic : DataFrame
            DataFrame with products already matched by generic description.

        Returns:
        --------
        DataFrame
            A DataFrame containing products matched by treated description, with rows
            already matched by EAN or description removed.
        """
        # Match by treated description
        treated_match = df_distinct_treated.select(
            "product_id",
            "source_table",
            "source_name",
            "unidades_contenido",
            "med_cant_contenido",
            "brand_original",
            "product_original",
            "product_source_code",
            "product_treated",
        ).join(
            df_master_treated.select("product_description", "product_treated"),
            on="product_treated",
        )

        # Rename columns and join with master table
        treated_match = (
            treated_match.dropDuplicates(subset=["product_id"])
            .withColumnRenamed("product_id", "PRODUCT_ID")
            .withColumnRenamed("source_table", "SUBSOURCE_NAME")
            .withColumnRenamed("unidades_contenido", "UNIDADES_CONTENIDO")
            .withColumnRenamed("med_cant_contenido", "MED_CANT_CONTENIDO")
            .withColumnRenamed("source_name", "SOURCE_NAME")
            .withColumnRenamed("brand_original", "BRAND")
            .withColumnRenamed("product_original", "PRODUCT_SOURCE_NAME")
            .withColumnRenamed("product_source_code", "PRODUCT_SOURCE_CODE")
            .withColumn("PRODUCT_MATCH_TYPE", F.lit("DESCRICAO_TRATADA"))
            .join(df_master_table, on="PRODUCT_DESCRIPTION")
            .drop("product_treated")
        )

        # Remove rows already matched by EAN or description
        treated_match = (
            treated_match.join(df_ean_match, on="PRODUCT_ID", how="left_anti")
            .join(df_ean_match_generic, on="PRODUCT_ID", how="left_anti")
            .join(df_description_match, on="PRODUCT_ID", how="left_anti")
            .join(df_description_match_generic, on="PRODUCT_ID", how="left_anti")
        )

        return treated_match

    def union_all_matches(
        self,
        df_ean_match,
        df_description_match,
        df_ean_match_generic,
        df_description_match_generic,
        df_treated_match,
    ) -> DataFrame:
        """
        Unions all matched DataFrames into a single DataFrame.

        This function combines multiple DataFrames containing product matches, including matches by EAN,
        description, and treated matches. It performs a union operation on all specified DataFrames and
        removes duplicate entries based on the 'PRODUCT_ID' column.

        Parameters:
        -----------
        df_ean_match : DataFrame
            DataFrame containing products matched by EAN.
        df_description_match : DataFrame
            DataFrame containing products matched by description.
        df_ean_match_generic : DataFrame
            DataFrame containing products matched by generic EAN.
        df_description_match_generic : DataFrame
            DataFrame containing products matched by generic description.
        df_treated_match : DataFrame
            DataFrame containing products matched by treated description.

        Returns:
        --------
        DataFrame
            A DataFrame that includes all unique product matches from the specified DataFrames,
            without duplicates based on 'PRODUCT_ID'.
        """
        # Union all direct matches
        df_direct_match = (
            df_ean_match.unionByName(df_description_match)
            .unionByName(df_ean_match_generic)
            .unionByName(df_description_match_generic)
            .unionByName(df_treated_match)
        )

        # Remove duplicates based on 'PRODUCT_ID'
        df_direct_match = df_direct_match.dropDuplicates(subset=["PRODUCT_ID"])

        return df_direct_match

    def add_unidade(self, df) -> DataFrame:
        """
        Appends '1unidade' to product descriptions lacking unit information.

        This function checks the 'product_description_treated' column for each row in the
        DataFrame. If the description already contains the term 'unidade', it remains unchanged.
        Otherwise, ' 1unidade' is concatenated to the end of the description.

        Parameters:
        -----------
        df : DataFrame
            The DataFrame containing product descriptions to be processed.

        Returns:
        --------
        DataFrame
            The modified DataFrame with updated product descriptions where necessary.
        """
        df = df.withColumn(
            "product_description_treated",
            when(
                df.product_description_treated.like("%unidade%"),
                df.product_description_treated,
            ).otherwise(concat(df.product_description_treated, lit(" 1unidade"))),
        )

        return df

    def pre_process_master_treated(self, df_master_treated) -> DataFrame:
        """
        Pre-processes the treated master DataFrame by selecting specific columns and removing duplicates.

        This function takes a DataFrame containing treated master product data, selects relevant columns,
        removes duplicate entries based on the 'product_description_treated' column, and appends
        '1unidade' to product descriptions that lack unit information.

        Parameters:
        -----------
        df_master_treated : DataFrame
            The DataFrame containing treated master product data to be pre-processed.

        Returns:
        --------
        DataFrame
            The processed DataFrame with selected columns, duplicates removed, and updated product descriptions.
        colunas_master = ['product_description',
                  'product_description_treated',
                  'category',
                  'brand_id',
                  'family',
                  'volume_unit',
                  'pack_quantity',
                  'package_unit']

        df_master_treated = df_master_treated.select(colunas_master)"""
        df_master_treated = df_master_treated.dropDuplicates(
            ["product_description_treated"]
        )

        # Appends '1unidade' to product descriptions lacking unit information
        df_master_treated = self.add_unidade(df_master_treated)

        return df_master_treated

    def create_list_of_words(self, master):
        """
        Creates a list of words from the master DataFrame and adds some key terms.

        This function extracts product descriptions from the specified column in the master DataFrame,
        concatenates them into a single string, splits the string into individual words, and appends
        a predefined list of key terms. Finally, it removes duplicates and returns a list of unique words.

        Parameters:
        -----------
        master : DataFrame
            The DataFrame containing product descriptions to be processed.

        Returns:
        --------
        list
            A list of unique words derived from the product descriptions and additional key terms.
        """
        descriptions = list(master.select("product_description_treated").collect())
        descriptions = [phrase[0] for phrase in descriptions]

        master_words = " ".join(descriptions)
        master_words = master_words.split(" ")

        master_words = master_words + [
            "do bem",
            "ln",
            "ow",
            "budweiser",
            "bohemia",
            "brahma",
            "skol",
            "antarctica",
            "wals",
            "beats",
            "pilsen",
            "ml",
            "misto",
            "garrafa",
        ]

        master_words = [word for word in master_words if len(word) > 1]

        master_words = list(set(master_words))
        return master_words

    @staticmethod
    def create_number_data(text):
        """
        Extracts only numerical information or package-related terms from the input text.

        This function processes the provided text and returns a string containing only words that
        are numerical or match predefined package terms. If the input text is None or empty,
        the function returns None.

        Parameters:
        -----------
        text : str
            The input text from which to extract numerical information or package terms.

        Returns:
        --------
        str or None
            A string of extracted numerical and package-related words, or None if the input is empty.
        """
        if not text:
            return None  # Check if the text is None or empty

        regexp = re.compile(r"\d")
        packages = ("garrafa", "pet", "lt", "barril", "draft")

        text = [
            word for word in text.split() if regexp.search(word) or word in packages
        ]
        text = " ".join(text)

        return text.strip()

    @staticmethod
    def create_brand_data(text):
        """
        Removes numerical information from the input text.

        This function processes the provided text and removes any words that end with 'ml',
        'unidade', or 'unidades'. If the input text is None or empty, the function returns None.

        Parameters:
        -----------
        text : str
            The input text from which to remove numerical information.

        Returns:
        --------
        str or None
            A string with numerical terms removed, or None if the input is empty.
        """
        if not text:
            return None  # Check if the text is None or empty

        text = [
            word
            for word in text.split()
            if not word.endswith("ml")
            and not word.endswith("unidade")
            and not word.endswith("unidades")
        ]
        text = " ".join(text)

        return text.strip()

    def transform_data(self, df_master_treated, df_distinct_treated_cleasing):
        """
        Transforms the distinct treated cleansing DataFrame by correcting typos, adding unit information,
        and creating relevant descriptions for volume, quantity, brand, and category.

        This function performs the following transformations on the provided DataFrames:
        1. Creates a list of words from the master DataFrame.
        2. Corrects potential typographical errors in the product descriptions using the master words as a reference.
        3. Appends '1unidade' to product descriptions that lack unit information.
        4. Creates a 'number_description' column containing only numerical information related to volume and quantity.
        5. Creates a 'brand_description' column containing relevant brand information, excluding unnecessary details.

        Parameters:
        -----------
        df_master_treated : DataFrame
            The DataFrame containing treated master product data used for typo correction and word matching.
        df_distinct_treated_cleasing : DataFrame
            The DataFrame containing distinct treated product descriptions to be transformed.

        Returns:
        --------
        DataFrame
            The transformed DataFrame with updated product descriptions, number descriptions, and brand descriptions.
        """
        # Create list of words present in the master
        master_words = self.create_list_of_words(df_master_treated)

        def fix_typo(text):
            if not text:
                return None  # Check if the text is None or empty

            words_list = text.split(" ")
            final_text = list()
            for word in words_list:
                if word in master_words or "ml" in word or all(map(str.isdigit, word)):
                    final_text.append(word + " ")
                else:
                    try:
                        matched_word = difflib.get_close_matches(
                            word, master_words, 1, cutoff=0.90
                        )[0]
                        final_text.append(matched_word + " ")
                    except IndexError:
                        final_text.append(word)

            final_text = "".join(final_text)
            final_text = final_text.strip()
            return final_text

        # Fix possible typos using the master words list as a reference
        fix_typo_udf = udf(fix_typo, StringType())
        df_distinct_treated_cleasing = df_distinct_treated_cleasing.withColumn(
            "product_description_treated", fix_typo_udf(F.col("product_treated"))
        )

        # Append '1unidade' to product descriptions lacking unit information
        df_distinct_treated_cleasing = self.add_unidade(df_distinct_treated_cleasing)

        # Create descriptions containing only relevant information for volume and quantity (number_description)
        create_number_data_udf = udf(
            CleansingPredictInput.create_number_data, StringType()
        )
        df_distinct_treated_cleasing = df_distinct_treated_cleasing.withColumn(
            "number_description",
            create_number_data_udf(col("product_description_treated")),
        )

        # Create descriptions containing only relevant information for brand, family, and category (brand_description)
        create_brand_data_udf = udf(
            CleansingPredictInput.create_brand_data, StringType()
        )

        df_distinct_treated_cleasing = df_distinct_treated_cleasing.withColumn(
            "brand_description",
            create_brand_data_udf(col("product_description_treated")),
        ).drop("brand_id")

        return df_distinct_treated_cleasing

    # This method is mandatory and the final transformation of your dataframe must be returned here
    def definitions(self):
        # Apply your transformation and return your final dataframe
        # read datasets
        df_master_table = self.process_master_table()

        df_master_table_generic = self.process_master_table_generic()

        df_distinct_products = self.process_distinct_products()

        df_distinct_treated = self.process_distinct_treated()

        df_master_treated = self.process_master_treated()
        # end read datasets

        # ean match
        df_master_table_explode = self.explode_column(df_master_table, "EAN", "EAN_EX")
        df_ean_match = self.match_by_product(
            df_master_table_explode, df_distinct_products, True
        )

        # update match columns
        df_ean_match = self.update_match_columns(df_ean_match)

        # remove rows that already have a match
        select_columns = ["PRODUCT_ID"]
        join_keys = df_distinct_products.PRODUCT_ID == df_ean_match.PRODUCT_ID

        df_distinct_products_generic = self.remove_matching_rows(
            df_distinct_products, df_ean_match, select_columns, join_keys
        )
        # end ean match

        # ean match generic
        df_master_table_generic_explode = self.explode_column(
            df_master_table_generic, "EAN", "EAN_EX"
        )
        df_ean_match_generic = self.match_by_product(
            df_master_table_generic_explode, df_distinct_products_generic, True
        )

        # update match columns (generic)
        df_ean_match_generic = self.update_match_columns(df_ean_match_generic)

        # remove rows that already have a match (generic)
        select_columns = ["PRODUCT_ID"]
        join_keys = (
            df_distinct_products_generic.PRODUCT_ID == df_ean_match_generic.PRODUCT_ID
        )

        df_distinct_products_descriptions = self.remove_matching_rows(
            df_distinct_products_generic,
            df_ean_match_generic,
            select_columns,
            join_keys,
        )
        # end ean match generic

        # description match
        df_master_table_explode_source_name = self.explode_column(
            df_master_table, "PRODUCT_SOURCE_NAME_MASTER", "SOURCE_EX"
        )

        df_description_match = self.match_by_product(
            df_master_table_explode_source_name,
            df_distinct_products_descriptions,
            False,
        )

        # update description match columns
        df_description_match = self.update_match_columns(df_description_match)

        # remove rows that already have a match (generic)
        select_columns = ["PRODUCT_ID"]
        join_keys = (
            df_distinct_products_descriptions.PRODUCT_ID
            == df_description_match.PRODUCT_ID
        )

        df_distinct_products_descriptions_generic = self.remove_matching_rows(
            df_distinct_products_descriptions, df_ean_match, select_columns, join_keys
        )
        # end description match

        # description match generic
        df_master_table_generic_explode_source_name = self.explode_column(
            df_master_table_generic, "PRODUCT_SOURCE_NAME_MASTER", "SOURCE_EX"
        )

        df_description_match_generic = self.match_by_product(
            df_master_table_generic_explode_source_name,
            df_distinct_products_descriptions_generic,
            False,
        )

        # update description match columns
        df_description_match_generic = self.update_match_columns(
            df_description_match_generic
        )
        # end description match generic

        # master treated
        df_treated_match = self.treated_match_by_description(
            df_distinct_treated,
            df_master_treated,
            df_master_table,
            df_ean_match,
            df_ean_match_generic,
            df_description_match,
            df_description_match_generic,
        )

        # update treated match columns
        df_treated_match = self.update_match_columns(df_treated_match)
        # end master treated

        # direct match
        df_direct_match = self.union_all_matches(
            df_ean_match,
            df_description_match,
            df_ean_match_generic,
            df_description_match_generic,
            df_treated_match,
        )

        # remove rows that already have a match (direct)
        select_columns = ["PRODUCT_ID"]
        join_keys = df_distinct_treated.product_id == df_direct_match.PRODUCT_ID

        df_distinct_treated_cleansing = self.remove_matching_rows(
            df_distinct_treated, df_direct_match, select_columns, join_keys
        )
        # end direct match

        df_master_treated = self.pre_process_master_treated(df_master_treated)

        df_distinct_treated_cleansing = self.transform_data(
            df_master_treated, df_distinct_treated_cleansing
        )

        return df_distinct_treated_cleansing

# COMMAND ----------

cleansing_predict = CleansingPredictInput()

df_distinct_treated_cleansing = cleansing_predict.definitions()

# COMMAND ----------

df_distinct_treated_cleansing.count()

# COMMAND ----------

# MAGIC %md
# MAGIC # DistinctTreatedCleansing

# COMMAND ----------

import json
def return_metadata(dataframe):
  schema_json = dataframe.schema.json()
 
  schemas = json.loads(schema_json)

  for schema in schemas['fields']:
    print(f"- name: '{schema['name']}'\n  description: ''\n  type: '{schema['type']}'")

# COMMAND ----------

return_metadata(df_distinct_treated_cleansing)

# COMMAND ----------

df_distinct_treated_cleansing.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Other Functions

# COMMAND ----------

def extract(self):

        data = [['brahma malzebier ln']
        ,['cerveja brahma garrafa 300ml 300ml unidade']
        ,['brahma 600ml apenas o liquido']
        ,['pack cerveja brahma malzbier ln 6unidade 355ml']
        ,['brahma super']
        ,['refrigerante cerveja brahma d malte 600000ml mapa 0089803']
        ,['cerveja brahma zero alcool ln 355ml']
        ,['cerveja brahma extra lager lt 350ml l12p8']
        ,['caixa brahma zero 350ml com 12 unidade']
        ,['pack brahma duplo malte 350ml']
        ,['cerveja brahma duplo malte 350ml']
        ,['brahma 600ml']
        ,['brahma zero lt 350ml']
        ,['cerveja brahma duplo malte 269ml']
        ,['cerveja brahma duplo malte garrafa ret 600ml 24unidade']
        ,['brahma pilsen buchudinha 300ml']
        ,['brahma duplo malte 300ml']
        ,['caixa brahma duplo malte 269ml com 15unidade']
        ,['garrafao brahma 1000ml unidade']
        ,['brahma duplo malte 1000ml']]

        df = spark.createDataFrame(data, ['product_description_treated'])

        return df

# COMMAND ----------

import difflib
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

class TypoCorrector:
    def __init__(self):
        self.master_words_broadcast = None

    def create_list_of_words(self, master):
        """
            Cria a lista de palavras da master, alem de adicionar algumas palavras chave
        """
        descriptions = master.select('product_description_treated').collect()

        # Verificar se cada descrição é uma tupla ou lista com ao menos um elemento
        descriptions = [phrase[0] for phrase in descriptions if phrase and len(phrase) > 0]

        master_words = ' '.join(descriptions)
        master_words = master_words.split(' ')

        master_words = master_words + ['do bem', 'ln', 'ow', 'budweiser', 
                                    'bohemia', 'brahma', 'skol', 'antarctica', 
                                    'wals', 'beats', 'pilsen', 'ml', 'misto', 'garrafa']

        master_words = [word for word in master_words if len(word) > 1]

        master_words = list(set(master_words))
        
        return master_words


    def load_master_words(self, df, column_name):
        """
        Carrega a lista de palavras mestre a partir de um DataFrame PySpark e faz o broadcast da lista.
        :param df: DataFrame PySpark que contém a coluna de palavras mestre
        :param column_name: Nome da coluna contendo as palavras mestre
        """
        # Coletar as palavras da coluna e armazená-las como uma lista
        words_list = self.create_list_of_words(df) #df.select(column_name).rdd.flatMap(lambda x: x).collect()
        
        # Fazer o broadcast da lista para todos os nós
        self.master_words_broadcast = spark.sparkContext.broadcast(words_list)

    # def fix_typo(self, text):
    #     """
    #     Corrige erros de digitação e normaliza palavras.
    #     Usa como base a lista de palavras mestre enviada via broadcast.
    #     """
    #     if not text:
    #         return None  # Verifica se o texto é None ou vazio

    #     # Acessa o valor do broadcast
    #     master_words = self.master_words_broadcast.value  
        
    #     words_list = text.split(' ')
    #     final_text = []
        
    #     for word in words_list:
    #         if word in master_words or 'ml' in word or word.isdigit():
    #             final_text.append(word)
    #         else:
    #             try:
    #                 matched_word = difflib.get_close_matches(word, master_words, 1, cutoff=0.90)
    #                 if matched_word:
    #                     final_text.append(matched_word[0])
    #                 else:
    #                     final_text.append(word)  # Caso não encontre correspondência, mantém a palavra original
    #             except:
    #                 final_text.append(word)  # Adiciona a palavra original em caso de erro
        
    #     return ' '.join(final_text)

    def correct_column(self, df, input_col, output_col):
        """
        Aplica a correção de erros de digitação em uma coluna de um DataFrame PySpark.
        :param df: DataFrame PySpark
        :param input_col: Nome da coluna de entrada com o texto a ser corrigido
        :param output_col: Nome da nova coluna com o texto corrigido
        :return: DataFrame com a nova coluna corrigida
        """
        # Verifica se o broadcast foi carregado
        if not self.master_words_broadcast:
            raise ValueError("Você deve carregar as palavras mestre antes de usar essa função.")

        # Recupera o valor do broadcast
        master_words = self.master_words_broadcast.value
        
        # Função UDF para aplicar a correção. Aqui, passamos 'master_words' diretamente para o UDF
        def fix_typo_udf(text):
            if not text:
                return None  # Verifica se o texto é None ou vazio
            
            words_list = text.split(' ')
            final_text = []
            
            for word in words_list:
                if word in master_words or 'ml' in word or word.isdigit():
                    final_text.append(word)
                else:
                    try:
                        matched_word = difflib.get_close_matches(word, master_words, 1, cutoff=0.90)
                        if matched_word:
                            final_text.append(matched_word[0])
                        else:
                            final_text.append(word)  # Caso não encontre correspondência, mantém a palavra original
                    except:
                        final_text.append(word)  # Adiciona a palavra original em caso de erro
            
            return ' '.join(final_text)

        # Converte a função de correção em UDF PySpark
        fix_typo_udf_spark = F.udf(fix_typo_udf, StringType())

        # Aplicar a função de correção à coluna especificada
        return df.withColumn(output_col, fix_typo_udf_spark(F.col(input_col)))

# COMMAND ----------

# Exemplo de uso no Databricks:

# Criar a instância da classe
typo_corrector = TypoCorrector()

# Supondo que 'df_palavras_mestre' seja um DataFrame PySpark com uma coluna 'master_column' contendo as palavras mestre
# Carregar as palavras mestre do DataFrame e fazer o broadcast
typo_corrector.load_master_words(df_master_treated, 'product_description_treated')

# Supondo que 'df' seja seu DataFrame PySpark com uma coluna chamada 'text_column'
df_corrigido = typo_corrector.correct_column(df_distinct_treated_cleasing, 'product_treated', 'product_description_treated')

# Mostrar o resultado
df_corrigido.show()

# COMMAND ----------

master_words

# COMMAND ----------

typo_corrector.master_words_broadcast

# COMMAND ----------

# Supondo que 'df' seja seu DataFrame PySpark com uma coluna chamada 'text_column'
df_corrigido = typo_corrector.correct_column(df_distinct_treated_cleasing, 'product_treated', 'product_description_treated')

# Mostrar o resultado
df_corrigido.show()
