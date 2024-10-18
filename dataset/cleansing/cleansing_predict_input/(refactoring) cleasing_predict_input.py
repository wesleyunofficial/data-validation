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
    
    def process_direct_match(self) -> DataFrame:
        """
        Retrieves the direct match table.

        This function reads the direct match table from the specified source
        and returns it as a DataFrame without applying any transformations.

        Steps performed:
        ----------------
        1. Reads the direct match table from the specified source.

        Returns:
        --------
        DataFrame
            The direct match DataFrame.
        """
        
        table_name = "brewdat_uc_saz_prod.gld_saz_sales_cleansing.cleansing_direct_match"
        df_direct_match = self.get_table(table_name)

        return df_direct_match

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
        df_distinct_treated = self.process_distinct_treated()

        df_master_treated = self.process_master_treated()

        df_direct_match = self.process_direct_match()
        # end read datasets

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
