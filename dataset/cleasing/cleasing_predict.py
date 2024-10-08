# Databricks notebook source
import json

from engineeringstore.core.transformation.task.task_entrypoint import TaskEntryPoint
from engineeringstore.core.transformation.transformation import Transformation

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.types import *
from pyspark.sql.types import StringType
from pyspark.sql.functions import (col, concat, explode, lit, pandas_udf, round, when)

import difflib

from pyspark.sql import SparkSession
import pandas as pd

spark.conf.set('spark.sql.execution.arrow.enabled', 'false')

# Main class for your transformation
class CleasingPredict(Transformation): #
    def __init__(self):
        super().__init__(
            dependencies = [
            ]
        )
        self.master_words = []

    def drop_columns(self, df, list_columns):
        return df.drop(*list_columns)
    
    def explode_column(self, df_to_explode, column_to_explode, alias_column):
        return df_to_explode.select(explode(col(column_to_explode)).alias(alias_column), "*")
    
    def process_master_table(self):
        
        # read
        table_name = "brewdat_uc_saz_prod.br_historical_sales.cz_mastertable"
        df_master_table = self.get_table(table_name)

        # transform

        # drop unnecessary columns
        df_master_table = self.drop_columns(df_master_table, ['year', 'month', 'day', 'ID', 'FRIENDLY_NAME'])

        # rename column source name
        df_master_table = df_master_table.withColumnRenamed('PRODUCT_SOURCE_NAME', 
                                                            'PRODUCT_SOURCE_NAME_MASTER')        

        return df_master_table
    
    def process_master_table_generic(self):

        table_name = "brewdat_uc_saz_prod.br_historical_sales.cz_mastertable_generic"  
        df_master_table_generic = self.get_table(table_name)

        # transform
        schema_sellin = schema = StructType([
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
        ])

        # drop unnecessary columns
        df_master_table_generic = self.drop_columns(df_master_table_generic, ['year', 'month', 'day', 'ID', 'FRIENDLY_NAME'])

        # add columns
        df_master_table_generic = (
            df_master_table_generic
            .withColumn('AUTO_FILL_SELLIN', lit(None))
            .withColumn('MATCH_INTERNAL_PRODUCT', lit(None).cast(ArrayType(schema_sellin)))
            )
        
        # rename column from PRODUCT_SOURCE_NAME to PRODUCT_SOURCE_NAME_MASTER
        df_master_table_generic = df_master_table_generic.withColumnRenamed('PRODUCT_SOURCE_NAME', 
                                                                            'PRODUCT_SOURCE_NAME_MASTER')
        
        return df_master_table_generic
    
    def process_distinct_products(self):

        # read  
        table_name = "brewdat_uc_saz_prod.gld_saz_sales_distinct_products.distinct_products"  
        df_distinct_products = self.get_table(table_name)
        #end read

        #transform
        df_distinct_products = (
            df_distinct_products
            .withColumn(
                "UNIDADES_CONTENIDO",
                when(
                    (col("SOURCE_NAME") == "SCANNTECH") & (col("SUBSOURCE_NAME") == "DIAFULL") & col("UNIDADES_CONTENIDO").isNull(),
                    1
                ).otherwise(col("UNIDADES_CONTENIDO"))
            )
            .withColumn(
                "MED_CANT_CONTENIDO",
                when(
                    (col("SOURCE_NAME") == "SCANNTECH") & (col("SUBSOURCE_NAME") == "DIAFULL") & col("MED_CANT_CONTENIDO").isNull(),
                    1
                ).otherwise(col("MED_CANT_CONTENIDO"))
            )
            )
        #end transform

        return df_distinct_products
    
    def process_distinct_treated(self):

        table_name = "brewdat_uc_saz_prod.gld_saz_sales_distinct_products.distinct_products_treated"  
        df_distinct_treated = self.get_table(table_name)

        return df_distinct_treated
    
    def process_master_treated(self):

        table_name = "brewdat_uc_saz_mlp_featurestore_prod.sales.cleansing_master_treated"  
        df_master_treated = self.get_table(table_name)        

        return df_master_treated
    
    def match_by_product(self, df_master_table, df_distinct_products, is_match_by_ean):

        df_match = (
            df_distinct_products.
                select(
                    'PRODUCT_ID',
                    'SUBSOURCE_NAME',
                    'SOURCE_NAME',
                    'BRAND',
                    'PRODUCT_SOURCE_NAME',
                    'PRODUCT_SOURCE_CODE',
                    'MED_CANT_CONTENIDO',
                    'UNIDADES_CONTENIDO'
                )
            )
        
        if is_match_by_ean:
            join_keys = df_master_table.EAN_EX == df_match.PRODUCT_SOURCE_CODE

            df_match = (
                df_match.filter("PRODUCT_SOURCE_CODE != 0")
            )

        else:
            join_keys = df_master_table.SOURCE_EX == df_match.PRODUCT_SOURCE_NAME

        df_match = (
            df_match.join(df_master_table, join_keys, 'inner')
        )

        if is_match_by_ean:
            # Adiciona a coluna 'PRODUCT_MATCH_TYPE'
            df_match = df_match.withColumn('PRODUCT_MATCH_TYPE', lit('EAN'))

            # Remove duplicatas e a coluna 'EAN_EX'
            df_match = df_match.dropDuplicates(subset=['PRODUCT_ID']).drop('EAN_EX')
        else:
            # Adiciona a coluna 'PRODUCT_MATCH_TYPE'
            df_match = df_match.withColumn('PRODUCT_MATCH_TYPE', lit('DESCRICAO'))
    
            # Remove duplicatas e a coluna 'SOURCE_EX'
            df_match = df_match.dropDuplicates(subset=['PRODUCT_ID']).drop('SOURCE_EX')            

        return df_match
    
    def match_by_ean(self, df_master_table, df_distinct_products):

        # Seleciona as colunas e faz o filtro
        df_ean_match = (
            df_distinct_products.alias('DistinctInput')
            .select(
            'PRODUCT_ID',
            'SUBSOURCE_NAME',
            'SOURCE_NAME',
            'BRAND',
            'PRODUCT_SOURCE_NAME',
            'PRODUCT_SOURCE_CODE',
            'MED_CANT_CONTENIDO',
            'UNIDADES_CONTENIDO'
            ).filter("PRODUCT_SOURCE_CODE != 0")
        )

        join_keys = df_master_table.EAN_EX == df_ean_match.PRODUCT_SOURCE_CODE

        # Faz o join com MasterOriginalExplode
        df_ean_match = (
            df_ean_match.join(
            df_master_table,
            join_keys,
            'inner'
            )
        )
        
        # Adiciona a coluna 'PRODUCT_MATCH_TYPE'
        df_ean_match = df_ean_match.withColumn('PRODUCT_MATCH_TYPE', lit('EAN'))
            
        # Remove duplicatas e a coluna 'EAN_EX'
        df_ean_match = df_ean_match.dropDuplicates(subset=['PRODUCT_ID']).drop('EAN_EX')

        return df_ean_match
    
    def match_by_description(self, df_master_table, df_distinct_products):
        # Seleciona as colunas relevantes e realiza o join
        df_description_match = (
            df_distinct_products.alias('DistinctInput').select(
            'PRODUCT_ID',
            'SUBSOURCE_NAME',
            'SOURCE_NAME',
            'BRAND',
            'PRODUCT_SOURCE_NAME',
            'PRODUCT_SOURCE_CODE',
            'MED_CANT_CONTENIDO',
            'UNIDADES_CONTENIDO'
            ).join(
            df_master_table.alias('MasterOriginalExplodeSourceName'),
            col('MasterOriginalExplodeSourceName.SOURCE_EX') == col('DistinctInput.PRODUCT_SOURCE_NAME'),
            'inner'
            )
        )
        
        # Adiciona a coluna 'PRODUCT_MATCH_TYPE'
        df_description_match = df_description_match.withColumn('PRODUCT_MATCH_TYPE', lit('DESCRICAO'))
        
        # Remove duplicatas e a coluna 'SOURCE_EX'
        df_description_match = df_description_match.dropDuplicates(subset=['PRODUCT_ID']).drop('SOURCE_EX')
        
        return df_description_match
    
    def update_match_columns(self, df_match):
        """
            Atualiza e transforma várias colunas no DataFrame 'EanMatch' de acordo com as condições sobre 'SOURCE_NAME' e 'SUBSOURCE_NAME'.
            Faz operações de atualização condicional, conversões de tipo e remoção de colunas.

            :param df: DataFrame PySpark contendo as colunas a serem atualizadas.
            :return: DataFrame transformado com colunas modificadas e removidas.
        """
        # Atualiza a coluna 'VOLUME_TOTAL'
        df_match = (
            df_match.withColumn(
            "VOLUME_TOTAL",
            when(
                (col("SOURCE_NAME") == "SCANNTECH") & (col("SUBSOURCE_NAME") == "DIAFULL"),
                col("MED_CANT_CONTENIDO")
            ).otherwise(col("VOLUME_TOTAL"))
            )
        )

        # Atualiza a coluna 'PACK_QUANTITY'
        df_match = (
            df_match.withColumn(
            "PACK_QUANTITY",
            when(
                (col("SOURCE_NAME") == "SCANNTECH") & (col("SUBSOURCE_NAME") == "DIAFULL"),
                col("UNIDADES_CONTENIDO")
            ).otherwise(col("PACK_QUANTITY"))
            )
        )

        # Converte 'MED_CANT_CONTENIDO' e 'UNIDADES_CONTENIDO' para int
        df_match = (
            df_match
            .withColumn("MED_CANT_CONTENIDO", col("MED_CANT_CONTENIDO").cast("int"))
            .withColumn('UNIDADES_CONTENIDO', col('UNIDADES_CONTENIDO').cast("int"))
            )
            
        # Cria a coluna 'VOLUME_CONTENIDO' arredondada
        df_match = df_match.withColumn('VOLUME_CONTENIDO', round(col("MED_CANT_CONTENIDO") / col('UNIDADES_CONTENIDO'), 0))
            
        # Converte 'VOLUME_CONTENIDO' para long
        df_match = df_match.withColumn('VOLUME_CONTENIDO', col('VOLUME_CONTENIDO').cast("long"))
            
        # Atualiza a coluna 'VOLUME_UNIT'
        df_match = (
            df_match.withColumn(
            "VOLUME_UNIT",
            when(
                (col("SOURCE_NAME") == "SCANNTECH") & (col("SUBSOURCE_NAME") == "DIAFULL"),
                col("VOLUME_CONTENIDO")
            ).otherwise(col("VOLUME_UNIT"))
            )
        )
            
        # Remove as colunas desnecessárias
        df_match = df_match.drop("MED_CANT_CONTENIDO", 'VOLUME_CONTENIDO', "UNIDADES_CONTENIDO")

        return df_match
    
    def remove_matching_rows(self, df_distinct_products, df_match, select_columns, join_keys):
        # 
        return df_distinct_products.join(
                df_match.select(select_columns),
                join_keys,
                how='left_anti'
            )

    def treated_match_by_description(self, df_distinct_treated, df_master_treated, df_master_table, df_ean_match, df_ean_match_generic, df_description_match, df_description_match_generic):

        # Realiza o match pela descrição tratada
        treated_match = (
            df_distinct_treated.select(
            'product_id',
            'source_table',
            'source_name',
            'unidades_contenido',
            'med_cant_contenido',
            'brand_original',
            'product_original',
            'product_source_code',
            'product_treated'
            ).join(
                df_master_treated.select('product_description', 'product_treated'),
                on='product_treated'
            )
        )

        # Renomeia as colunas e faz o join com o MasterOriginal
        treated_match = (
            treated_match.dropDuplicates(subset=['product_id'])
            .withColumnRenamed('product_id', 'PRODUCT_ID')
            .withColumnRenamed('source_table', 'SUBSOURCE_NAME')
            .withColumnRenamed('unidades_contenido', 'UNIDADES_CONTENIDO')
            .withColumnRenamed('med_cant_contenido', 'MED_CANT_CONTENIDO')
            .withColumnRenamed('source_name', 'SOURCE_NAME')
            .withColumnRenamed('brand_original', 'BRAND')
            .withColumnRenamed('product_original', 'PRODUCT_SOURCE_NAME')
            .withColumnRenamed('product_source_code', 'PRODUCT_SOURCE_CODE')
            .withColumn('PRODUCT_MATCH_TYPE', F.lit('DESCRICAO_TRATADA'))
            .join(df_master_table, on='PRODUCT_DESCRIPTION')
            .drop('product_treated')
            )
            
        # Remove os registros que já tiveram match por EAN ou descrição
        treated_match = (
            treated_match
            .join(df_ean_match, on='PRODUCT_ID', how='left_anti')
            .join(df_ean_match_generic, on='PRODUCT_ID', how='left_anti')
            .join(df_description_match, on='PRODUCT_ID', how='left_anti')
            .join(df_description_match_generic, on='PRODUCT_ID', how='left_anti')
            )
        
        return treated_match
    
    def union_all_matches(self, df_ean_match, df_description_match, df_ean_match_generic, df_description_match_generic, df_treated_match):

        # Realiza a união de todos os matches diretos
        df_direct_match = (
            df_ean_match
            .unionByName(df_description_match)
            .unionByName(df_ean_match_generic)
            .unionByName(df_description_match_generic)
            .unionByName(df_treated_match)
        )

        # Remove duplicatas com base em 'PRODUCT_ID'
        df_direct_match = df_direct_match.dropDuplicates(subset=['PRODUCT_ID'])
        
        return df_direct_match
    
    def pre_process_master_treated(self, df_master_treated):
        """colunas_master = ['product_description', 
                  'product_description_treated', 
                  'category', 
                  'brand_id', 
                  'family', 
                  'volume_unit', 
                  'pack_quantity', 
                  'package_unit']

        df_master_treated = df_master_treated.select(colunas_master)"""
        df_master_treated = df_master_treated.dropDuplicates(['product_description_treated'])

        return df_master_treated

    def add_unidade(self, df):
        """
            Adiciona '1unidade' ao fim das descrições sem informação de unidade
        """
  
        df = (
            df.withColumn('product_description_treated', 
                        when(df.product_description_treated.like('%unidade%'),
                            df.product_description_treated)
                                .otherwise(concat(df.product_description_treated, lit(' 1unidade'))))
            )
        
        return df
    
    def create_list_of_words(self, master):
        """
            Cria a lista de palavras da master, alem de adicionar algumas palavras chave
        """
        descriptions = list(master.select('product_description_treated').collect())
        descriptions = [phrase[0] for phrase in descriptions]

        master_words = ' '.join(descriptions)
        master_words = master_words.split(' ')

        master_words = master_words+ ['do bem', 'ln', 'ow', 'budweiser', 
                                'bohemia', 'brahma', 'skol', 'antarctica', 
                                'wals', 'beats', 'pilsen', 'ml', 'misto', 'garrafa']

        master_words = [word for word in master_words if len(word) > 1]

        master_words = list(set(master_words))
        return master_words
    
    def load_master_words(self, df_master_treated):

        # Adiciona '1unidade' ao fim das descrições sem informação de unidade
        df_master_treated = self.add_unidade(df_master_treated)

        # Cria lista de palavras presentes na master
        self.master_words = self.create_list_of_words(df_master_treated)
    
    def fix_typo(self, text, master_words):
        """
            Corrige erros de digitação e normaliza palavras. 
            Usa como base a lista de palavras presentes na master (master_words)
        """
        if not text:
            return None  # Verifica se o texto é None ou vazio

        words_list = text.split(' ')
        final_text = list()
        for word in words_list:
            if word in master_words or 'ml' in word or all(map(str.isdigit, word)):
                final_text.append(word + ' ')
            else:
                try:
                    matched_word = difflib.get_close_matches(word, master_words, 1, cutoff=0.90)[0]
                    final_text.append(matched_word + ' ')
                except:
                    pass
                    
        final_text = ''.join(final_text)
        final_text = final_text.strip()
        return final_text
    
    #@udf(T.StringType())
    @staticmethod
    def create_number_data(text):
        """
            Deixa somente informações numéricas ou de package
        """

        regexp = re.compile(r'\d')
        packages = ('garrafa', 'pet', 'lt', 'barril', 'draft')
        
        text = [word for word in text.split() if regexp.search(word) or word in packages]
        text = ' '.join(text)
            
        return text.strip()    
    
    #@udf(T.StringType())
    @staticmethod
    def create_brand_data(text):
        """
            Remove informações numéricas
        """        
        text = [word for word in text.split() if not word.endswith('ml') and not word.endswith('unidade') and not word.endswith('unidades')]
        text = ' '.join(text)
    
        return text.strip()


    
    def transform_data(self, df_master_treated, df_distinct_treated_cleasing):
        

        # Corrige possíveis erros de digitação, usando como base a lista de palavras da Master
        #fix_typo_udf = F.udf(CleasingPredict.fix_typo, StringType())

        # col_input = 'product_description_treated'
        # col_output = 'product_treated'

        # df_distinct_treated_cleasing = (
        #      df_distinct_treated_cleasing.withColumn(col_input, fix_typo_udf(F.col(col_output), master_words))
        # )

        # df_distinct_treated_cleasing = (
        #     df_distinct_treated_cleasing
        #         .withColumn('product_description_treated', 
        #                     self.fix_typo(df_distinct_treated_cleasing.product_treated, 
        #                                   master_words))
        # )

        # Adiciona '1unidade' ao fim das descrições sem informação de unidade
        #df_distinct_treated_cleasing = add_unidade(df_distinct_treated_cleasing)

        # Cria descrições só com informação importantes para Volume e Quantidade (number_description) 
        # df_distinct_treated_cleasing = (
        #     df_distinct_treated_cleasing
        #         .withColumn('number_description', 
        #                     CleasingPredict.create_number_data('product_description_treated')
        #                 )
        #     )

        # Cria descrições só com informação importantes para Marca, Familia e Categoria (brand_description) 
        # df_input = (
        #     df_distinct_treated_cleasing
        #         .withColumn('brand_description', 
        #                     CleasingPredict.create_brand_data('product_description_treated'))
        #         .drop('brand_id')
        #     )

        return master_words

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
        df_master_table_explode = self.explode_column(df_master_table, 'EAN', 'EAN_EX')
        df_ean_match = self.match_by_product(df_master_table_explode, df_distinct_products, True)

        ## update match columns
        df_ean_match = self.update_match_columns(df_ean_match)

        ## remove rows that already have a match
        select_columns = ['PRODUCT_ID']
        join_keys = df_distinct_products.PRODUCT_ID == df_ean_match.PRODUCT_ID
        
        df_distinct_products_generic = (
            self.remove_matching_rows(
                df_distinct_products, 
                df_ean_match, 
                select_columns, 
                join_keys
            )
        )
        # end ean match

        # ean match generic
        df_master_table_generic_explode = self.explode_column(df_master_table_generic, 'EAN', 'EAN_EX')
        df_ean_match_generic = self.match_by_product(df_master_table_generic_explode, df_distinct_products_generic, True)

        ## update match columns (generic)
        df_ean_match_generic = self.update_match_columns(df_ean_match_generic)        

        ## remove rows that already have a match (generic)
        select_columns = ['PRODUCT_ID']
        join_keys = df_distinct_products_generic.PRODUCT_ID == df_ean_match_generic.PRODUCT_ID        

        df_distinct_products_descriptions = (
            self.remove_matching_rows(
                df_distinct_products_generic, 
                df_ean_match_generic, 
                select_columns, 
                join_keys
            )        
        )
        # end ean match generic

        # description match
        df_master_table_explode_source_name = self.explode_column(
            df_master_table, 'PRODUCT_SOURCE_NAME_MASTER', 'SOURCE_EX'
        )

        df_description_match = self.match_by_product(
            df_master_table_explode_source_name, df_distinct_products_descriptions, False
        )

        ## update description match columns
        df_description_match = self.update_match_columns(df_description_match)

        ## remove rows that already have a match (generic)
        select_columns = ['PRODUCT_ID']
        join_keys = df_distinct_products_descriptions.PRODUCT_ID == df_description_match.PRODUCT_ID #analysis      

        df_distinct_products_descriptions_generic = (
            self.remove_matching_rows(df_distinct_products_descriptions, 
                    df_ean_match, 
                    select_columns, 
                    join_keys)
            )
        # end description match

        # description match generic
        df_master_table_generic_explode_source_name = self.explode_column(
            df_master_table_generic, 'PRODUCT_SOURCE_NAME_MASTER', 'SOURCE_EX'
        )

        df_description_match_generic = self.match_by_product(
            df_master_table_generic_explode_source_name, 
            df_distinct_products_descriptions_generic,
            False
        )
    
        ## update description match columns
        df_description_match_generic = self.update_match_columns(df_description_match_generic)
        # end description match generic

        # master treated
        df_treated_match = (
            self.treated_match_by_description(
                df_distinct_treated,
                df_master_treated,
                df_master_table,
                df_ean_match,
                df_ean_match_generic,
                df_description_match,
                df_description_match_generic
            )
        )

        #update treated match columns
        df_treated_match = self.update_match_columns(df_treated_match)
        # end master treated

        # direct match
        df_direct_match = (
            self.union_all_matches(
                df_ean_match, 
                df_description_match, 
                df_ean_match_generic, 
                df_description_match_generic, 
                df_treated_match)
            )
        
        ## remove rows that already have a match (direct)
        select_columns = ['PRODUCT_ID']
        join_keys = df_distinct_treated.product_id == df_direct_match.PRODUCT_ID

        df_distinct_treated_cleasing = (
            self.remove_matching_rows(
                df_distinct_treated, 
                df_direct_match, 
                select_columns, 
                join_keys)
            )
        # end direct match

        df_master_treated = self.pre_process_master_treated(df_master_treated)

        self.load_master_words(df_master_treated)

        master_words  = self.transform_data(df_master_treated, df_distinct_treated_cleasing)

        return df_master_treated, df_distinct_treated_cleasing

# COMMAND ----------

cleasing_predict = CleasingPredict()

df_master_treated, df_distinct_treated_cleasing = cleasing_predict.definitions()

cleasing_predict.master_words

# COMMAND ----------

df_distinct_treated_cleasing.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Functions

# COMMAND ----------

# MAGIC %md
# MAGIC # Other Functions

# COMMAND ----------

import difflib
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

class TypoCorrector:
    def __init__(self, master_words):
        # Lista de palavras de referência para correção
        self.master_words = master_words

    def create_list_of_words(self, master):
        """
            Cria a lista de palavras da master, alem de adicionar algumas palavras chave
        """
        descriptions = list(master.select('product_description_treated').collect())
        descriptions = [phrase[0] for phrase in descriptions]

        master_words = ' '.join(descriptions)
        master_words = master_words.split(' ')

        master_words = master_words+ ['do bem', 'ln', 'ow', 'budweiser', 
                                'bohemia', 'brahma', 'skol', 'antarctica', 
                                'wals', 'beats', 'pilsen', 'ml', 'misto', 'garrafa']

        master_words = [word for word in master_words if len(word) > 1]

        master_words = list(set(master_words))
        return master_words

    def fix_typo(self, text):
        """
        Corrige erros de digitação e normaliza palavras.
        Usa como base a lista de palavras presentes na master (self.master_words)
        """
        if not text:
            return None  # Verifica se o texto é None ou vazio
        
        words_list = text.split(' ')
        final_text = []
        
        for word in words_list:
            if word in self.master_words or 'ml' in word or word.isdigit():
                final_text.append(word)
            else:
                try:
                    matched_word = difflib.get_close_matches(word, self.master_words, 1, cutoff=0.90)
                    if matched_word:
                        final_text.append(matched_word[0])
                    else:
                        final_text.append(word)  # Caso não encontre correspondência, mantém a palavra original
                except:
                    final_text.append(word)  # Adiciona a palavra original em caso de erro
        
        return ' '.join(final_text)
    
    def fix_typo_old(self, df, text):
        """
            Corrige erros de digitação e normaliza palavras. 
            Usa como base a lista de palavras presentes na master (master_words)
        """

        master_words = self.create_list_of_words(df)

        words_list = text.split(' ')
        final_text = list()
        for word in words_list:
            if word in master_words or 'ml' in word or all(map(str.isdigit, word)):
                final_text.append(word + ' ')
            else:
                try:
                    matched_word = difflib.get_close_matches(word, master_words, 1, cutoff=0.90)[0]
                    final_text.append(matched_word + ' ')
                except:
                    pass
                    
        final_text = ''.join(final_text)
        final_text = final_text.strip()
        return final_text

    def correct_column(self, df, df_master, input_col, output_col):
        """
        Aplica a correção de erros de digitação em uma coluna de um DataFrame PySpark
        :param df: DataFrame PySpark
        :param input_col: Nome da coluna de entrada com o texto a ser corrigido
        :param output_col: Nome da nova coluna com o texto corrigido
        :return: DataFrame com a nova coluna corrigida
        """
        # Função UDF para aplicar a correção
        fix_typo_udf = F.udf(self.fix_typo, StringType())
        fix_typo_udf_old = F.udf(self.fix_typo_old, StringType())
        
        # Aplicar a função de correção à coluna especificada
        return df.withColumn(output_col, fix_typo_udf_old(F.col(input_col), df_master))

# Exemplo de uso:
# Criar a lista de palavras mestre
# master_words = ['pyspark', 'dataframe', 'correction', 'error', 'model', 'ml']

# Criar a instância da classe
# typo_corrector = TypoCorrector(master_words)

# # Supondo que 'df' seja seu DataFrame PySpark com uma coluna chamada 'text_column'
# df_corrigido = typo_corrector.correct_column(df, 'product_treated', 'product_description_treated')

# # Mostrar o resultado
# df_corrigido.show()



# COMMAND ----------

# Criar a instância da classe
typo_corrector = TypoCorrector(master_words)

# Supondo que 'df' seja seu DataFrame PySpark com uma coluna chamada 'text_column'
df_corrigido = typo_corrector.correct_column(df_distinct_treated_cleasing, df_master_treated, 'product_treated', 'product_description_treated')

# Mostrar o resultado
# df_corrigido.show()


# COMMAND ----------

df_corrigido.display()

# COMMAND ----------


