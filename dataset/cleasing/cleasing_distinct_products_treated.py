# Databricks notebook source
# Databricks notebook source
import json

from engineeringstore.core.transformation.task.task_entrypoint import TaskEntryPoint
from engineeringstore.core.transformation.transformation import Transformation

from pyspark.sql.functions import (
    col,
    concat_ws,
    lit,
    monotonically_increasing_id,
    reduce,
    regexp_replace,
    udf,
    when,
)
from pyspark.sql.types import StringType
from pyspark.sql import DataFrame
import re
from datetime import datetime
import random
from unicodedata import normalize

from pyspark.sql import types as T


def description_treatment(string):

    crt_dict = {
        "refrigerante": ["refri", "refrig", "refr", "ref", "refriger"],
        "cerveja": ["cerv", "cervej", "cer", "cerve"],
        "chopp": ["chop",],
        "tonica": ["ton", "tonic"],
        "itaipava": ["itpv", "itaip"],
        "do bem": ["db"],
        "h2oh": ["h2o", "h20h", "h20"],
        "antarctica": [
            "antartica",
            "ant",
            "antar",
            "antart",
            "antartc",
            "antarct",
            "antarctic",
            "antarc",
            "antartic",
        ],
        "schweppes": ["schw"],
        "guarana": ["guar", "guara", "guarah"],
        "heineken": ["heinek"],
        "laranja": ["lar", "laran", "lja"],
        "limao": ["lim"],
        "maCZbier": ["maCZebier", "maCZ", "malizbier", "maCZb"],
        "pilsen": ["pil", "pils"],
        "budweiser": ["bud", "budweis"],
        "alcool": ["alc"],
        "brahma": ["brama", "ahma"],
        "original": ["orig", "origina", "origin", "ori"],
        "agua": ["ag"],
        "coca cola": ["cocacola"],
        "pepsi": ["pespi"],
        "sprite": ["sprit"],
        "mineral": ["min"],
        "stella": ["stela"],
        "caixa": ["cx", "cxa"],
        "unidade": ["un", "uni", "unidad", "und"],
        "lt": ["lata", "latao", "latagelada", "latas", "lat", "lta"],
        "garrafa": ["gf", "gfa", "grfa", "grf"],
        "ln": ["lneck", "longnec", "lnk", "long", "lon"],
        "zero": ["0,0", "0 alcool", "sacucar", "menos acucar"],
        "puro malte": ["pmalte"],
        "senses": ["sense", "sens"],
        "garrafa 300ml": ["litrinho"],
    }

    # abv_dict = {}
    # for k, v in crt_dict.items():
    #     for x in v:
    #         abv_dict[x] = k

    stopw = [
        "atac",
        "beb",
        "irrelevante",
        "cl",
        "retornavel",
        "retorn",
        "gelada",
        "atcd",
    ]

    # Inverter o dicionário para substituir abreviações
    abv_dict = {abbr: full for full, abbrs in crt_dict.items() for abbr in abbrs}

    string = normalize("NFKD", string.lower()).encode("ascii", "ignore").decode("utf-8")
    #string = self.fix_volume(string)
    #string = self.fix_specifics_words(string)
    #string = self.fix_volume(string)

    list_string = [
        abv_dict[word] if word in abv_dict.keys() else word for word in string.split()
    ]
    
    list_string = [word for word in list_string if word not in stopw]
    string = " ".join(list_string)

    return string

# Criar a UDF
description_udf = udf(description_treatment, StringType())

# Main class for your transformation
class DistinctProductsTreated(Transformation):
    def __init__(self):
        super().__init__(dependencies=[])

    def extract_table_distinct_products(self) -> DataFrame:
        """ """

        df_distinct_products = self.get_table(
            "brewdat_uc_saz_prod.gld_saz_sales_distinct_products.distinct_products"
        )

        df_distinct_products = df_distinct_products.withColumnRenamed(
            "PRODUCT_SOURCE_CODE", "EAN_SOURCE_CODE"
        )

        df_distinct_products = df_distinct_products.withColumn(
            "EAN_SOURCE_CODE",
            when(col("SUBSOURCE_NAME").contains("AUDIT"), lit(0)).otherwise(
                col("EAN_SOURCE_CODE")
            ),
        )

        return df_distinct_products

    def transform_distinct_products(self, df_distinct_products: DataFrame) -> DataFrame:

        df_distinct_products = df_distinct_products.withColumnRenamed(
            "PRODUCT_ID", "product_id"
        )

        df_distinct_products = (
            df_distinct_products.withColumn(
                "PRODUCT_SOURCE_NAME_ORIGINAL", col("PRODUCT_SOURCE_NAME")
            )
            .withColumn("BRAND_ORIGINAL", col("BRAND"))
            .withColumn(
                "BRAND", regexp_replace(col("BRAND"), "(?i)OUTRA MARCA", "OUTRAMARCA")
            )
            .withColumn(
                "PRODUCT_SOURCE_NAME",
                regexp_replace(col("PRODUCT_SOURCE_NAME"), "(?i)DO BEM", "DOBEM"),
            )
            .withColumn(
                "PRODUCT_SOURCE_NAME",
                regexp_replace(col("PRODUCT_SOURCE_NAME"), "(?i)JOTA EFE", "JOTAEFE"),
            )
        )

        return df_distinct_products

    def select_columns_distinct_products(
        self, df_distinct_products: DataFrame
    ) -> DataFrame:

        use_brand = False

        if use_brand:
            df_distinct_products = (
                df_distinct_products.select(
                    col("BRAND").alias("brand_id"),
                    col("BRAND_ORIGINAL").alias("brand_original"),
                    col("MED_CANT_CONTENIDO").alias("med_cant_contenido"),
                    col("UNIDADES_CONTENIDO").alias("unidades_contenido"),
                    col("PRODUCT_SOURCE_NAME").alias("product"),
                    col("PRODUCT_SOURCE_NAME_ORIGINAL").alias("product_original"),
                    "product_id",
                    col("EAN_SOURCE_CODE").alias("product_source_code"),
                    col("SOURCE_NAME").alias("source_name"),
                    col("SUBSOURCE_NAME").alias("source_table"),
                )
                .withColumn("product_aggregated", concat_ws(" ", "product", "brand_id"))
                .distinct()
            )
        else:
            df_distinct_products = (
                df_distinct_products.select(
                    col("BRAND").alias("brand_id"),
                    col("BRAND_ORIGINAL").alias("brand_original"),
                    col("MED_CANT_CONTENIDO").alias("med_cant_contenido"),
                    col("UNIDADES_CONTENIDO").alias("unidades_contenido"),
                    col("PRODUCT_SOURCE_NAME").alias("product"),
                    col("PRODUCT_SOURCE_NAME_ORIGINAL").alias("product_original"),
                    "product_id",
                    col("EAN_SOURCE_CODE").alias("product_source_code"),
                    col("SOURCE_NAME").alias("source_name"),
                    col("SUBSOURCE_NAME").alias("source_table"),
                )
                .withColumn("product_aggregated", col("product"))
                .distinct()
            )

        df_distinct_products = df_distinct_products.where(
            col("source_table") != lit("FABRICA")
        )

        return df_distinct_products

    def insert_random_id(self, df, column_id_name):

        dt = datetime.now()
        random.seed(dt.microsecond)
        # random_prefix = random.randint(1, 10000)

        df = df.withColumn(column_id_name, monotonically_increasing_id())

        return df
    
    @staticmethod
    def unique_word_string(text):
        ulist = []
        [ulist.append(x) for x in text.split(" ") if x not in ulist]
        return " ".join(ulist)

    def create_column_product_treated(self, df):

        column_aggregate_from = "product_aggregated"
        column_aggregate_to = "product_treated"
        column_id = "id"

        if column_id not in df.columns:
            df = self.insert_random_id(df, column_id)

        if type(column_aggregate_from) is list:
            df = reduce(
                lambda df, i: df.withColumn(
                    column_aggregate_to,
                    df[column_aggregate_to] + lit(" ") + df[column_aggregate_from[i]],
                ),
                range(len(column_aggregate_from)),
                df,
            )
        else:
            df = df.withColumn(column_aggregate_to, df[column_aggregate_from])

        return df

    @staticmethod
    def fix_unity(string):

        # 's/' -> sem 'c/' ou ' c ' -> com
        string = re.sub("s/", " sem ", string)
        string = re.sub("(c/|\sc\s)", " com ", string)
        # 12x250 - 12 x 250
        string = re.sub(r"\b(\d+)(x|\s+x)(\d+|\s+\d+)", r"\1unidade \3", string)
        # 6un
        string = re.sub(r"\b(\d+|\d+\s+)(un|unidad|unid|und)\b", r"\1unidade ", string)
        # c6 - com 6 - cx6
        # string = re.sub(r'\b(cx|c|cxc|com)(\s+\d+|\d+)(|un)\b',
        string = re.sub(
            r"\b(caixa|caixa com|cx|cxa)(\s+\d+|\d+)(|un)((\s+)(unidades?))?\b",  # PARA CORRIGIR PROBLEMA DOS PACKS
            r"\2unidade ",
            string,
        )
        # pack 3|6
        # string = re.sub(r'\b(pack)(\s+)(\d+)', r'\3unidade', string)
        string = re.sub(
            r"(pack( de| com)?)(\s+)(\d+)((\s+)(unidades?))?", r"\4unidades", string
        )  # PARA CORRIGIR PROBLEMA DOS PACKS
        string = re.sub("sixpack|six pack", "6unidade", string)
        string = re.sub("twelvepack|twelve pack", "12unidade", string)

        return string
    
    @staticmethod
    def fix_l(string):
        pattern = (
            r"(\d+)(,|\.|\s+|"
            ")(\d+|"
            ")(\s+|"
            ")(lts|ltr|lt|lits|litros|litro|l)(\s+|$)"
        )
        find = re.search(pattern, string)
        if find:
            number = re.sub(pattern, r"\1.\3", find.group())
            number = " " + str(int(float(number) * 1000)) + "ml "
            string = re.sub(find.group(), number, string)
        string = re.sub("litrao|litro", " 1000ml ", string)

        return string
    
    @staticmethod
    def fix_ml(string):
        string = re.sub(r"(\d+)(\s+|" ")(ml|m)(\s+|$)", r" \1\3 ", string)
        string = re.sub(r"(\d+)(m)(\s+|$)", r"\1ml ", string)

        return string

    @staticmethod
    def fix_volume(string):
        string = DistinctProductsTreated.fix_unity(string)
        string = DistinctProductsTreated.fix_l(string)
        string = DistinctProductsTreated.fix_ml(string)

        string = re.sub(r"\b(\d+)\s(unidade)", r"\1\2", string)
        string = re.sub(r"0(\d+unidade)", r"\1", string)

        return string

    ###############################################################################
    @staticmethod
    def fix_specifics_words(string):
        string = re.sub("(transf.|transferencia|transf)\s+de\s+mesa\s+\d+", "", string)
        string = re.sub(r"(beck's\b)|(beck( s|\b))", "becks", string)
        string = re.sub(
            "coca( [^c]|$)|com\s+cola|colacola|cola\s+cola|coca( |)cola",
            "coca-cola",
            string,
        )
        string = re.sub("refguarana", "refrigerante guarana", string)
        string = re.sub("refpet", "refrigerante pet", string)
        string = re.sub("serra(\s+|" ")(malte|malt|m )", "serramalte", string)
        string = re.sub(r"#(.*)#", "", string)
        string = re.sub("n/a|#\d+ |[^A-Za-z0-9|]+", " ", string)
        string = re.sub("\s+(one|on|o)\s+(way|w)(\s+|$)", " ow ", string)
        string = re.sub("oneway", "ow", string)
        string = re.sub("(\s|^)(long neck|long nek|long n|l neck|l n )", " ln ", string)
        string = re.sub("com gas", "comgas", string)
        string = re.sub("sem gas", "semgas", string)
        string = re.sub("sub zero", "subzero", string)
        string = re.sub(r"\bjesus guarana\b", "guarana jesus", string)
        string = re.sub(r"\bbrahma (chopp\b|chop\b)", "brahma", string)
        string = re.sub(r"\bproibida pilsen\b", "proibida", string)
        string = re.sub(r"antarcti\b", "antarctica", string)
        string = re.sub(r"\b150 bpm\b", "150bpm", string)
        string = re.sub(r"\bnao informado\b", "", string)
        string = re.sub(r"\b(zer|zr)\b", "zero", string)

        return string

    # @staticmethod
    # def description_treatment(string):

    #     crt_dict = {
    #         "refrigerante": ["refri", "refrig", "refr", "ref", "refriger"],
    #         "cerveja": ["cerv", "cervej", "cer", "cerve"],
    #         "chopp": [
    #             "chop",
    #         ],
    #         "tonica": ["ton", "tonic"],
    #         "itaipava": ["itpv", "itaip"],
    #         "do bem": ["db"],
    #         "h2oh": ["h2o", "h20h", "h20"],
    #         "antarctica": [
    #             "antartica",
    #             "ant",
    #             "antar",
    #             "antart",
    #             "antartc",
    #             "antarct",
    #             "antarctic",
    #             "antarc",
    #             "antartic",
    #         ],
    #         "schweppes": ["schw"],
    #         "guarana": ["guar", "guara", "guarah"],
    #         "heineken": ["heinek"],
    #         "laranja": ["lar", "laran", "lja"],
    #         "limao": ["lim"],
    #         "maCZbier": ["maCZebier", "maCZ", "malizbier", "maCZb"],
    #         "pilsen": ["pil", "pils"],
    #         "budweiser": ["bud", "budweis"],
    #         "alcool": ["alc"],
    #         "brahma": ["brama", "ahma"],
    #         "original": ["orig", "origina", "origin", "ori"],
    #         "agua": ["ag"],
    #         "coca cola": ["cocacola"],
    #         "pepsi": ["pespi"],
    #         "sprite": ["sprit"],
    #         "mineral": ["min"],
    #         "stella": ["stela"],
    #         "caixa": ["cx", "cxa"],
    #         "unidade": ["un", "uni", "unidad", "und"],
    #         "lt": ["lata", "latao", "latagelada", "latas", "lat", "lta"],
    #         "garrafa": ["gf", "gfa", "grfa", "grf"],
    #         "ln": ["lneck", "longnec", "lnk", "long", "lon"],
    #         "zero": ["0,0", "0 alcool", "sacucar", "menos acucar"],
    #         "puro malte": ["pmalte"],
    #         "senses": ["sense", "sens"],
    #         "garrafa 300ml": ["litrinho"],
    #     }

    #     abv_dict = {}
    #     for k, v in crt_dict.items():
    #         for x in v:
    #             abv_dict[x] = k

    #     stopw = [
    #         "atac",
    #         "beb",
    #         "irrelevante",
    #         "cl",
    #         "retornavel",
    #         "retorn",
    #         "gelada",
    #         "atcd",
    #     ]

    #     string = (
    #         normalize("NFKD", string.lower()).encode("ascii", "ignore").decode("utf-8")
    #     )
    #     string = DistinctProductsTreated.fix_volume(string)
    #     string = DistinctProductsTreated.fix_specifics_words(string)
    #     string = DistinctProductsTreated.fix_volume(string)

    #     list_string = [
    #         abv_dict[word] if word in abv_dict.keys() else word
    #         for word in string.split()
    #     ]
    #     list_string = [word for word in list_string if word not in stopw]
    #     string = " ".join(list_string)

    #     return string

    def text_treatment_dataframe(self, df, master):

        def add_ml(line):
            words = line.split()
            vol = [i for i in words if i in vols]
            if len(vol)==1:
                value = vol[0]
                return re.sub(value, value+'ml', line)
            else:
                return line

        vols = [i.volume_unit for i in master.select('volume_unit').distinct().collect()]
        column_to_treat = 'product_treated'
        column_id = 'id'

        df = df.withColumn(column_id, monotonically_increasing_id())
        df = df.fillna('')

        # Considera apenas palavras unicas na string
        unique_word_string_udf = udf(DistinctProductsTreated.unique_word_string, StringType())
        df = df.withColumn(column_to_treat, unique_word_string_udf(column_to_treat))

        # Remove codigos do texto, caracteres irrelevates, padroniza volume e passa para UTF-8
        #description_treatment_udf = udf(lambda line: DistinctProductsTreated.description_treatment(line), StringType())
        #description_treatment_udf = udf(DistinctProductsTreated.description_treatment, StringType())
        df = df.withColumn(column_to_treat, description_udf(column_to_treat))

        # Adiciona ml em numeros que provavelmente sao volumes
        add_ml_udf = udf(lambda line: add_ml(line), StringType())
        df = df.withColumn(column_to_treat, add_ml_udf(column_to_treat))

        return df
    
    def process_description_column(self, df, column_name):
        
        #description_treatment_udf = udf(lambda line: DistinctProductsTreated.description_treatment(line), StringType())
        df = df.withColumn(column_name, description_udf(column_name))

        return df


    # def process_column(self, df, column_name):
    #     # Transformar a função Python comum em uma UDF do PySpark
    #     unique_word_udf = udf(DistinctProductsTreated.unique_word_string, StringType())
        
    #     # Aplicar a UDF na coluna do DataFrame usando withColumn
    #     df = df.withColumn('product_treated', unique_word_udf(df[column_name]))
    #     return df

    def apply_transformation(self, df_distinct_products: DataFrame) -> DataFrame:
        fields = [
            "product_aggregated",
            "brand_id",
            "unidades_contenido",
            "med_cant_contenido",
            "source_name",
            "product",
            "product_original",
            "brand_original",
            "product_source_code",
            "product_id",
            "source_table",
        ]

        df_distinct_products = df_distinct_products.select(fields)

        # Remover valores nulos, menos nas que sejam de unidades e medidas contenido
        colunas_para_verificar = [
            coluna
            for coluna in df_distinct_products.columns
            if coluna not in ["unidades_contenido", "med_cant_contenido"]
        ]
        df_distinct_treated = df_distinct_products.dropna(subset=colunas_para_verificar)

        master = self.get_table(
            "brewdat_uc_saz_mlp_featurestore_prod.sales.cleansing_master_treated"
        )

        df_distinct_treated = self.create_column_product_treated(df_distinct_treated)        
        df_distinct_treated = self.text_treatment_dataframe(df_distinct_treated, master)

        return df_distinct_treated

    # This method is mandatory and the final transformation of your dataframe must be returned here
    def definitions(self):
        # Apply your transformation and return your final dataframe
        df_extracted = self.extract_table_distinct_products()

        df_transformed = self.transform_distinct_products(df_extracted)

        df_selected = self.select_columns_distinct_products(df_transformed)

        df_distinct_treated = self.apply_transformation(df_selected)

        #df_distinct_products_treated = self.process_description_column(df_distinct_treated, 'product_treated')

        return df_distinct_treated

# COMMAND ----------

distinct_products_treated = DistinctProductsTreated()

df_distinct_products_treated = distinct_products_treated.definitions()

# COMMAND ----------

df_distinct_products_treated.display()

# COMMAND ----------

# from pyspark.sql.functions import udf
# from pyspark.sql.types import StringType

# class TextProcessor:
#     def unique_word_string(self, text):
#         if text is None:
#             return ""

#         ulist = []
#         [ulist.append(x) for x in text.split(" ") if x not in ulist]
#         return " ".join(ulist)

#     def process_column(self, df, column_name):
#         # Transformar a função Python comum em uma UDF do PySpark
#         unique_word_udf = udf(self.unique_word_string, StringType())
        
#         # Aplicar a UDF na coluna do DataFrame usando withColumn
#         df = df.withColumn('unique_words', unique_word_udf(df[column_name]))
#         return df
    
#     def definitions(self):
#         df = spark.createDataFrame([[1, "gato cachorro gato pássaro cachorro"], [2, "gato cachorro pássaro",]], ["int_column", "text_column"])

#         df_final = self.process_column(df, 'text_column')

#         return df_final

# COMMAND ----------

# # Supondo que você tenha um DataFrame com uma coluna de texto chamada 'text_column'
# df = spark.createDataFrame([[1, "gato cachorro gato pássaro cachorro"], [2, "gato cachorro pássaro",]], ["int_column", "text_column"])

# # Criar uma instância da classe
# processor = TextProcessor()

# # Aplicar o método process_column ao DataFrame
# df_processed = processor.process_column(df, 'text_column')

# # Mostrar os resultados
# df_processed.show(truncate=False)

# COMMAND ----------

# from pyspark.sql.functions import udf
# from pyspark.sql.types import StringType

# # Definir a função UDF fora da classe para evitar problemas de serialização
# def unique_word_string(text):
#     ulist = []
#     [ulist.append(x) for x in text.split(" ") if x not in ulist]
#     return " ".join(ulist)

# # Registrar a UDF
# unique_word_udf = udf(unique_word_string, StringType())

# class TextProcessor:
#     def process_column(self, df, column_name):
#         # Aplicar a UDF na coluna do DataFrame usando withColumn
#         df = df.withColumn('unique_words', unique_word_udf(df[column_name]))
#         return df
    
#     def definitions(self):
#         df = spark.createDataFrame([[1, "gato cachorro gato pássaro cachorro"], [2, "gato cavalo cachorro pássaro cavalo"]], ["int_column", "text_column"])

#         df_final = self.process_column(df, 'text_column')

#         return df_final


# COMMAND ----------

processor = TextProcessor()

df_final = processor.definitions()

# COMMAND ----------

df_final.display()

# COMMAND ----------

# from pyspark.sql.functions import udf
# from pyspark.sql.types import StringType

# class TextProcessor:
#     @staticmethod
#     def unique_word_string(text):
#         ulist = []
#         [ulist.append(x) for x in text.split(" ") if x not in ulist]
#         return " ".join(ulist)

#     def process_column(self, df, column_name):
#         # Transformar a função Python comum em uma UDF do PySpark
#         unique_word_udf = udf(TextProcessor.unique_word_string, StringType())
        
#         # Aplicar a UDF na coluna do DataFrame usando withColumn
#         df = df.withColumn('unique_words', unique_word_udf(df[column_name]))
#         return df
    
#     def definitions(self):
#         df = spark.createDataFrame([[1, "gato cachorro gato pássaro cachorro"], [2, "gato cachorro pássaro"]], ["int_column", "text_column"])

#         df_final = self.process_column(df, 'text_column')

#         return df_final


# COMMAND ----------

# processor = TextProcessor()

# df_final = processor.definitions()

# df_final.display()

# COMMAND ----------


