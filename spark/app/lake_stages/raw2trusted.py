import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import when
import os
import shutil
from pyspark.sql.types import StringType,BooleanType,DateType,IntegerType,FloatType

def createFolder(path):

    if not os.path.exists(path):
        os.makedirs(path)

def clearFolder(path):
    if os.path.exists(path):
        shutil.rmtree(path)

def clean(path):
    test = os.listdir(path)
    for item in test:
        if item.endswith(".crc"):
            os.remove(os.path.join(path, item))

spark = (SparkSession
    .builder
    .getOrCreate()
)
sc = spark.sparkContext
sc.setLogLevel("WARN")


path_raw = '/usr/local/lake/raw/enem/'
path_trusted = '/usr/local/lake/trusted/enem/'

clearFolder(path_trusted)
createFolder(path_trusted)

df_parquet = spark.read.load(path= f'{path_raw}*.parquet', format='parquet')

#SELECIONANDO OS CAMPOS QUE IREMOS USAR

originalCols = ["NU_INSCRICAO","TP_SEXO","TP_COR_RACA","TP_ESCOLA","TP_ENSINO",
              "CO_MUNICIPIO_ESC","NO_MUNICIPIO_ESC","CO_UF_ESC","SG_UF_ESC","TP_LOCALIZACAO_ESC",
              "TP_SIT_FUNC_ESC","NU_NOTA_REDACAO","NU_NOTA_CN","NU_NOTA_CH","NU_NOTA_LC", "NU_NOTA_MT", 
              "TP_PRESENCA_CN","TP_PRESENCA_CH", "TP_PRESENCA_LC", "TP_PRESENCA_MT"]

df_parquet = df_parquet.select(*originalCols)

df_parquet = df_parquet.withColumn("COD_INSCRICAO", when(df_parquet.NU_INSCRICAO.isNull() ,-1)
                                 .otherwise(df_parquet.NU_INSCRICAO).cast(IntegerType()))


df_parquet = df_parquet.withColumn("COD_SEXO", when(df_parquet.TP_SEXO == "M",1)
                                 .when(df_parquet.TP_SEXO == "F",2)
                                 .when(df_parquet.TP_SEXO.isNull() ,0)
                                 .otherwise(0).cast(IntegerType()))

df_parquet = df_parquet.withColumn("DESC_SEXO", when(df_parquet.TP_SEXO == "M","MASCULINO")
                                 .when(df_parquet.TP_SEXO == "F","FEMININO")
                                 .when(df_parquet.TP_SEXO.isNull() ,"NÃO INFORMADO")
                                 .otherwise("NÃO INFORMADO").cast(StringType()))                                

df_parquet = df_parquet.withColumn("FLG_SEXO", when(df_parquet.TP_SEXO == "M","M")
                                 .when(df_parquet.TP_SEXO == "F","F")
                                 .when(df_parquet.TP_SEXO.isNull() ,"N")
                                 .otherwise("N").cast(StringType()))            

df_parquet = df_parquet.withColumn("COD_RACA", when(df_parquet.TP_COR_RACA.isNull() ,-1)
                                 .otherwise(df_parquet.TP_COR_RACA).cast(IntegerType()))

df_parquet = df_parquet.withColumn("DESC_RACA", when(df_parquet.TP_COR_RACA == 0,"NÃO DECLARADA")
                                 .when(df_parquet.TP_COR_RACA == 1,"BRANCA")
                                 .when(df_parquet.TP_COR_RACA == 2,"PRETA")
                                 .when(df_parquet.TP_COR_RACA == 3,"PARDA")
                                 .when(df_parquet.TP_COR_RACA == 4,"AMARELA")
                                 .when(df_parquet.TP_COR_RACA == 5,"INDIGENA")
                                 .when(df_parquet.TP_COR_RACA.isNull() ,"NÃO INFORMADO")
                                 .otherwise("NÃO INFORMADO").cast(StringType()))    

df_parquet = df_parquet.withColumn("COD_ESCOLA", when(df_parquet.TP_ESCOLA.isNull() ,-1)
                                 .otherwise(df_parquet.TP_ESCOLA).cast(IntegerType()))

df_parquet = df_parquet.withColumn("DESC_ESCOLA", when(df_parquet.TP_ESCOLA == 1,"NÃO RESPONDEU")
                                 .when(df_parquet.TP_ESCOLA == 2,"PUBLICA")
                                 .when(df_parquet.TP_ESCOLA == 3,"PRIVADA")
                                 .when(df_parquet.TP_ESCOLA == 4,"EXTERIOR")
                                 .when(df_parquet.TP_ESCOLA.isNull() ,"NÃO INFORMADO")
                                 .otherwise("NÃO INFORMADO").cast(StringType()))    

df_parquet = df_parquet.withColumn("COD_ENSINO", when(df_parquet.TP_ENSINO.isNull() ,-1)
                                 .otherwise(df_parquet.TP_ENSINO).cast(IntegerType()))

df_parquet = df_parquet.withColumn("DESC_ENSINO", when(df_parquet.TP_ENSINO == 1,"ENSINO REGULAR")
                                 .when(df_parquet.TP_ENSINO == 2,"EDUCAÇÃO ESPECIAL - MODALIDADE SUBSTITUTIVA")
                                 .when(df_parquet.TP_ENSINO == 3,"EDUCAÇÃO DE JOVENS E ADULTOS")
                                 .when(df_parquet.TP_ENSINO.isNull() ,"NÃO INFORMADO")
                                 .otherwise("NÃO INFORMADO").cast(StringType())) 

df_parquet = df_parquet.withColumn("COD_MUNICIPIO", when(df_parquet.CO_MUNICIPIO_ESC.isNull() ,-1)
                                 .otherwise(df_parquet.CO_MUNICIPIO_ESC).cast(IntegerType()))

df_parquet = df_parquet.withColumn("DESC_MUNICIPIO", when(df_parquet.NO_MUNICIPIO_ESC.isNull() ,"NÃO INFORMADO")
                                 .otherwise(df_parquet.NO_MUNICIPIO_ESC).cast(StringType()))


df_parquet = df_parquet.withColumn("COD_UF", when(df_parquet.CO_UF_ESC.isNull() ,-1)
                                 .otherwise(df_parquet.CO_UF_ESC).cast(IntegerType()))

df_parquet = df_parquet.withColumn("DESC_UF", when(df_parquet.SG_UF_ESC.isNull() ,"NÃO INFORMADO")
                                 .otherwise(df_parquet.SG_UF_ESC).cast(StringType()))

df_parquet = df_parquet.withColumn("COD_ZONA", when(df_parquet.TP_LOCALIZACAO_ESC.isNull() ,-1)
                                 .otherwise(df_parquet.TP_LOCALIZACAO_ESC).cast(IntegerType()))

df_parquet = df_parquet.withColumn("DESC_ZONA", when(df_parquet.TP_LOCALIZACAO_ESC == 1,"URBANA")
                                 .when(df_parquet.TP_LOCALIZACAO_ESC == 2,"RURAL")
                                 .when(df_parquet.TP_LOCALIZACAO_ESC.isNull() ,"NÃO INFORMADO")
                                 .otherwise("NÃO INFORMADO").cast(StringType()))       

df_parquet = df_parquet.withColumn("COD_SITUACAO_ESCOLA", when(df_parquet.TP_SIT_FUNC_ESC.isNull() ,-1)
                                 .otherwise(df_parquet.TP_SIT_FUNC_ESC).cast(IntegerType()))

df_parquet = df_parquet.withColumn("DESC_SITUACAO_ESCOLA", when(df_parquet.TP_SIT_FUNC_ESC == 1,"EM ATIVIDADE")
                                 .when(df_parquet.TP_SIT_FUNC_ESC == 2,"PARALISADA")
                                 .when(df_parquet.TP_SIT_FUNC_ESC == 3,"EXTINTA")
                                 .when(df_parquet.TP_SIT_FUNC_ESC == 3,"ESCOLA EXTINTA EM ANOS ANTERIORES")
                                 .when(df_parquet.TP_SIT_FUNC_ESC.isNull() ,"NÃO INFORMADO")
                                 .otherwise("NÃO INFORMADO").cast(StringType()))    

df_parquet = df_parquet.withColumn("VLR_NOTA_REDACAO", when(df_parquet.NU_NOTA_REDACAO.isNull() ,0.0)
                                 .otherwise(df_parquet.NU_NOTA_REDACAO).cast(FloatType()))

df_parquet = df_parquet.withColumn("VLR_NOTA_CN", when(df_parquet.NU_NOTA_CN.isNull() ,0.0)
                                 .otherwise(df_parquet.NU_NOTA_CN).cast(FloatType()))

df_parquet = df_parquet.withColumn("VLR_NOTA_CH", when(df_parquet.NU_NOTA_CH.isNull() ,0.0)
                                 .otherwise(df_parquet.NU_NOTA_CH).cast(FloatType()))

df_parquet = df_parquet.withColumn("VLR_NOTA_LC", when(df_parquet.NU_NOTA_LC.isNull() ,0.0)
                                 .otherwise(df_parquet.NU_NOTA_LC).cast(FloatType()))

df_parquet = df_parquet.withColumn("VLR_NOTA_MT", when(df_parquet.NU_NOTA_MT.isNull() ,0.0)
                                 .otherwise(df_parquet.NU_NOTA_MT).cast(FloatType()))

df_parquet = df_parquet.withColumn("FLG_PRESENCA_CN", when(df_parquet.TP_PRESENCA_CN.isNull() ,-1)
                                 .otherwise(df_parquet.TP_PRESENCA_CN).cast(IntegerType()))

df_parquet = df_parquet.withColumn("FLG_PRESENCA_CH", when(df_parquet.TP_PRESENCA_CH.isNull() ,-1)
                                 .otherwise(df_parquet.TP_PRESENCA_CH).cast(IntegerType()))

df_parquet = df_parquet.withColumn("FLG_PRESENCA_LC", when(df_parquet.TP_PRESENCA_LC.isNull() ,-1)
                                 .otherwise(df_parquet.TP_PRESENCA_LC).cast(IntegerType()))

df_parquet = df_parquet.withColumn("FLG_PRESENCA_MT", when(df_parquet.TP_PRESENCA_MT.isNull() ,-1)
                                 .otherwise(df_parquet.TP_PRESENCA_MT).cast(IntegerType()))

df_parquet = df_parquet.drop(*originalCols)
df_parquet = df_parquet.distinct()
df_parquet.repartition(10).write.mode('overwrite').parquet(path_trusted)
clean(path_trusted)

