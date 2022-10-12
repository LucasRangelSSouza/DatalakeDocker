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


path_business = '/usr/local/lake/business/enem/fato_enem/'
path_trusted = '/usr/local/lake/trusted/enem/'

clearFolder(path_business)
createFolder(path_business)

df_parquet = spark.read.load(path= f'{path_trusted}*.parquet', format='parquet')

#SELECIONANDO OS CAMPOS QUE IREMOS USAR
df_parquet = df_parquet.select(["COD_SEXO","COD_RACA","COD_ESCOLA","COD_ENSINO",
"COD_MUNICIPIO","COD_UF","COD_ZONA","COD_SITUACAO_ESCOLA","COD_INSCRICAO","VLR_NOTA_REDACAO",
"VLR_NOTA_CN","VLR_NOTA_CH","VLR_NOTA_LC","VLR_NOTA_MT","FLG_PRESENCA_CN","FLG_PRESENCA_CH",
"FLG_PRESENCA_LC","FLG_PRESENCA_MT"])
          
df_parquet = df_parquet.distinct()


df_parquet.repartition(10).write.mode('overwrite').parquet(path_business)
clean(path_business)

