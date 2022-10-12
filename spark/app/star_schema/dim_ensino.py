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


spark = (SparkSession
    .builder
    .getOrCreate()
)
sc = spark.sparkContext
sc.setLogLevel("WARN")


path_business = '/usr/local/lake/business/enem/dim_ensino/'
path_trusted = '/usr/local/lake/trusted/enem/'

clearFolder(path_business)
createFolder(path_business)

df_parquet = spark.read.load(path= f'{path_trusted}*.parquet', format='parquet')

#SELECIONANDO OS CAMPOS QUE IREMOS USAR
df_parquet = df_parquet.select(["COD_ENSINO","DESC_ENSINO"])
df_parquet = df_parquet.distinct()
df_parquet.repartition(1).write.mode('overwrite').parquet(path_business)

