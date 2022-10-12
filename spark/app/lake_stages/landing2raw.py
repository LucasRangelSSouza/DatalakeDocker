import sys
from tarfile import BLOCKSIZE
from pyspark.sql import SparkSession
import os
import shutil

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


csv_file = '/usr/local/lake/landing/microdados_enem/DADOS/MICRODADOS_ENEM_2020.csv'
path_raw = '/usr/local/lake/raw/enem/'

clearFolder(path_raw)
createFolder(path_raw)
df_csv = spark.read.options(header = 'True', delimiter=';', encoding = 'ISO-8859-1').csv(csv_file)
df_csv.repartition(10).write.mode('overwrite').parquet(path_raw)
clean(path_raw)