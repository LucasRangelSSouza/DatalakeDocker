import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, col, to_timestamp
from pyspark.sql.types import DoubleType

# Create spark session
spark = (SparkSession
    .builder
    .getOrCreate()
)

####################################
# Parameters
####################################
table = sys.argv[1]
path = sys.argv[2]
postgres_db = sys.argv[3]
postgres_user = sys.argv[4]
postgres_pwd = sys.argv[5]

####################################
# Read Data
####################################
print('reading parquet')
df_parquet = spark.read.load(path= f'{path}*.parquet', format='parquet')

####################################
# Load data to Postgres
####################################
print('saving postgress')
(
    df_parquet.write
    .format("jdbc")
    .option("url", postgres_db)
    .option("dbtable", f"public.{table}")
    .option("user", postgres_user)
    .option("password", postgres_pwd)
    .mode("overwrite")
    .save()
)