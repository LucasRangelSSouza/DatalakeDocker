from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os
import shutil
from zipfile import ZipFile
import requests
from io import BytesIO

SPARK_MASTER = "spark://spark:7077"
MICRODADOS_ENEM_URL = "https://download.inep.gov.br/microdados/microdados_enem_2020.zip"

def createFolder(path):

    if not os.path.exists(path):
        os.makedirs(path)

def clearFolder(path):
    if os.path.exists(path):
        shutil.rmtree(path)

def downloadFileFunc():
    clearFolder('/usr/local/lake/landing/microdados_enem')
    #Realizando requisição na url do enem
    bytesFile = BytesIO( requests.get(MICRODADOS_ENEM_URL, verify=False).content )
    zipfile = ZipFile(bytesFile)
    zipfile.extractall('/usr/local/lake/landing/microdados_enem/')

now = datetime.now()

default_args = {
    "owner": "Lucas_Rangel",
    "depends_on_past": False,
    "start_date": datetime(now.year, now.month, now.day),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 5,
    "retry_delay": timedelta(minutes=1)
}


with DAG('pipeline-microdados', default_args=default_args, schedule_interval=None) as dag:

    start = DummyOperator(task_id="start")
    middle = DummyOperator(task_id="go")
    end = DummyOperator(task_id="end")

   

    downloadFile = PythonOperator(
        task_id = 'Download-descompacta-Microdados',
        python_callable = downloadFileFunc,
        op_kwargs = {}
    )
    
       
    
    
    landing2Raw = SparkSubmitOperator(
        task_id="landing2raw",
        application="/usr/local/spark/app/lake_stages/landing2raw.py",
        name='landing2raw',
        conn_id="spark_default",
        verbose=1,
        conf={"spark.master":SPARK_MASTER},
        application_args=[],
        dag=dag)
    
    

    raw2trusted = SparkSubmitOperator(
        task_id="raw2trusted",
        application="/usr/local/spark/app/lake_stages/raw2trusted.py",
        name='raw2trusted',
        conn_id="spark_default",
        verbose=1,
        conf={"spark.master":SPARK_MASTER},
        application_args=[],
        dag=dag)


    jobsBusiness = []
    jobsPostgress = []
    for artifact in ['fato_enem','dim_sexo','dim_raca','dim_escola',
                    'dim_ensino','dim_uf','dim_municipio','dim_zona',
                    'dim_situacao_escola']:

        businessJob = SparkSubmitOperator(
            task_id=artifact,
            application=f"/usr/local/spark/app/star_schema/{artifact}.py",
            name=artifact,
            conn_id="spark_default",
            verbose=1,
            conf={"spark.master":SPARK_MASTER},
            application_args=[],
            dag=dag)

        savePostsgress = SparkSubmitOperator(
            task_id=f"save_{artifact}_database",
            application="/usr/local/spark/app/lake_stages/savePostgressDB.py", # Spark application path created in airflow and spark cluster
            name=f"save_{artifact}_database",
            conn_id="spark_default",
            verbose=1,
            conf={"spark.master":SPARK_MASTER},
            application_args=[artifact,f'/usr/local/lake/business/enem/{artifact}/',"jdbc:postgresql://postgres/test","test","postgres"],
            jars="/usr/local/spark/resources/jars/postgresql-9.4.1207.jar",
            driver_class_path="/usr/local/spark/resources/jars/postgresql-9.4.1207.jar",
            dag=dag)

        jobsBusiness.append(businessJob)
        jobsPostgress.append(savePostsgress)

    start >> downloadFile >> landing2Raw >> raw2trusted  >> jobsBusiness>> middle>> jobsPostgress >> end