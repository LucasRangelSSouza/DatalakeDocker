from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.python_operator import PythonOperator
from airflow import settings
from airflow.models import Connection
from datetime import datetime, timedelta
import os

def createLandingFunc():
    newpath = r'/usr/local/lake/landing' 
    if not os.path.exists(newpath):
        os.makedirs(newpath)

def createRawFunc():
    newpath = r'/usr/local/lake/raw' 
    if not os.path.exists(newpath):
        os.makedirs(newpath)


def createTrustedFunc():
    newpath = r'/usr/local/lake/trusted' 
    if not os.path.exists(newpath):
        os.makedirs(newpath)

def createBusinessFunc():
    newpath = r'/usr/local/lake/business' 
    if not os.path.exists(newpath):
        os.makedirs(newpath)

def configureSparkFunc():
    conn = Connection(
            conn_id='spark_default',
            host='spark://spark',
            port=7077,
            extra='{"queue": "root.default"}'
    ) 
    session = settings.Session() 
    session.add(conn)
    session.commit() 

now = datetime.now()

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(now.year, now.month, now.day),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}


with DAG('Prepare_lakehouse', default_args=default_args, schedule_interval=None) as dag:

    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")

    createLanding = PythonOperator(
        task_id = 'Create_Landing_Zone',
        python_callable = createLandingFunc,
        op_kwargs = {}
    )

    createRaw = PythonOperator(
            task_id = 'Create_Raw_Zone',
            python_callable = createRawFunc,
            op_kwargs = {}
        )

    createTrusted = PythonOperator(
            task_id = 'Create_Trusted_Zone',
            python_callable = createTrustedFunc,
            op_kwargs = {}
        )

    createBusiness = PythonOperator(
            task_id = 'Create_business_Zone',
            python_callable = createBusinessFunc,
            op_kwargs = {}
        )

    configureSpark = PythonOperator(
            task_id = 'configureSpark',
            python_callable = configureSparkFunc,
            op_kwargs = {}
        )

    start >> [createLanding,createRaw,createTrusted,createBusiness,configureSpark] >> end