from airflow.models import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
import os 

SPARK_HOME = os.environ['SPARK_HOME']
MINIO_HOST = Variable.get("minio_host")
MINIO_ACCESS_KEY = Variable.get("minio_access_key")
MINIO_SECRET_KEY = Variable.get("minio_secret_key")
POSTGRES_HOST = Variable.get("postgres_host")
POSTGRES_USER = Variable.get("postgres_user")
POSTGRES_PASSWORD = Variable.get("postgres_password")
IMDB_BUCKET_NAME = Variable.get("imdb_bucket_name")
IMDB_OBJECT_PREFIX = Variable.get("imdb_object_prefix")

args = {
    'owner': 'imdb',
}

with DAG(
    dag_id='imdb_etl',
    default_args=args,
    schedule_interval=None,
    start_date=days_ago(2),
    tags=['spark', 'minio', 'postgres', 'imdb'],
) as dag:

    titles = SparkSubmitOperator(
        task_id="titles",
        conn_id='imdb_spark',
        application=f"{SPARK_HOME}/pyspark_apps/titles.py", 
        name='titles',
        executor_memory='6g',
        driver_memory='2g',
        total_executor_cores=6,
        env_vars={
            "MINIO_HOST": MINIO_HOST,
            "MINIO_ACCESS_KEY": MINIO_ACCESS_KEY,
            "MINIO_SECRET_KEY": MINIO_SECRET_KEY,
            "POSTGRES_HOST": POSTGRES_HOST,
            "POSTGRES_USER": POSTGRES_USER,
            "POSTGRES_PASSWORD": POSTGRES_PASSWORD,
            "IMDB_BUCKET_NAME": IMDB_BUCKET_NAME,
            "IMDB_OBJECT_PREFIX": IMDB_OBJECT_PREFIX 
        }
    )

    episodes = SparkSubmitOperator(
        task_id="episodes",
        conn_id='imdb_spark',
        application=f"{SPARK_HOME}/pyspark_apps/episodes.py", 
        name='episodes',
        executor_memory='6g',
        driver_memory='2g',
        total_executor_cores=6,
        env_vars={
            "MINIO_HOST": MINIO_HOST,
            "MINIO_ACCESS_KEY": MINIO_ACCESS_KEY,
            "MINIO_SECRET_KEY": MINIO_SECRET_KEY,
            "POSTGRES_HOST": POSTGRES_HOST,
            "POSTGRES_USER": POSTGRES_USER,
            "POSTGRES_PASSWORD": POSTGRES_PASSWORD,
            "IMDB_BUCKET_NAME": IMDB_BUCKET_NAME,
            "IMDB_OBJECT_PREFIX": IMDB_OBJECT_PREFIX 
        }
    )

    casts = SparkSubmitOperator(
        task_id="casts",
        conn_id='imdb_spark',
        application=f"{SPARK_HOME}/pyspark_apps/casts.py", 
        name='casts',
        executor_memory='6g',
        driver_memory='2g',
        total_executor_cores=6,
        env_vars={
            "MINIO_HOST": MINIO_HOST,
            "MINIO_ACCESS_KEY": MINIO_ACCESS_KEY,
            "MINIO_SECRET_KEY": MINIO_SECRET_KEY,
            "POSTGRES_HOST": POSTGRES_HOST,
            "POSTGRES_USER": POSTGRES_USER,
            "POSTGRES_PASSWORD": POSTGRES_PASSWORD,
            "IMDB_BUCKET_NAME": IMDB_BUCKET_NAME,
            "IMDB_OBJECT_PREFIX": IMDB_OBJECT_PREFIX 
        }  
    )

    crew = SparkSubmitOperator(
        task_id="crew",
        conn_id='imdb_spark',
        application=f"{SPARK_HOME}/pyspark_apps/crew.py", 
        name='crew',
        executor_memory='6g',
        driver_memory='2g',
        total_executor_cores=6,
        env_vars={
            "MINIO_HOST": MINIO_HOST,
            "MINIO_ACCESS_KEY": MINIO_ACCESS_KEY,
            "MINIO_SECRET_KEY": MINIO_SECRET_KEY,
            "POSTGRES_HOST": POSTGRES_HOST,
            "POSTGRES_USER": POSTGRES_USER,
            "POSTGRES_PASSWORD": POSTGRES_PASSWORD,
            "IMDB_BUCKET_NAME": IMDB_BUCKET_NAME,
            "IMDB_OBJECT_PREFIX": IMDB_OBJECT_PREFIX 
        }   
    )

    download_date = SparkSubmitOperator(
        task_id="download_date",
        conn_id='imdb_spark',
        application=f"{SPARK_HOME}/pyspark_apps/download_date.py", 
        name='download_date',
        executor_memory='1g',
        driver_memory='1g',
        total_executor_cores=1,
        env_vars={
            "MINIO_HOST": MINIO_HOST,
            "MINIO_ACCESS_KEY": MINIO_ACCESS_KEY,
            "MINIO_SECRET_KEY": MINIO_SECRET_KEY,
            "POSTGRES_HOST": POSTGRES_HOST,
            "POSTGRES_USER": POSTGRES_USER,
            "POSTGRES_PASSWORD": POSTGRES_PASSWORD,
            "IMDB_BUCKET_NAME": IMDB_BUCKET_NAME,
            "IMDB_OBJECT_PREFIX": IMDB_OBJECT_PREFIX 
        }  
    )

    download_date >> titles >> episodes >> casts >> crew
