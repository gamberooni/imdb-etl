from airflow.models import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
import os 

SPARK_HOME = os.environ['SPARK_HOME']

args = {
    'owner': 'imdb',
}

with DAG(
    dag_id='imdb_etl',
    default_args=args,
    schedule_interval=None,
    start_date=days_ago(2),
    tags=['spark', 'minio', 'postgres'],
) as dag:

    titles = SparkSubmitOperator(
        task_id="titles",
        conn_id='imdb_spark',
        application=f"{SPARK_HOME}/pyspark_apps/titles.py", 
        name='titles',
        executor_memory='5g',
        driver_memory='2g',
        total_executor_cores=5,
    )

    episodes = SparkSubmitOperator(
        task_id="episodes",
        conn_id='imdb_spark',
        application=f"{SPARK_HOME}/pyspark_apps/episodes.py", 
        name='episodes',
        executor_memory='5g',
        driver_memory='2g',
        total_executor_cores=5,
    )

    casts = SparkSubmitOperator(
        task_id="casts",
        conn_id='imdb_spark',
        application=f"{SPARK_HOME}/pyspark_apps/casts.py", 
        name='casts',
        executor_memory='5g',
        driver_memory='2g',
        total_executor_cores=5,
    )

    crew = SparkSubmitOperator(
        task_id="crew",
        conn_id='imdb_spark',
        application=f"{SPARK_HOME}/pyspark_apps/crew.py", 
        name='crew',
        executor_memory='5g',
        driver_memory='2g',
        total_executor_cores=5,
    )

    download_date = SparkSubmitOperator(
        task_id="download_date",
        conn_id='imdb_spark',
        application=f"{SPARK_HOME}/pyspark_apps/download_date.py", 
        name='download_date',
        executor_memory='1g',
        driver_memory='1g',
        total_executor_cores=1,
    )

    download_date >> titles >> episodes >> casts >> crew
