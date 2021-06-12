from airflow.models import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
import os 

SPARK_HOME = os.environ['SPARK_HOME']
PYSPARK_DRIVER_PYTHON = '/opt/bitnami/python/bin/python3'

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

    dim_title_desc = SparkSubmitOperator(
        task_id="dim_title_desc",
        conn_id='imdb_spark',
        application=f"{SPARK_HOME}/pyspark_apps/dim_title_desc.py", 
        name='dim_title_desc',
        executor_memory='5g',
        driver_memory='2g',
        total_executor_cores=5,
        # env_vars={
        #     'PYSPARK_DRIVER_PYTHON': PYSPARK_DRIVER_PYTHON,
        #     'PYSPARK_PYTHON': PYSPARK_DRIVER_PYTHON
        # }
    )

    dim_episodes = SparkSubmitOperator(
        task_id="dim_episodes",
        conn_id='imdb_spark',
        application=f"{SPARK_HOME}/pyspark_apps/dim_episodes.py", 
        name='dim_episodes',
        executor_memory='5g',
        driver_memory='2g',
        total_executor_cores=5,
        # env_vars={
        #     'PYSPARK_DRIVER_PYTHON': PYSPARK_DRIVER_PYTHON,
        #     'PYSPARK_PYTHON': PYSPARK_DRIVER_PYTHON
        # }
    )

    dim_casts = SparkSubmitOperator(
        task_id="dim_casts",
        conn_id='imdb_spark',
        application=f"{SPARK_HOME}/pyspark_apps/dim_casts.py", 
        name='dim_casts',
        executor_memory='5g',
        driver_memory='2g',
        total_executor_cores=5,
        # env_vars={
        #     'PYSPARK_DRIVER_PYTHON': PYSPARK_DRIVER_PYTHON,
        #     'PYSPARK_PYTHON': PYSPARK_DRIVER_PYTHON
        # }
    )

    dim_crew = SparkSubmitOperator(
        task_id="dim_crew",
        conn_id='imdb_spark',
        application=f"{SPARK_HOME}/pyspark_apps/dim_crew.py", 
        name='dim_crew',
        executor_memory='5g',
        driver_memory='2g',
        total_executor_cores=5,
        # env_vars={
        #     'PYSPARK_DRIVER_PYTHON': PYSPARK_DRIVER_PYTHON,
        #     'PYSPARK_PYTHON': PYSPARK_DRIVER_PYTHON
        # }
    )

    dim_download_date = SparkSubmitOperator(
        task_id="dim_download_date",
        conn_id='imdb_spark',
        application=f"{SPARK_HOME}/pyspark_apps/dim_download_date.py", 
        name='dim_download_date',
        executor_memory='1g',
        driver_memory='1g',
        total_executor_cores=1,
        env_vars={
            'PYSPARK_DRIVER_PYTHON': PYSPARK_DRIVER_PYTHON,
            'PYSPARK_PYTHON': PYSPARK_DRIVER_PYTHON
        }
    )

    dim_download_date >> dim_title_desc >> dim_episodes >> dim_casts >> dim_crew
