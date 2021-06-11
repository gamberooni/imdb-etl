from airflow.models import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.amazon.aws.operators.s3_bucket import S3CreateBucketOperator
from airflow.utils.dates import days_ago
import os 

SPARK_HOME = os.environ['SPARK_HOME']
BUCKET_NAME = 'testing'

args = {
    'owner': 'Airflow',
}

with DAG(
    dag_id='test_spark_minio',
    default_args=args,
    schedule_interval=None,
    start_date=days_ago(2),
    tags=['spark', 'minio'],
) as dag:

    test_spark_minio = SparkSubmitOperator(
        task_id="test_spark_minio",
        conn_id='imdb_spark',
        application=f"{SPARK_HOME}/pyspark_apps/test_spark_minio.py", 
        name='test_spark_minio',
        executor_memory='1g',
        driver_memory='1g',
        total_executor_cores=1
    )

    # create_bucket = S3CreateBucketOperator(
    #     task_id='create_bucket',
    #     aws_conn_id='imdb_minio',
    #     bucket_name=BUCKET_NAME,
    # )

    test_spark_minio