from airflow.models import DAG
from airflow.providers.apache.spark.operators.spark_jdbc import SparkJDBCOperator
from airflow.providers.apache.spark.operators.spark_sql import SparkSqlOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
import os 

PYSPARK_HOME = os.environ['PYSPARK_HOME']

args = {
    'owner': 'Airflow',
}

with DAG(
    dag_id='spark_read_minio',
    default_args=args,
    schedule_interval=None,
    start_date=days_ago(2),
    tags=['spark', 'minio'],
) as dag:

    submit_job = SparkSubmitOperator(
        task_id="read_minio",
        conn_id='imdb_spark',
        application=f"{PYSPARK_HOME}/read_minio.py", 
        name='read_from_minio',
        executor_memory='4g',
        driver_memory='4g',
        total_executor_cores=6,
        env_vars={'JAVA_HOME': '/usr/lib/jvm/java-8-openjdk-amd64'}
    )

    # [START howto_operator_spark_jdbc]
    # jdbc_to_spark_job = SparkJDBCOperator(
    #     cmd_type='jdbc_to_spark',
    #     jdbc_table="foo",
    #     spark_jars="${SPARK_HOME}/jars/postgresql-42.2.12.jar",
    #     jdbc_driver="org.postgresql.Driver",
    #     metastore_table="bar",
    #     save_mode="overwrite",
    #     save_format="JSON",
    #     task_id="jdbc_to_spark_job",
    # )