from airflow import settings
from airflow.models import Connection
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
import logging

logging.basicConfig(
    format='%(levelname)s: %(asctime)s - %(message)s', 
    datefmt='%d-%b-%y %H:%M:%S', 
    level=logging.INFO
    )

MINIO_ACCESS_KEY = Variable.get("minio_access_key")
MINIO_SECRET_KEY = Variable.get("minio_secret_key")
POSTGRES_USER = Variable.get("postgres_user")
POSTGRES_PASSWORD = Variable.get("postgres_password")

args = {
    'owner': 'imdb',
}

def create_conn(conn_id, conn_type, host, login=None, password=None, port=None, schema=None, extra=None):
    conn = Connection(
        conn_id=conn_id,
        conn_type=conn_type,
        host=host,
        port=port,
        schema=schema,
        password=password,
        login=login,
        extra=extra
    )
    session = settings.Session()
    conn_name = session\
    .query(Connection)\
    .filter(Connection.conn_id == conn.conn_id)\
    .first()

    if str(conn_name) == str(conn_id):
        return logging.info(f"Connection {conn_id} already exists")

    session.add(conn)
    session.commit()
    logging.info(Connection.log_info(conn))
    logging.info(f'Connection {conn_id} is created')

with DAG(
    dag_id='create_connections',
    schedule_interval=None,
    default_args=args,
    start_date=days_ago(2),
    max_active_runs=1,
    tags=['connections', 'imdb'],
) as dag:

    create_minio_cnx = PythonOperator(
        task_id="create_minio_cnx", 
        python_callable=create_conn,
        op_kwargs={
            "conn_id": "imdb_minio", 
            "conn_type": "s3",
            "host": "",
            "login": MINIO_ACCESS_KEY,
            "password": MINIO_SECRET_KEY,
            "extra": "{\"host\": \"http://minio:9000\"}"
        },
    )

    create_postgres_cnx = PythonOperator(
        task_id="create_postgres_cnx", 
        python_callable=create_conn,
        op_kwargs={
            "conn_id": "imdb_postgres", 
            "conn_type": "postgres",
            "host": "imdb_postgres",
            "login": POSTGRES_USER,
            "password": POSTGRES_PASSWORD,
            "schema": "imdb",
            "port": 5432,
        },
    )    

    create_spark_cnx = PythonOperator(
        task_id="create_spark_cnx", 
        python_callable=create_conn,
        op_kwargs={
            "conn_id": "imdb_spark", 
            "conn_type": "spark",
            "host": "spark://spark",
            "port": 7077,
        },
    )        