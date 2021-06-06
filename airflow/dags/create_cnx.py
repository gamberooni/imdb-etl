from airflow import settings
from airflow.models import Connection
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import logging

logging.basicConfig(
    format='%(levelname)s: %(asctime)s - %(message)s', 
    datefmt='%d-%b-%y %H:%M:%S', 
    level=logging.INFO
    )

def create_conn(conn_id, conn_type, host, login, password, port=None, schema=None, extra=None):
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
    start_date=days_ago(2),
    max_active_runs=1,
    tags=['connections'],
) as dag:

    create_minio_cnx = PythonOperator(
        task_id="create_minio_cnx", 
        python_callable=create_conn,
        op_kwargs={
            "conn_id": "imdb_minio", 
            "conn_type": "s3",
            "host": "",
            "login": "admin",
            "password": "password",
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
            "login": "admin",
            "password": "password",
            "schema": "imdb",
            "port": 5432,
        },
    )    