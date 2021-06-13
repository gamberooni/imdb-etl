import datetime
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
import os

args = {"owner": "imdb"}

with DAG(
    dag_id="create_tables",
    start_date=datetime.datetime(2020, 2, 2),
    schedule_interval="@once",
    default_args=args,
    catchup=False,
    tags=['postgres'],
) as dag:

    create_download_date_table = PostgresOperator(
        task_id="create_download_date_table",
        postgres_conn_id="imdb_postgres",
        sql="""
        CREATE TABLE IF NOT EXISTS download_date (
            id SERIAL PRIMARY KEY,
            year SMALLINT NOT NULL,
            month SMALLINT NOT NULL,
            day SMALLINT NOT NULL
        );
        """,
    )

    create_episodes_table = PostgresOperator(
        task_id="create_episodes_table",
        postgres_conn_id="imdb_postgres",
        sql="""
        CREATE TABLE IF NOT EXISTS episodes (
            id SERIAL PRIMARY KEY,
            title_id INT REFERENCES titles (id),
            season SMALLINT,
            total_episodes SMALLINT
        );
        """,
    )    

    create_titles_table = PostgresOperator(
        task_id="create_titles_table",
        postgres_conn_id="imdb_postgres",
        sql="""
        CREATE TABLE IF NOT EXISTS titles (
            id SERIAL PRIMARY KEY,
            download_date_id INT,
            tconst VARCHAR(20) UNIQUE NOT NULL,
            type VARCHAR(40),
            primary_title VARCHAR(800) NOT NULL,
            original_title VARCHAR(800),
            is_adult BOOLEAN,
            start_year SMALLINT,
            end_year SMALLINT,
            runtime_minutes SMALLINT,
            av_rating FLOAT(2),
            num_votes INT,
            genre_1 VARCHAR(25),
            genre_2 VARCHAR(25),
            genre_3 VARCHAR(25)
        );
        """,
    )    

    create_casts_table = PostgresOperator(
        task_id="create_casts_table",
        postgres_conn_id="imdb_postgres",
        sql="""
        CREATE TABLE IF NOT EXISTS casts (
            id SERIAL PRIMARY KEY,
            name VARCHAR(200) NOT NULL,
            age SMALLINT,
            is_alive BOOLEAN,
            nconst VARCHAR(15) NOT NULL
        );
        """,
    )        

    create_crew_table = PostgresOperator(
        task_id="create_crew_table",
        postgres_conn_id="imdb_postgres",
        sql="""
        CREATE TABLE IF NOT EXISTS crew (
            id SERIAL PRIMARY KEY,
            name VARCHAR(200) NOT NULL,
            age SMALLINT,
            is_alive BOOLEAN,
            nconst VARCHAR(15) NOT NULL
        );
        """,
    )    

    create_titles_casts_table = PostgresOperator(
        task_id="create_titles_casts_table",
        postgres_conn_id="imdb_postgres",
        sql="""
        CREATE TABLE IF NOT EXISTS titles_casts (
            title_id INT REFERENCES titles (id),
            cast_id INT REFERENCES casts (id),
            character VARCHAR(300)
        );
        """,
    )      

    create_titles_crew_table = PostgresOperator(
        task_id="create_titles_crew_table",
        postgres_conn_id="imdb_postgres",
        sql="""
        CREATE TABLE IF NOT EXISTS titles_crew (
            title_id INT REFERENCES titles (id),
            crew_id INT REFERENCES crew (id),
            role VARCHAR(50)
        );
        """,
    )                             

    set_fkey = PostgresOperator(
        task_id="set_fkey",
        postgres_conn_id="imdb_postgres",
        sql="""
        ALTER TABLE titles
        ADD CONSTRAINT titles_download_date_id_fkey FOREIGN KEY (download_date_id)
        REFERENCES download_date (id);
        """,
    )                  

    drop_schema = PostgresOperator(
        task_id="drop_schema",
        postgres_conn_id="imdb_postgres",
        sql="""
        DROP SCHEMA IF EXISTS public CASCADE;
        """,
    )                                                 

    create_schema = PostgresOperator(
        task_id="create_schema",
        postgres_conn_id="imdb_postgres",
        sql="""
        CREATE SCHEMA IF NOT EXISTS public;
        """,
    )   

    drop_schema >> create_schema 
    create_schema >> create_titles_table >> set_fkey
    create_schema >> create_download_date_table >> set_fkey
    create_titles_table >> create_episodes_table  
    create_titles_table >> create_crew_table >> create_titles_crew_table 
    create_titles_table >> create_casts_table >> create_titles_casts_table 
