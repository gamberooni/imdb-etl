import datetime
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
import os

default_args = {"owner": "airflow"}

with DAG(
    dag_id="create_tables",
    start_date=datetime.datetime(2020, 2, 2),
    schedule_interval="@once",
    default_args=default_args,
    catchup=False,
    tags=['postgres'],
) as dag:

    create_dim_download_date_table = PostgresOperator(
        task_id="create_dim_download_date_table",
        postgres_conn_id="imdb_postgres",
        sql="""
        CREATE TABLE IF NOT EXISTS dim_download_date (
            id SERIAL PRIMARY KEY,
            year SMALLINT NOT NULL,
            month SMALLINT NOT NULL,
            day SMALLINT NOT NULL
        );
        """,
    )

    create_dim_episodes_table = PostgresOperator(
        task_id="create_dim_episodes_table",
        postgres_conn_id="imdb_postgres",
        sql="""
        CREATE TABLE IF NOT EXISTS dim_episodes (
            id SERIAL PRIMARY KEY,
            season SMALLINT,
            total_episodes SMALLINT,
            tconst VARCHAR(20) NOT NULL
        );
        """,
    )    

    create_dim_title_desc_table = PostgresOperator(
        task_id="create_dim_title_desc_table",
        postgres_conn_id="imdb_postgres",
        sql="""
        CREATE TABLE IF NOT EXISTS dim_title_desc (
            id SERIAL PRIMARY KEY,
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

    create_dim_casts_table = PostgresOperator(
        task_id="create_dim_casts_table",
        postgres_conn_id="imdb_postgres",
        sql="""
        CREATE TABLE IF NOT EXISTS dim_casts (
            id SERIAL PRIMARY KEY,
            name VARCHAR(200) NOT NULL,
            age SMALLINT,
            is_alive BOOLEAN,
            nconst VARCHAR(15) NOT NULL
        );
        """,
    )        
 
    create_fact_titles_table = PostgresOperator(
        task_id="create_fact_titles_table",
        postgres_conn_id="imdb_postgres",
        sql="""
        CREATE TABLE IF NOT EXISTS fact_titles (
            id SERIAL PRIMARY KEY,
            download_date_id INT REFERENCES dim_download_date (id),
            episode_id INT REFERENCES dim_episodes (id),
            title_desc_id INT REFERENCES dim_title_desc (id),                   
            daily_avRatingChange FLOAT(2),
            weekly_avRatingChange FLOAT(2),
            daily_numVotesChange INT,
            weekly_numVotesChange INT
        );
        """,
    )      

    create_titles_crew_table = PostgresOperator(
        task_id="create_titles_crew_table",
        postgres_conn_id="imdb_postgres",
        sql="""
        CREATE TABLE IF NOT EXISTS titles_crew (
            title_id INT,
            crew_id INT REFERENCES dim_crew (id),
            role VARCHAR(50)
        );
        """,
    )                             

    create_dim_crew_table = PostgresOperator(
        task_id="create_dim_crew_table",
        postgres_conn_id="imdb_postgres",
        sql="""
        CREATE TABLE IF NOT EXISTS dim_crew (
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
            title_id INT,
            cast_id INT REFERENCES dim_casts (id),
            character VARCHAR(300)
        );
        """,
    )                                                                  

    set_fkey = PostgresOperator(
        task_id="set_fkey",
        postgres_conn_id="imdb_postgres",
        sql="""
        ALTER TABLE titles_casts 
        ADD CONSTRAINT titles_casts_title_id_fkey FOREIGN KEY (title_id)
        REFERENCES fact_titles (id);

        ALTER TABLE titles_crew 
        ADD CONSTRAINT titles_crew_title_id_fkey FOREIGN KEY (title_id)
        REFERENCES fact_titles (id);
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
    create_schema >> create_dim_title_desc_table >> create_fact_titles_table
    create_schema >> create_dim_download_date_table >> create_fact_titles_table
    create_schema >> create_dim_episodes_table >> create_fact_titles_table 
    create_schema >> create_dim_crew_table >> create_titles_crew_table >> create_fact_titles_table
    create_schema >> create_dim_casts_table >> create_titles_casts_table >> create_fact_titles_table
    
    create_fact_titles_table >> set_fkey