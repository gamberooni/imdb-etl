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
            year SMALLINT,
            month SMALLINT,
            day SMALLINT
        );
        """,
    )

    create_dim_ratings_table = PostgresOperator(
        task_id="create_dim_ratings_table",
        postgres_conn_id="imdb_postgres",
        sql="""
        CREATE TABLE IF NOT EXISTS dim_ratings (
            id SERIAL PRIMARY KEY,
            tconst VARCHAR(15),
            av_rating FLOAT(2),
            num_votes INT
        );
        """,
    )    

    create_dim_episode_table = PostgresOperator(
        task_id="create_dim_episode_table",
        postgres_conn_id="imdb_postgres",
        sql="""
        CREATE TABLE IF NOT EXISTS dim_episode (
            id SERIAL PRIMARY KEY,
            seasonNumber SMALLINT,
            epCount SMALLINT
        );
        """,
    )    

    create_dim_title_desc_table = PostgresOperator(
        task_id="create_dim_title_desc_table",
        postgres_conn_id="imdb_postgres",
        sql="""
        CREATE TABLE IF NOT EXISTS dim_title_desc (
            id SERIAL PRIMARY KEY,
            tconst VARCHAR(20),
            type VARCHAR(40),
            primary_title VARCHAR(800),
            original_title VARCHAR(800),
            is_adult BOOLEAN,
            start_year SMALLINT,
            end_year SMALLINT,
            runtime_minutes SMALLINT
        );
        """,
    )    

    create_dim_casts_table = PostgresOperator(
        task_id="create_dim_casts_table",
        postgres_conn_id="imdb_postgres",
        sql="""
        CREATE TABLE IF NOT EXISTS dim_casts (
            id SERIAL PRIMARY KEY,
            name VARCHAR(200),
            character VARCHAR(300),
            age SMALLINT,
            is_alive BOOLEAN,
            nconst VARCHAR(15),
            tconst VARCHAR(15)
        );
        """,
    )        
 
    create_fact_titles_table = PostgresOperator(
        task_id="create_fact_titles_table",
        postgres_conn_id="imdb_postgres",
        sql="""
        CREATE TABLE IF NOT EXISTS fact_titles (
            id SERIAL PRIMARY KEY,
            downloadDate_id INT REFERENCES dim_download_date (id),
            ratings_id INT REFERENCES dim_ratings (id),
            episode_id INT REFERENCES dim_episode (id),
            titleDesc_id INT REFERENCES dim_title_desc (id),
            titles_genres_id INT,
            titles_casts_id INT,    
            titles_crew_id INT,                          
            tconst VARCHAR(20) UNIQUE,
            daily_avRatingChange FLOAT(2),
            weekly_avRatingChange FLOAT(2),
            daily_numVotesChange INT,
            weekly_numVotesChange INT
        );
        """,
    )      

    create_titles_genres_table = PostgresOperator(
        task_id="create_titles_genres_table",
        postgres_conn_id="imdb_postgres",
        sql="""
        CREATE TABLE IF NOT EXISTS titles_genres (
            title_id INT,
            genre_id INT REFERENCES dim_genres (id)
        );
        """,
    )         

    create_dim_genres_table = PostgresOperator(
        task_id="create_dim_genres_table",
        postgres_conn_id="imdb_postgres",
        sql="""
        CREATE TABLE IF NOT EXISTS dim_genres (
            id SERIAL PRIMARY KEY,
            genre VARCHAR(25)
        );
        """,
    )     

    create_titles_crew_table = PostgresOperator(
        task_id="create_titles_crew_table",
        postgres_conn_id="imdb_postgres",
        sql="""
        CREATE TABLE IF NOT EXISTS titles_crew (
            title_id INT,
            crew_id INT REFERENCES dim_crew (id)
        );
        """,
    )                             

    create_dim_crew_table = PostgresOperator(
        task_id="create_dim_crew_table",
        postgres_conn_id="imdb_postgres",
        sql="""
        CREATE TABLE IF NOT EXISTS dim_crew (
            id SERIAL PRIMARY KEY,
            name VARCHAR(200),
            role VARCHAR(50),
            age SMALLINT,
            is_alive BOOLEAN,
            tconst VARCHAR(15),
            nconst VARCHAR(15)
        );
        """,
    )    

    create_titles_casts_table = PostgresOperator(
        task_id="create_titles_casts_table",
        postgres_conn_id="imdb_postgres",
        sql="""
        CREATE TABLE IF NOT EXISTS titles_casts (
            title_id INT,
            cast_id INT REFERENCES dim_casts (id)
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

        ALTER TABLE titles_genres 
        ADD CONSTRAINT titles_genres_title_id_fkey FOREIGN KEY (title_id)
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
    create_schema >> create_dim_episode_table >> create_fact_titles_table 
    create_schema >> create_dim_genres_table >> create_titles_genres_table >> create_fact_titles_table
    create_schema >> create_dim_crew_table >> create_titles_crew_table >> create_fact_titles_table
    create_schema >> create_dim_casts_table >> create_titles_casts_table >> create_fact_titles_table
    create_schema >> create_dim_ratings_table >> create_fact_titles_table
    
    create_fact_titles_table >> set_fkey