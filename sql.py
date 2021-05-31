import os
from conf import SCHEMA_NAME

drop_schema_sql = f"DROP SCHEMA IF EXISTS {SCHEMA_NAME} CASCADE;"
create_schema_sql = f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME};"

create_dim_downloadDate_table = f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.dim_downloadDate (
        id SERIAL PRIMARY KEY,
        year SMALLINT,
        month SMALLINT,
        day SMALLINT,
    );
    """

create_dim_ratings_table = f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.dim_ratings (
        id SERIAL PRIMARY KEY,
        avRating FLOAT(2),
        numVotes INT,
    );
    """

create_dim_episode_table = f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.dim_episode (
        id SERIAL PRIMARY KEY,
        seasonNumber SMALLINT,
        epCount SMALLINT,
    );
    """

create_dim_titleDesc_table = f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.dim_titleDesc (
        id SERIAL PRIMARY KEY,
        tconst VARCHAR(20),
        type VARCHAR(20),
        primaryTitle VARCHAR(200),
        originalTitle VARCHAR(200),
        isAdult BOOLEAN,
        startYear SMALLINT,
        endYear SMALLINT,
        runtimeMinutes SMALLINT,
        language VARCHAR(50),
        region VARCHAR(50),
    );
    """

create_titles_genres_table = f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.titles_genres (
        title_id INT REFERENCES {SCHEMA_NAME}.titles (id),
        genre_id INT REFERENCES {SCHEMA_NAME}.dim_genres (id),
    );
    """

create_dim_genres_table = f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.dim_genres (
        id SERIAL PRIMARY KEY,
        genre VARCHAR(25),
    );
    """ 

create_titles_crew_table = f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.titles_crew (
        title_id INT REFERENCES {SCHEMA_NAME}.titles (id),
        crew_id INT REFERENCES {SCHEMA_NAME}.dim_crew (id),
    );
    """    

create_dim_crew_table = f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.dim_crew (
        id SERIAL PRIMARY KEY,
        name VARCHAR(200),
        role VARCHAR(20),
        age SMALLINT,
        isAlive BOOLEAN,
        nconst VARCHAR(20),
    );
    """     

create_titles_casts_table = f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.titles_casts (
        title_id INT REFERENCES {SCHEMA_NAME}.titles (id),
        cast_id INT REFERENCES {SCHEMA_NAME}.dim_casts (id),
    );
    """    

create_dim_casts_table = f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.dim_casts (
        id SERIAL PRIMARY KEY,
        name VARCHAR(200),
        characterName VARCHAR(200),
        age SMALLINT,
        isAlive BOOLEAN,
        nconst VARCHAR(20),
    );
    """         

create_fact_titles_table = f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.fact_titles (
        id SERIAL PRIMARY KEY,
        downloadDate_id INT REFERENCES {SCHEMA_NAME}.dim_downloadDate (id),
        ratings_id INT REFERENCES {SCHEMA_NAME}.dim_ratings (id),
        episode_id INT REFERENCES {SCHEMA_NAME}.dim_episode (id),
        titleDesc_id INT REFERENCES {SCHEMA_NAME}.dim_titleDesc (id),
        titles_genres_id INT REFERENCES {SCHEMA_NAME}.titles_genres (id),
        titles_casts_id INT REFERENCES {SCHEMA_NAME}.titles_casts (id),    
        titles_crew_id INT REFERENCES {SCHEMA_NAME}.titles_crew (id),                          
        tconst VARCHAR(20) UNIQUE,
        daily_avRatingChange FLOAT(2),
        weekly_avRatingChange FLOAT(2),
        daily_numVotesChange INT,
        weekly_numVotesChange INT, 
    );
    """

create_tables_sql = [
    create_fact_titles_table, 
    create_dim_titleDesc_table, 
    create_dim_downloadDate_table,
    create_dim_episode_table,
    create_dim_genres_table,
    create_dim_crew_table,
    create_dim_casts_table,
    create_dim_ratings_table,
    create_titles_casts_table,
    create_titles_crew_table,
    create_titles_genres_table,
    ]         
