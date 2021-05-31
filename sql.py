import os
from conf import SCHEMA_NAME

drop_schema_sql = f"DROP SCHEMA IF EXISTS {SCHEMA_NAME} CASCADE;"
create_schema_sql = f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME};"



create_titles_table = f"""
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
    create_directors_table, 
    create_stars_table, 
    create_writers_table, 
    create_genres_table, 
    create_titles_table,
    create_titles_directors_tables,
    create_titles_stars_tables,
    create_titles_writers_tables,
    create_titles_genres_tables,
    ]         
