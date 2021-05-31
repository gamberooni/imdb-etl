import psycopg2
from conf import *
from sql import *
import logging

# logging.basicConfig(
#     format='%(levelname)s: %(asctime)s - %(message)s', 
#     datefmt='%d-%b-%y %H:%M:%S', 
#     level=logging.INFO
#     )

conn = psycopg2.connect(
    host=POSTGRES_HOSTNAME, 
    port=POSTGRES_PORT, 
    database=POSTGRES_DATABASE, 
    user=POSTGRES_USER, 
    password=POSTGRES_PASSWORD
    )
cursor = conn.cursor()

# drop the schema if exists
cursor.execute(drop_schema_sql)  
logging.info(f"Dropped schema '{SCHEMA_NAME}'")
# create the schema
cursor.execute(create_schema_sql)  
logging.info(f"Created schema '{SCHEMA_NAME}'")
conn.commit()
# create tables
for create_table in create_tables_sql:
    cursor.execute(create_table)
conn.commit()
logging.info("Finished creating tables")