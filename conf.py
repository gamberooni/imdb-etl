import os

BUCKET_NAME = os.getenv('BUCKET_NAME', "imdb")
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', "admin") 
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', "password") 
MINIO_HOST = os.getenv('MINIO_HOST', "localhost") 
MINIO_PORT = os.getenv('MINIO_PORT', "9000")

POSTGRES_USER = os.getenv('POSTGRES_USER', "admin")
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', "password") 
POSTGRES_HOSTNAME = os.getenv('POSTGRES_HOSTNAME', "localhost") 
POSTGRES_PORT = os.getenv('POSTGRES_PORT', "5433") 
POSTGRES_DATABASE = os.getenv('POSTGRES_DATABASE', "imdb")
DB_URI = f'postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOSTNAME}:{POSTGRES_PORT}/{POSTGRES_DATABASE}'
SCHEMA_NAME = os.getenv('SCHEMA_NAME', "imdb")

files_to_dl = [
    "https://datasets.imdbws.com/name.basics.tsv.gz",
    "https://datasets.imdbws.com/title.akas.tsv.gz",
    "https://datasets.imdbws.com/title.basics.tsv.gz",
    "https://datasets.imdbws.com/title.crew.tsv.gz",
    "https://datasets.imdbws.com/title.episode.tsv.gz",
    "https://datasets.imdbws.com/title.principals.tsv.gz",
    "https://datasets.imdbws.com/title.ratings.tsv.gz"
]