# imdb-etl

## Steps
1. Download IMDb dataset from [here](https://datasets.imdbws.com/)
2. Unzip and upload to MinIO
3. Pyspark read file contents (.tsv files) into dataframe
4. Pyspark transformation
5. Push transformed data into OLAP database
6. Create visualizations

## docker-compose
1. Create redash db
> docker-compose run --rm redash create_db
2. Create all the containers
> docker-compose up -d 

## Visit Airflow Web UI 
- Go to localhost:8080 - username: airflow, password: airflow

- unit testing 
- metrics monitoring
- use delta lake?