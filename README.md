# README

## Introduction
This is a self-learning project on ETL using Spark, Airflow, MinIO and Redash. The IMDb datasets can be found [here](https://datasets.imdbws.com/) and the corresponding documentation can be found [here](https://www.imdb.com/interfaces/). 

After doing some casual browsing as well as data wrangling on the tsv files downloaded from the datasets page, I decided to design the schema as shown below:
<p align="center">
  <img src="./images/schema.png" />
</p>

The datasets are downloaded from the datasets page and uploaded to MinIO. After that, I read the tsv files into dataframes and did some processing to fit the schema. The processed data is loaded into a Postgres database. A BI tool like Redash then pulls data out from the database to populate the [dashboard](#dashboard). I used Airflow to automate the ETL process. Following is the high-level overview of the pipeline:
<p align="center">
  <img src="./images/imdb-etl.png" />
</p>

## Dashboard
A dashboard created using Redash
<p align="center">
  <img src="./images/redash-1.png" />
</p>
<p align="center">
  <img src="./images/redash-2.png" />
</p>

## Steps
1. Change the values in `airflow/config.json` and `airflow/create_cnx.py`
2. Initialize `Redash` database
> docker-compose run --rm redash create_db
3. Start all services using docker compose
> docker-compose up -d 
4. Visit `Airflow Web UI` at `localhost:8082` and login using
> username: airflow
>
> password: airflow
5. Go to Admin > Variables > Choose File > choose the `config.json` in `airflow` directory > Import Variables
6. Trigger the DAGs that only need to run **ONCE** (create_connections and create_tables)
7. Trigger the `upload_imdb_datasets_minio` DAG and the `imdb_etl` DAG will be triggered after the first one has finished. 
8. Visit `Redash` at `localhost` and set up password
9. Add data source, create queries using the sql statements in `dashboard-queries` folder and create a dashboard from those queries

## Spark standalone on local machine when developing using notebooks
1. Installation
```
sudo apt update  
sudo apt install default-jdk scala git -y
java -version; javac -version; scala -version; git --version
wget https://downloads.apache.org/spark/spark-3.1.2/spark-3.1.2-bin-hadoop3.2.tgz
tar -xvzf spark-*
sudo mv spark-3.1.2-bin-hadoop3.2 /opt/spark
echo "export SPARK_HOME=/opt/spark" >> ~/.profile
echo "export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin" >> ~/.profile
echo "export PYSPARK_PYTHON=/usr/bin/python3" >> ~/.profile
echo "export SPARK_MASTER_WEBUI_PORT=8080" >> ~/.profile
source ~/.profile
```

2. Jar Dependencies
- Go to [Maven Repository](https://mvnrepository.com/) and download all these jar files. Then move all of them to `$SPARK_HOME/jars`. You need to delete the original `guava` jar file in `$SPARK_HOME/jars`

| No. | Jar File            | Version       |
| :-  | :-                  | :-            |
| 1.  | hadoop-aws          | 3.2.0         |
| 2.  | aws-java-sdk-bundle | 1.11.375      |
| 3.  | guava               | 30.1.1-jre    |
| 4.  | jets3t              | 0.9.4         |
| 5.  | postgresql          | 42.2.0        |

3. Start master and worker 
```
$SPARK_HOME/sbin/start-master.sh
$SPARK_HOME/sbin/start-worker.sh spark://<machine_hostname>:7077 
```

4. In the jupyter notebooks, run the following lines to set number of cores and amount of memory to use
```
import os

master = "spark://<machine_hostname>:7077"  
os.environ['PYSPARK_SUBMIT_ARGS'] = f'--master {master} --driver-memory 4g --total-executor-cores 6 --executor-memory 8g --packages org.postgresql:postgresql:42.1.1 pyspark-shell'
```
> **__NOTE:__** Visit `localhost:8080` and you can see the value of your machine hostname

5. Order of execution for notebooks
```
1. download_date.ipynb
2. titles.ipynb
3. episodes.ipynb
4. casts.ipynb
5. crew.ipynb
```

## Great Expectations
1. Install great_expectations
```
. venv/bin/activate
pip install great_expectations
```

2. Initialize Great Expectations deployment
```
great_expectations init
```

3. Connect to local filesystem datasource (Pandas)
```
$ great_expectations datasource new

the datasources section will look something like this:
datasources:
  imdb_pandas:
    data_asset_type:
      module_name: great_expectations.dataset
      class_name: PandasDataset
    batch_kwargs_generators:
      subdir_reader:
        class_name: SubdirReaderBatchKwargsGenerator
        base_directory: ./data
    module_name: great_expectations.datasource
    class_name: PandasDatasource

```

4. Place all the tsv files in the `great_expectations/data` directory (because I don't know how to use files from S3 yet...)

5. Create expectation suite for each tsv file
```
$ great_expectations suite new

Add the following in the `batch_kwargs` dictionary in order to read tsv file
"reader_options": {"sep": "\t"}
```

6. Write table and column expectations in the Jupyter Notebook - expectations [glossary](https://docs.greatexpectations.io/en/latest/reference/glossary_of_expectations.html)


### Use S3 store for expectations, validations and data_docs site
```
sudo apt install awscli

activate venv then
$ pip install boto3==1.17.0 fsspec 's3fs<=0.4'

set aws credentials
$ aws configure
```

#### Expectations json files
1. Create bucket
2. Copy existing expectation json files to s3 bucket
```
$ aws s3 --endpoint-url http://localhost:9000 sync expectations s3://great-expectations/expectations
```

3. Verify ge is using s3 to store expectations
```
$ great_expectations suite list
```

#### Validations files
1. Create bucket
2. Copy validations files to s3 bucket
```
$ aws s3 --endpoint-url http://localhost:9000 sync uncommitted/validations s3://great-expectations/validations
```

3. Verify ge is using s3 to store validations
```
$ great_expectations store list
```

#### Minio data docs site
1. Create a bucket called `data-docs`
2. Use minio client to change bucket policy to `download`
```
$ docker run -it --entrypoint=/bin/sh minio/mc

# change it to your IP address 
$ mc alias set minio http://192.168.0.191:9000 admin password --api S3v4
$ mc policy set download minio/data-docs
```

4. Build data_docs in the `data-docs` bucket
```
great_expectations docs build --site-name s3_site
```

5. Visit `localhost:82` to view the data docs
> The `minio_site` Nginx service defined in the `docker-compose.yaml` will serve the data_docs site


#### Minio datasource
1. Set bucket policy to download
```
mc policy set download minio/imdb
```

2. Edit `great_expectations.yml`
```
datasources:
  s3_pandas:
    class_name: PandasDatasource
    boto3_options:
      endpoint_url: 'http://localhost:9000'
    batch_kwargs_generators:
      pandas_s3_generator:
        class_name: S3GlobReaderBatchKwargsGenerator
        boto3_options:
          endpoint_url: 'http://localhost:9000'
        reader_options:
          sep: "\t"
        bucket: imdb # Only the bucket name here (i.e., no prefix)
        assets:
          imdb_data:
            prefix: 2021-06/26/ # trailing slash is important
            regex_filter: .*.tsv  # The regex filter will filter the results returned by S3 for the key and prefix to only those matching the regex
    module_name: great_expectations.datasource
    data_asset_type:
      class_name: PandasDataset
      module_name: great_expectations.dataset
```

3. Open Jupyter Notebook and write expectations
```
great_expectations suite scaffold s3_pandas
```


## Todo
- Refactor code to reduce hardcoded stuffs
- metrics monitoring