# imdb-etl

## Steps
1. Download IMDb dataset from [here](https://datasets.imdbws.com/)
2. Unzip and upload to MinIO
3. Pyspark read file contents (.tsv files) into dataframe
4. Pyspark transformation
5. Push transformed data into OLAP database
6. Create visualizations

## Install Spark
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
echo "export SPARK_MASTER_WEBUI_PORT=8082" >> ~/.profile
source ~/.profile
```

## Spark Standalone Cluster
1. Start master and worker
```
$SPARK_HOME/sbin/start-master.sh
$SPARK_HOME/sbin/start-worker.sh spark://zy-ubuntu:7077 
```

2. In the jupyter notebook, run the following lines to set number of cores and amount of memory to use
```
import os

master = "spark://zy-ubuntu:7077"  
os.environ['PYSPARK_SUBMIT_ARGS'] = f'--master {master} --driver-memory 4g --total-executor-cores 6 --executor-memory 8g --packages org.postgresql:postgresql:42.1.1 pyspark-shell'
```

## Order of execution
1. dim_title_desc.ipynb
2. dim_episodes.ipynb
3. dim_casts.ipynb
4. dim_crew.ipynb

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