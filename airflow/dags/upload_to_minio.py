from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.s3_bucket import S3CreateBucketOperator
from airflow.utils.dates import days_ago
from time import sleep
import os
import logging
import requests
import gzip
import shutil
import datetime

BUCKET_NAME = os.environ.get('BUCKET_NAME', 'test-bucket')
IMDB_DATASETS_BASE_URL = os.environ.get('IMDB_DATASETS_BASE_URL', 'https://datasets.imdbws.com/')

now = datetime.datetime.now()
OBJECT_PREFIX = now.strftime("%Y-%m/%d")

def _remove_local_file(filename):
    counter = 0
    while 1:
        # try for 5 times only      
        if counter > 4:
            return False
        else:
            counter += 1

        try:
            os.remove(filename)
            logging.info(f"Deleted file {filename}")
            return True
        except OSError:
            logging.error(f"Unable to delete file {filename}. File is still being used by another process.")
            sleep(5)  

def _upload_file(tsv_file, object_prefix):
    key = object_prefix + "/" + tsv_file
    s3_hook = S3Hook(aws_conn_id="imdb_minio")
    s3_hook.load_file(
        filename=tsv_file,
        key=key,
        bucket_name=BUCKET_NAME,
    )

def _rename_file(ori_filename, rename_to):
    airflow_home_dir = os.environ.get('AIRFLOW_HOME', '/opt/airflow')
    old_file = os.path.join(airflow_home_dir, ori_filename)
    new_file = os.path.join(airflow_home_dir, rename_to)
    os.rename(old_file, new_file)
    return new_file

def _unzip_gz(file):
    with gzip.open(file, 'rb') as f_in:  # unzip and open the .gz file
        filename = file.split('.gz')[0]
        with open(filename, 'wb') as f_out:  # open another blank file
            shutil.copyfileobj(f_in, f_out)  # copy the .gz file contents to the blank file
    logging.info(f"Finished unzipping {file}. Output is {filename}")

    # tsv_filename = _rename_file(filename, object_prefix)

    return filename

def download_file(base_url, filename, object_prefix):
    # concatenate to get the url of the file 
    full_path = base_url + filename

    logging.info(f"Downloading from {full_path}")
    gz_filename = full_path.split('/')[-1]
    # Note the stream=True parameter below
    with requests.get(full_path, stream=True) as r:
        r.raise_for_status()
        with open(gz_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192): 
                # If you have chunk encoded response uncomment if
                # and set chunk_size parameter to None.
                #if chunk: 
                f.write(chunk)
    logging.info(f"Finished downloading from {full_path}")

    tsv_file = _unzip_gz(gz_filename)

    _upload_file(tsv_file, object_prefix)
    _remove_local_file(tsv_file)
    _remove_local_file(gz_filename)

    logging.info(f"Uploaded {tsv_file} to MinIO as {object_prefix + tsv_file}")

    return True

with DAG(
    dag_id='upload_imdb_datasets_minio',
    schedule_interval=None,
    start_date=days_ago(2),
    max_active_runs=1,
    tags=['minio'],
) as dag:

    create_bucket = S3CreateBucketOperator(
        task_id='create_bucket',
        aws_conn_id='imdb_minio',
        bucket_name=BUCKET_NAME,
    )

    upload_name_basics = PythonOperator(
        task_id="upload_name_basics", 
        python_callable=download_file,
        op_kwargs={
            'base_url': IMDB_DATASETS_BASE_URL, 
            "filename": "name.basics.tsv.gz",
            "object_prefix": OBJECT_PREFIX,
        },
    )

    upload_title_akas = PythonOperator(
        task_id="upload_title_akas", 
        python_callable=download_file,
        op_kwargs={
            'base_url': IMDB_DATASETS_BASE_URL, 
            "filename": "title.akas.tsv.gz",
            "object_prefix": OBJECT_PREFIX,
        },
    )

    upload_title_basics = PythonOperator(
        task_id="upload_title_basics", 
        python_callable=download_file,
        op_kwargs={
            'base_url': IMDB_DATASETS_BASE_URL, 
            "filename": "title.basics.tsv.gz",
            "object_prefix": OBJECT_PREFIX,
        },
    )

    upload_title_crew = PythonOperator(
        task_id="upload_title_crew", 
        python_callable=download_file,
        op_kwargs={
            'base_url': IMDB_DATASETS_BASE_URL, 
            "filename": "title.crew.tsv.gz",
            "object_prefix": OBJECT_PREFIX,
        },
    )

    upload_title_episode = PythonOperator(
        task_id="upload_title_episode", 
        python_callable=download_file,
        op_kwargs={
            'base_url': IMDB_DATASETS_BASE_URL, 
            "filename": "title.episode.tsv.gz",
            "object_prefix": OBJECT_PREFIX,
        },
    )

    upload_title_principals = PythonOperator(
        task_id="upload_title_principals", 
        python_callable=download_file,
        op_kwargs={
            'base_url': IMDB_DATASETS_BASE_URL, 
            "filename": "title.principals.tsv.gz",
            "object_prefix": OBJECT_PREFIX,
        },
    )

    upload_title_ratings = PythonOperator(
        task_id="upload_title_ratings", 
        python_callable=download_file,
        op_kwargs={
            'base_url': IMDB_DATASETS_BASE_URL, 
            "filename": "title.ratings.tsv.gz",
            "object_prefix": OBJECT_PREFIX,
        },
    )

    create_bucket >> upload_name_basics 
    create_bucket >> upload_title_akas 
    create_bucket >> upload_title_basics 
    create_bucket >> upload_title_crew 
    create_bucket >> upload_title_episode 
    create_bucket >> upload_title_principals
    create_bucket >> upload_title_ratings               
