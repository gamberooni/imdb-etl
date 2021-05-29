import requests
import gzip
import shutil
import os 
from minio import Minio
import logging
import datetime

logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)

def download_file(url):
    logging.info(f"Downloading from {url}...")
    local_filename = url.split('/')[-1]
    # NOTE the stream=True parameter below
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192): 
                # If you have chunk encoded response uncomment if
                # and set chunk_size parameter to None.
                #if chunk: 
                f.write(chunk)
    logging.info(f"Finished downloading from {url}")
    return local_filename

# def extract_gz(file):
#     try:
#         for file in os.listdir(os.getcwd()):  # list all files in the directory
#             if file.endswith(".gz"):  # if .gz extension
#                 with gzip.open(file, 'rb') as f_in:  # unzip and open the .gz file
#                     with open(file.split('.gz')[0], 'wb') as f_out:  # open another blank file
#                         shutil.copyfileobj(f_in, f_out)  # copy the .gz file contents to the blank file
#         return True
#     except:
#         return False

def extract_gz(file):
    with gzip.open(file, 'rb') as f_in:  # unzip and open the .gz file
        filename = file.split('.gz')[0]
        with open(filename, 'wb') as f_out:  # open another blank file
            shutil.copyfileobj(f_in, f_out)  # copy the .gz file contents to the blank file
    logging.info(f"Finished extracting from {file}")

files_to_dl = [
    "https://datasets.imdbws.com/name.basics.tsv.gz",
    "https://datasets.imdbws.com/title.akas.tsv.gz",
    "https://datasets.imdbws.com/title.basics.tsv.gz",
    "https://datasets.imdbws.com/title.crew.tsv.gz",
    "https://datasets.imdbws.com/title.episode.tsv.gz",
    "https://datasets.imdbws.com/title.principals.tsv.gz",
    "https://datasets.imdbws.com/title.ratings.tsv.gz"
]

client = Minio(
    "192.168.0.188:9000",
    access_key="admin",
    secret_key="password",
    secure=False,
)

bucket_name = "imdb-data"

# create the bucket if not exists
if client.bucket_exists(bucket_name):
    logging.info(f"Bucket '{bucket_name}' already exists.")
    logging.info("Not creating new bucket.")
else:
    client.make_bucket(bucket_name)
    logging.info(f"Created bucket '{bucket_name}'")

now = datetime.datetime.now()
yyyy_mm_today = now.strftime("%Y-%m")
dd_today = now.strftime("%d")

for url in files_to_dl:
    file = download_file(url)
    extract_gz(file)
    # file = "name.basics.tsv"
    fbytes = open(file, "rb")
    result = client.put_object(
        bucket_name, f"{yyyy_mm_today}/{dd_today}/{file}", fbytes, os.path.getsize(fbytes)
    )
    logging.info(f"Finished uploading {file} to MinIO")
