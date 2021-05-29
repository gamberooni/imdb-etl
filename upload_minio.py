import requests
import gzip
import shutil
import os 
from minio import Minio
import logging
import datetime
from time import sleep

logging.basicConfig(
    format='%(levelname)s: %(asctime)s - %(message)s', 
    datefmt='%d-%b-%y %H:%M:%S', 
    level=logging.INFO
    )

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

def unzip_gz(file):
    with gzip.open(file, 'rb') as f_in:  # unzip and open the .gz file
        filename = file.split('.gz')[0]
        with open(filename, 'wb') as f_out:  # open another blank file
            shutil.copyfileobj(f_in, f_out)  # copy the .gz file contents to the blank file
    logging.info(f"Finished unzipping {file}. Output is {filename}")
    return filename

def remove_file(filename):
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
    gz_file = download_file(url)
    tsv_file = unzip_gz(gz_file)
    fbytes = open(tsv_file, "rb")  # open file and read as bytes
    # upload the opened file to minio
    result = client.put_object(
        bucket_name, f"{yyyy_mm_today}/{dd_today}/{tsv_file}", fbytes, os.path.getsize(tsv_file)
    )
    logging.info(f"Finished uploading {tsv_file} to MinIO")
    fbytes.close()  # close file after finish uploading

    ret = remove_file(gz_file)
    ret = remove_file(tsv_file)
    