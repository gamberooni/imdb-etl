from pyspark.sql import SparkSession
# import os

# master = "spark://zy-ubuntu:7077"  
# os.environ['PYSPARK_SUBMIT_ARGS'] = f'--master {master} \
#     --driver-memory 4g \
#     --total-executor-cores 8 \
#     --executor-memory 8g \
#     --packages org.postgresql:postgresql:42.1.1 pyspark-shell'

def read_minio():
    spark = SparkSession.builder \
        .appName("read minio") \
        .getOrCreate()

    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", "admin")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "password")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://127.0.0.1:9000")  # must not use 'localhost'
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
    spark.sparkContext._jsc.hadoopConfiguration().set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark.sparkContext._jsc.hadoopConfiguration().set("spark.hadoop.fs.s3a.path.style.access", "true")
    spark.sparkContext._jsc.hadoopConfiguration().set("-Dcom.amazonaws.services.s3.enableV4", "true")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.multipart.size", "104857600")    

    df = spark.read.csv("s3a://imdb/2021-06/06/title.ratings.tsv", sep=r'\t', header=True)
    df.show()

def main():
    read_minio()

if __name__ == '__main__':
    main()
    