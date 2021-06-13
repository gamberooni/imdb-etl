from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import ShortType
import os 

MINIO_HOST = os.environ['MINIO_HOST']
MINIO_ACCESS_KEY = os.environ['MINIO_ACCESS_KEY']
MINIO_SECRET_KEY = os.environ['MINIO_SECRET_KEY']
POSTGRES_HOST = os.environ['POSTGRES_HOST']
POSTGRES_USER = os.environ['POSTGRES_USER']
POSTGRES_PASSWORD = os.environ['POSTGRES_PASSWORD']
IMDB_BUCKET_NAME = os.environ['IMDB_BUCKET_NAME']
IMDB_OBJECT_PREFIX = os.environ['IMDB_OBJECT_PREFIX']


def upload_dim_episodes():
    spark = SparkSession.builder \
        .appName("episodes") \
        .getOrCreate()

    # set config to read from minio
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", MINIO_ACCESS_KEY)
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", MINIO_SECRET_KEY)
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.endpoint", f"http://{MINIO_HOST}")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
    spark.sparkContext._jsc.hadoopConfiguration().set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark.sparkContext._jsc.hadoopConfiguration().set("spark.hadoop.fs.s3a.path.style.access", "true")
    spark.sparkContext._jsc.hadoopConfiguration().set("-Dcom.amazonaws.services.s3.enableV4", "true")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.multipart.size", "104857600")    

    # read from minio 
    title_episodes_df = spark.read.csv(f"s3a://{IMDB_BUCKET_NAME}/{IMDB_OBJECT_PREFIX}/title.episode.tsv", sep=r'\t', header=True)  

    title_episodes_df = title_episodes_df.withColumn('seasonNumber', F.col('seasonNumber').cast(ShortType()))
    title_episodes_df = title_episodes_df.withColumn('episodeNumber', F.col('episodeNumber').cast(ShortType()))
    title_episodes_df = title_episodes_df.drop('tconst')

    null_df = title_episodes_df.filter(F.col("seasonNumber").isNull() & F.col("episodeNumber").isNull())
    null_df_final = null_df.groupBy('parentTconst').count()
    null_df_final = null_df_final.withColumn('season', F.lit(None)).withColumnRenamed('count', 'total_episodes').withColumnRenamed('parentTconst', 'tconst')

    not_null_df = title_episodes_df.filter(F.col("seasonNumber").isNotNull() & F.col("episodeNumber").isNotNull())
    not_null_df_final = not_null_df.groupBy('parentTconst', 'seasonNumber').agg(F.max('episodeNumber').alias('episode_count'))
    not_null_df_final = not_null_df_final.sort('parentTconst', F.col('seasonNumber').desc(), F.col('episode_count').desc())
    not_null_df_final = not_null_df_final.withColumnRenamed('seasonNumber', 'season').withColumnRenamed('parentTconst', 'tconst')

    result_df = null_df_final.union(not_null_df_final)

    titles_pg_df = spark.read.format('jdbc').options(
        url=f'jdbc:postgresql://{POSTGRES_HOST}/imdb',
        driver='org.postgresql.Driver',
        dbtable='titles',
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
        ).load()

    titles_pg_df = titles_pg_df.select('tconst', 'id')

    df_final = result_df.join(titles_pg_df, ['tconst']).drop('tconst').withColumnRenamed('id', 'title_id')
    df_final.write.format('jdbc').options(
        url=f'jdbc:postgresql://{POSTGRES_HOST}/imdb',
        driver='org.postgresql.Driver',
        dbtable='episodes',
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
        ).mode('append').save()

def main():
    upload_dim_episodes()

if __name__ == '__main__':
    main()
                