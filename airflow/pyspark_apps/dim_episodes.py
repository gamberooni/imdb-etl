from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import ShortType
import datetime

def upload_dim_episodes():
    spark = SparkSession.builder \
        .appName("IMDb ETL Task 2") \
        .getOrCreate()

    # set config to read from minio
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", "admin")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "password")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://172.18.0.15:9000")  # must use IP address
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
    spark.sparkContext._jsc.hadoopConfiguration().set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark.sparkContext._jsc.hadoopConfiguration().set("spark.hadoop.fs.s3a.path.style.access", "true")
    spark.sparkContext._jsc.hadoopConfiguration().set("-Dcom.amazonaws.services.s3.enableV4", "true")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.multipart.size", "104857600")    

    # read from minio 
    title_episodes_df = spark.read.csv("s3a://imdb/2021-06/06/title.episode.tsv", sep=r'\t', header=True)  

    title_episodes_df = title_episodes_df.withColumn('seasonNumber', F.col('seasonNumber').cast(ShortType()))
    title_episodes_df = title_episodes_df.withColumn('episodeNumber', F.col('episodeNumber').cast(ShortType()))
    title_episodes_df = title_episodes_df.drop('tconst')

    # titles with seasonNumber = null
    null_df = title_episodes_df.filter(F.col("seasonNumber").isNull() & F.col("episodeNumber").isNull())
    null_df_final = null_df.groupBy('parentTconst').count()
    null_df_final = null_df_final.withColumn('season', F.lit(None)).withColumnRenamed('count', 'total_episodes').withColumnRenamed('parentTconst', 'tconst')

    # titles with season number
    not_null_df = title_episodes_df.filter(F.col("seasonNumber").isNotNull() & F.col("episodeNumber").isNotNull())
    not_null_df_final = not_null_df.groupBy('parentTconst', 'seasonNumber').agg(F.max('episodeNumber').alias('episode_count'))
    not_null_df_final = not_null_df_final.sort('parentTconst', F.col('seasonNumber').desc(), F.col('episode_count').desc())
    not_null_df_final = not_null_df_final.withColumnRenamed('seasonNumber', 'season').withColumnRenamed('parentTconst', 'tconst')

    # union to concatenate both dfs
    result_df = null_df_final.union(not_null_df_final)

    result_df.write.format('jdbc').options(
        url='jdbc:postgresql://imdb_postgres:5432/imdb',
        driver='org.postgresql.Driver',
        dbtable='dim_episodes',
        user='admin',
        password='password'
        ).mode('append').save()

    title_desc_pg_df = spark.read.format('jdbc').options(
        url='jdbc:postgresql://imdb_postgres:5432/imdb',
        driver='org.postgresql.Driver',
        dbtable='dim_title_desc',
        user='admin',
        password='password'
        ).load()

    # get title_desc_id
    title_desc_pg_df = title_desc_pg_df.withColumnRenamed('id', 'title_desc_id')      
    fact_titles_df = title_desc_pg_df.select('title_desc_id')

    episodes_pg_df = spark.read.format('jdbc').options(
        url='jdbc:postgresql://imdb_postgres:5432/imdb',
        driver='org.postgresql.Driver',
        dbtable='dim_episodes',
        user='admin',
        password='password'
        ).load()
        
    # join to associate the episodes df with title_desc_id
    episodes_pg_df = episodes_pg_df.withColumnRenamed('id', 'episode_id')      
    desc_ep_joined_df = episodes_pg_df.join(title_desc_pg_df, ['tconst']).select('episode_id', 'title_desc_id')
    fact_titles_df = fact_titles_df.join(desc_ep_joined_df, ['title_desc_id'], how='full')

    # get download date id 
    dl_date_pg_df = spark.read.format('jdbc').options(
        url='jdbc:postgresql://imdb_postgres:5432/imdb',
        driver='org.postgresql.Driver',
        dbtable='dim_download_date',
        user='admin',
        password='password'
        ).load()

    now = datetime.datetime.now()

    df_date_id = dl_date_pg_df.filter(
            (F.col('year') == now.year) & 
            (F.col('month') == now.month) &
            (F.col('day') == now.day))
            
    dl_date_id = df_date_id.select('id').collect()[0][0]

    # insert fact titles df into db with download date, episodes and title desc id
    fact_titles_df = fact_titles_df.withColumn('download_date_id', F.lit(dl_date_id))      

    fact_titles_df.write.format('jdbc').options(
        url='jdbc:postgresql://imdb_postgres:5432/imdb',
        driver='org.postgresql.Driver',
        dbtable='fact_titles',
        user='admin',
        password='password'
        ).mode('append').save()

def main():
    upload_dim_episodes()

if __name__ == '__main__':
    main()
                