from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType, ShortType, DecimalType
import datetime
import os

MINIO_HOST = os.environ['MINIO_HOST']
MINIO_ACCESS_KEY = os.environ['MINIO_ACCESS_KEY']
MINIO_SECRET_KEY = os.environ['MINIO_SECRET_KEY']
POSTGRES_HOST = os.environ['POSTGRES_HOST']
POSTGRES_USER = os.environ['POSTGRES_USER']
POSTGRES_PASSWORD = os.environ['POSTGRES_PASSWORD']
IMDB_BUCKET_NAME = os.environ['IMDB_BUCKET_NAME']
IMDB_OBJECT_PREFIX = os.environ['IMDB_OBJECT_PREFIX']

def set_df_columns_nullable(spark, df, column_list, nullable=True):
    for struct_field in df.schema:
        if struct_field.name in column_list:
            struct_field.nullable = nullable
    df_mod = spark.createDataFrame(df.rdd, df.schema)
    return df_mod

def upload_dim_title_desc():
    spark = SparkSession.builder \
        .appName("titles") \
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
    title_basics_df = spark.read.csv(f"s3a://{IMDB_BUCKET_NAME}/{IMDB_OBJECT_PREFIX}/title.basics.tsv", sep=r'\t', header=True)  

    genres_split = F.split(title_basics_df['genres'], ',')
    
    title_basics_df = title_basics_df.withColumn('genre_1', genres_split.getItem(0))
    title_basics_df = title_basics_df.withColumn('genre_2', genres_split.getItem(1))
    title_basics_df = title_basics_df.withColumn('genre_3', genres_split.getItem(2))
    title_basics_df = title_basics_df.withColumnRenamed('titleType', 'type') \
        .withColumnRenamed('primaryTitle', 'primary_title') \
        .withColumnRenamed('originalTitle', 'original_title') \
        .withColumnRenamed('isAdult', 'is_adult') \
        .withColumnRenamed('startYear', 'start_year') \
        .withColumnRenamed('endYear', 'end_year') \
        .withColumnRenamed('runtimeMinutes', 'runtime_minutes') \
        .drop('genres')

    title_basics_df = title_basics_df.withColumn('end_year', F.when(F.col('end_year') == '\\N', F.lit(None)).otherwise(F.col('end_year'))) 
    title_basics_df = title_basics_df.withColumn('runtime_minutes', F.when(F.col('runtime_minutes') == '\\N', F.lit(None)).otherwise(F.col('runtime_minutes'))) 
    title_basics_df = title_basics_df.withColumn('is_adult', F.when(F.col('is_adult') == '0', F.lit(False)).otherwise(F.lit(True)))   
    title_basics_df = title_basics_df.withColumn('start_year', F.col('start_year').cast(ShortType()))
    title_basics_df = title_basics_df.withColumn('end_year', F.col('end_year').cast(ShortType()))
    title_basics_df = title_basics_df.withColumn('runtime_minutes', F.col('runtime_minutes').cast(ShortType()))

    title_ratings_df = spark.read.csv("s3a://imdb/2021-06/06/title.ratings.tsv", sep=r'\t', header=True)  
    title_ratings_df = title_ratings_df.withColumnRenamed("averageRating", 'av_rating').withColumnRenamed('numVotes', 'num_votes')
    title_ratings_df = title_ratings_df.withColumn('av_rating', F.col('av_rating').cast(DecimalType(10, 1)))
    title_ratings_df = title_ratings_df.withColumn('num_votes', F.col('num_votes').cast(IntegerType()))

    df_final = title_basics_df.join(title_ratings_df, ['tconst'])
    df_final = set_df_columns_nullable(spark, df_final, ['tconst', 'primary_title', 'original_title'], False)

    dl_date_pg_df = spark.read.format('jdbc').options(
        url=f'jdbc:postgresql://{POSTGRES_HOST}/imdb',
        driver='org.postgresql.Driver',
        dbtable='download_date',
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
        ).load()

    now = datetime.datetime.now()

    df_date_id = dl_date_pg_df.filter(
            (F.col('year') == now.year) & 
            (F.col('month') == now.month) &
            (F.col('day') == now.day))
            
    dl_date_id = df_date_id.select('id').collect()[0][0]
    df_final = df_final.withColumn('download_date_id', F.lit(dl_date_id))      

    # insert df into dim_casts table
    df_final.write.format('jdbc').options(
        url=f'jdbc:postgresql://{POSTGRES_HOST}/imdb',
        driver='org.postgresql.Driver',
        dbtable='titles',
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
        ).mode('append').save()

def main():
    upload_dim_title_desc()

if __name__ == '__main__':
    main()
    