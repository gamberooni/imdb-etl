from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import when
from pyspark.sql.types import IntegerType
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

def upload_dim_casts():
    spark = SparkSession.builder \
        .appName("casts") \
        .getOrCreate()

    # set config to read from minio
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", MINIO_ACCESS_KEY)
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", MINIO_SECRET_KEY)
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.endpoint", f"http://{MINIO_HOST}")  # must use IP address
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
    spark.sparkContext._jsc.hadoopConfiguration().set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark.sparkContext._jsc.hadoopConfiguration().set("spark.hadoop.fs.s3a.path.style.access", "true")
    spark.sparkContext._jsc.hadoopConfiguration().set("-Dcom.amazonaws.services.s3.enableV4", "true")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.multipart.size", "104857600")        

    title_principals_df = spark.read.csv(f"s3a://{IMDB_BUCKET_NAME}/{IMDB_OBJECT_PREFIX}/title.principals.tsv", sep=r'\t', header=True)  
    title_principals_df = title_principals_df.drop('ordering').drop('job').drop('category')

    title_principals_df = title_principals_df.withColumn("characters", F.regexp_replace(F.col("characters"), '[\[\]\"]', "").alias("replaced"))
    title_principals_df = title_principals_df.withColumn('characters', F.explode(F.split('characters', ',')))
    title_principals_df = title_principals_df.withColumnRenamed('characters', 'character')
    title_casts_df = title_principals_df.filter(
            (F.col('category') == 'self') | 
            (F.col('category') == 'actor') | 
            (F.col('category') == 'actress'))

    # read tsv file into df
    name_basics_df = spark.read.csv(f"s3a://{IMDB_BUCKET_NAME}/{IMDB_OBJECT_PREFIX}/name.basics.tsv", sep=r'\t', header=True)  

    # rename column
    name_basics_df = name_basics_df.withColumnRenamed('primaryName', 'name')

    # calculate age from birth year and death year
    name_basics_df = name_basics_df.withColumn("age", when((F.col("birthYear") != '\\N') & (F.col("deathYear") != '\\N'), (F.col('deathYear').cast(IntegerType()) - F.col('birthYear').cast(IntegerType()))).when((F.col("birthYear") != '\\N') & (F.col("deathYear") == '\\N'), (datetime.datetime.now().year - F.col('birthYear')).cast(IntegerType())).otherwise(None))

    # create is_alive column based on conditions
    is_alive_col = F.when(
        (F.col("birthYear") != '\\N') & (F.col("deathYear") != '\\N'), False
    ).when((F.col("birthYear") != '\\N') & (F.col("deathYear") == '\\N'), True).otherwise(None)

    name_basics_df = name_basics_df.withColumn('is_alive', is_alive_col)

    # drop unused columns
    name_basics_df_dropped = name_basics_df.drop('primaryProfession', 'knownForTitles', 'birthYear', 'deathYear')

    df_casts = title_casts_df.join(name_basics_df_dropped, ['nconst'])
    df_casts = df_casts.withColumn('character', F.when(F.col('character') == '\\N', F.lit(None)).otherwise(F.col('character')))

    df_upload = df_casts.drop('tconst').drop('character').drop_duplicates(['nconst'])

    # insert df into dim_casts table
    df_upload.write.format('jdbc').options(
        url=f'jdbc:postgresql://{POSTGRES_HOST}/imdb',
        driver='org.postgresql.Driver',
        dbtable='casts',
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
        ).mode('append').save()

    titles_df = spark.read.format('jdbc').options(
        url=f'jdbc:postgresql://{POSTGRES_HOST}/imdb',
        driver='org.postgresql.Driver',
        dbtable='titles',
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
        ).load()

    titles_id_df = titles_df.select('id', 'tconst').withColumnRenamed('id', 'title_id')

    casts_from_pg_df = spark.read.format('jdbc').options(
        url=f'jdbc:postgresql://{POSTGRES_HOST}/imdb',
        driver='org.postgresql.Driver',
        dbtable='casts',
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
        ).load()

    casts_id_df = casts_from_pg_df.select('id', 'nconst').withColumnRenamed('id', 'cast_id')

    df_casts_composite_table = df_casts.select('nconst', 'tconst', 'character')

    df_tmp = casts_id_df.join(df_casts_composite_table, ['nconst'])

    df_titles_casts = df_tmp.join(titles_id_df, ['tconst'])
    df_titles_casts = df_titles_casts.drop('nconst').drop('tconst')

    # insert df into titles_casts table
    df_titles_casts.write.format('jdbc').options(
        url=f'jdbc:postgresql://{POSTGRES_HOST}/imdb',
        driver='org.postgresql.Driver',
        dbtable='titles_casts',
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
        ).mode('append').save()

def main():
    upload_dim_casts()

if __name__ == '__main__':
    main()
                