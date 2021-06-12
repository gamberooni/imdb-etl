from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import when
from pyspark.sql.types import IntegerType
import datetime

def upload_dim_crew():
    spark = SparkSession.builder \
        .appName("IMDB ETL Task 4") \
        .getOrCreate()

    # set config to read from minio
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", "admin")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "password")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://192.168.0.188:9000")  # must use IP address
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
    spark.sparkContext._jsc.hadoopConfiguration().set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark.sparkContext._jsc.hadoopConfiguration().set("spark.hadoop.fs.s3a.path.style.access", "true")
    spark.sparkContext._jsc.hadoopConfiguration().set("-Dcom.amazonaws.services.s3.enableV4", "true")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.multipart.size", "104857600")    

    title_principals_df = spark.read.csv("s3a://imdb/2021-06/06/title.principals.tsv", sep=r'\t', header=True)  
    title_crew_df = title_principals_df.filter(
            (F.col('category') != 'self') &
            (F.col('category') != 'actor') & 
            (F.col('category') != 'actress'))

    title_crew_df = title_crew_df.drop('job').drop('characters').drop('ordering')
    title_crew_df = title_crew_df.withColumnRenamed('category', 'role')

    # read tsv file into df
    name_basics_df = spark.read.csv("s3a://imdb/2021-06/06/name.basics.tsv", sep=r'\t', header=True)

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

    df_crew = title_crew_df.join(name_basics_df_dropped, ['nconst'])

    df_upload = df_crew.drop('tconst').drop('role').drop_duplicates(['nconst'])

    df_upload.write.format('jdbc').options(
        url='jdbc:postgresql://imdb_postgres:5432/imdb',
        driver='org.postgresql.Driver',
        dbtable='dim_crew',
        user='admin',
        password='password'
        ).mode('append').save()

    title_desc_df = spark.read.format('jdbc').options(
        url='jdbc:postgresql://imdb_postgres:5432/imdb',
        driver='org.postgresql.Driver',
        dbtable='dim_title_desc',
        user='admin',
        password='password'
        ).load()

    title_desc_id_df = title_desc_df.select('id', 'tconst')
    title_desc_id_df = title_desc_id_df.withColumnRenamed('id', 'title_id')

    crew_from_pg_df = spark.read.format('jdbc').options(
        url='jdbc:postgresql://imdb_postgres:5432/imdb',
        driver='org.postgresql.Driver',
        dbtable='dim_crew',
        user='admin',
        password='password'
        ).load()

    crew_id_df = crew_from_pg_df.select('id', 'nconst')
    crew_id_df = crew_id_df.withColumnRenamed('id', 'crew_id')

    df_crew_composite_table = df_crew.select('nconst', 'tconst', 'role')

    df_tmp = crew_id_df.join(df_crew_composite_table, ['nconst'])

    df_titles_crew = df_tmp.join(title_desc_id_df, ['tconst'])
    df_titles_crew = df_titles_crew.drop('nconst').drop('tconst')

    df_titles_crew.write.format('jdbc').options(
        url='jdbc:postgresql://imdb_postgres:5432/imdb',
        driver='org.postgresql.Driver',
        dbtable='titles_crew',
        user='admin',
        password='password'
        ).mode('append').save()

def main():
    upload_dim_crew()

if __name__ == '__main__':
    main()
