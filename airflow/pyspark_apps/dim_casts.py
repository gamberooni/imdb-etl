from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import when
from pyspark.sql.types import IntegerType
import datetime

def upload_dim_casts():
    spark = SparkSession.builder \
        .appName("IMDb ETL Task 3") \
        .getOrCreate()

    title_principals_df = spark.read.csv("s3a://imdb/2021-06/06/title.principals.tsv", sep=r'\t', header=True)  

    title_principals_df = title_principals_df.drop('ordering').drop('job').drop('category')

    title_principals_df = title_principals_df.withColumn("characters", F.regexp_replace(F.col("characters"), '[\[\]\"]', "").alias("replaced"))
    title_principals_df = title_principals_df.withColumn('characters', F.explode(F.split('characters', ',')))
    title_principals_df = title_principals_df.withColumnRenamed('characters', 'character')
    title_casts_df = title_principals_df.filter(
            (F.col('category') == 'self') | 
            (F.col('category') == 'actor') | 
            (F.col('category') == 'actress'))

    # read tsv file into df
    name_basics_df = spark.read.csv("name.basics.tsv", sep=r'\t', header=True)

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
        url='jdbc:postgresql://imdb_postgres:5432/imdb',
        driver='org.postgresql.Driver',
        dbtable='dim_casts',
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

    fact_titles_pg_df = spark.read.format('jdbc').options(
        url='jdbc:postgresql://imdb_postgres:5432/imdb',
        driver='org.postgresql.Driver',
        dbtable='fact_titles',
        user='admin',
        password='password'
        ).load()
    fact_titles_pg_df = fact_titles_pg_df.withColumnRenamed('title_desc_id', 'title_id')      

    title_desc_id_df = fact_titles_pg_df.join(title_desc_id_df, ['title_id']).select('title_id', 'tconst')  # now title_id and tconst match

    casts_from_pg_df = spark.read.format('jdbc').options(
        url='jdbc:postgresql://imdb_postgres:5432/imdb',
        driver='org.postgresql.Driver',
        dbtable='dim_casts',
        user='admin',
        password='password'
        ).load()

    casts_id_df = casts_from_pg_df.select('id', 'nconst')
    casts_id_df = casts_id_df.withColumnRenamed('id', 'cast_id')

    df_casts_composite_table = df_casts.select('nconst', 'tconst', 'character')
    df_tmp = casts_id_df.join(df_casts_composite_table, ['nconst'])

    df_titles_casts = df_tmp.join(title_desc_id_df, ['tconst'])
    df_titles_casts = df_titles_casts.drop('nconst').drop('tconst')

    # insert df into titles_casts table
    df_titles_casts.write.format('jdbc').options(
        url='jdbc:postgresql://imdb_postgres:5432/imdb',
        driver='org.postgresql.Driver',
        dbtable='titles_casts',
        user='admin',
        password='password'
        ).mode('append').save()


def main():
    upload_dim_casts()

if __name__ == '__main__':
    main()
                