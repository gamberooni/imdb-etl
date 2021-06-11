from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, ShortType
import datetime

def upload_download_date():
    spark = SparkSession.builder \
        .appName("IMDb ETL Task - Download Date") \
        .getOrCreate()

    # create a df with year, month, day with today's date
    now = datetime.datetime.now()

    data = [(now.year,now.month,now.day)]

    schema = StructType([ \
        StructField("year",ShortType(),True), \
        StructField("month",ShortType(),True), \
        StructField("day",ShortType(),True), \
    ])
    
    dl_date_df = spark.createDataFrame(data=data,schema=schema)

    # insert dataset download date into db
    dl_date_df.write.format('jdbc').options(
        url='jdbc:postgresql://imdb_postgres:5432/imdb',
        driver='org.postgresql.Driver',
        dbtable='dim_download_date',
        user='admin',
        password='password'
        ).mode('append').save()    

def main():
    upload_download_date()

if __name__ == '__main__':
    main()
