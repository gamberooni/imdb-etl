from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, ShortType
import datetime
import os 

POSTGRES_HOST = os.environ['POSTGRES_HOST']
POSTGRES_USER = os.environ['POSTGRES_USER']
POSTGRES_PASSWORD = os.environ['POSTGRES_PASSWORD']

def upload_download_date():
    spark = SparkSession.builder \
        .appName("download_date") \
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
        url=f'jdbc:postgresql://{POSTGRES_HOST}/imdb',
        driver='org.postgresql.Driver',
        dbtable='download_date',
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
      ).mode('append').save()

def main():
    upload_download_date()

if __name__ == '__main__':
    main()
