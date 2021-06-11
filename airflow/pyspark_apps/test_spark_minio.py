from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, ShortType
import datetime

def test_spark_minio():
    spark = SparkSession.builder \
        .appName("test spark and minio") \
        .getOrCreate()

    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", "admin")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "password")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://172.18.0.15:9000")  # must not use 'localhost'
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
    spark.sparkContext._jsc.hadoopConfiguration().set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark.sparkContext._jsc.hadoopConfiguration().set("spark.hadoop.fs.s3a.path.style.access", "true")
    spark.sparkContext._jsc.hadoopConfiguration().set("-Dcom.amazonaws.services.s3.enableV4", "true")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.multipart.size", "104857600")    

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
    dl_date_df.write.save("s3a://testing/", format='csv', header=True)

def main():
    test_spark_minio()

if __name__ == '__main__':
    main()
    