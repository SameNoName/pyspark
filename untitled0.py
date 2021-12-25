# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import udf, to_date, date_format
from pyspark.sql.types import TimestampType

spark = SparkSession.builder.appName("Basic").getOrCreate()

df = spark.read.csv('access.txt', sep = ' ', inferSchema=True)
df = df.select(df["_c0"].alias("IP"),df["_c3"].alias("buffDate"),\
               df["_c4"].alias("Request"),df["_c5"].alias("Status"),\
               df["_c6"].alias("Port"),df["_c7"].alias("URL"),\
               df["_c8"].alias("Browser"),df["_c9"].alias("?"),)


print('Count of the error 404: ', df.filter(df['Status']==404).count())


print('Count of unique URL: ', df.select('URL').distinct().count())


func =  udf (lambda x: datetime.strptime(x, '[%d/%b/%Y:%H:%M:%S]'), TimestampType())
df = df.withColumn('buffDate1', func(df['buffDate']))
df = df.drop('buffDate')
func1 = udf (lambda x: datetime.strptime(x, '*:%H:%M:%S'), TimestampType())
df = df.withColumn("Date",to_date("buffDate1"))
df = df.withColumn('Time',date_format('buffDate1', 'h:m:s a'))
df = df.drop('buffDate1')
df.show()


print('Count of HTTP statuses: ', df.select('Status').distinct().count())