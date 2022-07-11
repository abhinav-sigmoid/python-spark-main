    from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf, to_utc_timestamp, coalesce
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, IntegerType, ArrayType, MapType
from datetime import datetime
import pytz
import os

spark = SparkSession \
    .builder \
    .appName("Reading CSV") \
    .master("local[3]") \
    .getOrCreate()

# file_path = ['../data/AAPL.csv',
#              '../data/GOOG.csv',
#              '../data/IBM.csv',
#              '../data/META.csv',
#              '../data/NFLX.csv']


df = spark.read.options(header=True).csv('../data')
# df = spark.read.options(header=True).csv(file_path)
df.show()
# Query No - 2
df.createOrReplaceTempView('data')
most_traded_stock_each_day = spark.sql(
    "select company,max(Volume) from data group by company")
most_traded_stock_each_day.show()
# Query No - 5
std_dv_each_stock = spark.sql("select company,STDDEV(open) as STD from data group by company")
std_dv_each_stock.show()
# Query No - 6

mean_each_stock = spark.sql("select company,avg(open) as Mean from data group by company")
# median_each_stock = spark.sql("SELECT company,((SELECT TOP 1 open FROM (SELECT TOP 50 PERCENT open FROM data WHERE open IS NOT NULL ORDER BY open ASC) FirstHalf ORDER BY open DESC) + (SELECT TOP 1 open FROM (SELECT TOP 50 PERCENT open FROM data WHERE open IS NOT NULL ORDER BY open DESC) SecondHalf ORDER BY open ASC)) / 2 AS Median group by company")
# mean_each_stock.show()
# median_each_stock = spark.sql("select company,median(open) as Median from data group by company")
# median_each_stock.show()

# Query No - 7

avg_volume_stock = spark.sql("select company, avg(Volume) as Average from data group by company")
avg_volume_stock.show()

# Query No - 8
avg_max_volume_stock = spark.sql(
    "select company,avg(Volume) as AVG from data group by company order by AVG DESC limit 1")
avg_max_volume_stock.show()

# Query No - 9
high_less_price_stock = spark.sql(
    "select company,max(open) as Max_Stock_Price,min(open) as Min_Stock_Price from data group by company")
high_less_price_stock.show()
