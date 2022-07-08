import json

from flask import Flask, jsonify
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import min, max

app = Flask(__name__)
spark = SparkSession.builder.getOrCreate()
df = spark.read.options(header=True).csv('../data')
df.createOrReplaceTempView('stocks_data')


@app.route('/')
def hello_world():
    return "hello world"


# On each of the days find which stock has moved maximum %age wise in both directions (+ve, -ve)
@app.route('/movement')
def stock_max_movement():
    pass


# 2nd
# Which stock was most traded stock on each day
@app.route("/max_traded_stock")
def most_traded_stock():
    df = spark.sql(
        "select t1.* from `stocks_data` t1 join ( select Date, Max(Volume) AS max_volume from `stocks_data` Group By Date) t2 on t1.Date = t2.Date and t1.Volume = t2.max_volume ")
    data = df.select('*').rdd.flatMap(lambda x: x).collect()
    return jsonify({'Data': data})


# 3rd
# Which stock had the max gap up or gap down opening
# from the previous day close price I.e. (previous day close - current day open price )
@app.route("/max_min_gap")
def max_min_gap_in_stock_price():
    new_table = spark.sql(
        " SELECT Date,company,Open,Close , Close - LAG(Open,1,NULL) OVER (PARTITION BY company ORDER BY Date) as gap FROM stocks_data")
    new_table.createOrReplaceTempView("max_min_table")
    query = "select company, min(gap),max(gap) from max_min_table group by company"
    df = spark.sql(query)
    data = df.select('*').rdd.flatMap(lambda x: x).collect()
    return jsonify({'Data': data})


# 4th
# Which stock has moved maximum from 1st Day data to the latest day
@app.route("/maximum_movement")
def max_movement_from_first_day_to_last_day():
    query = "select distinct company, abs(first_value(Open) over(partition by company order by Date)- first_value(close) over(partition by company order by Date desc) )as maximum_movement from stocks_data"
    df = spark.sql(query)
    data = df.select('*').rdd.flatMap(lambda x: x).collect()
    return jsonify({'Data': data})


# 5th
# Find the standard deviations for each stock over the period
@app.route("/std_deviation")
def standard_deviations_over_the_period():
    query = "SELECT company,stddev(Volume) as Std_deviation_of_stock FROM stocks_data GROUP BY company"
    df = spark.sql(query)
    data = df.select('*').rdd.flatMap(lambda x: x).collect()
    return jsonify({'Data': data})


# 6th
# Find the mean and median prices for each stock
@app.route("/mean_and_median")
def mean_and_median_of_stocks():
    query = "SELECT company,percentile_approx(Open, 0.5) as median_open,percentile_approx(Close, 0.5) as median_close,mean(Open) as mean_of_Open,mean(Close) as mean_of_Close FROM stocks_data GROUP BY company"
    df = spark.sql(query)
    data = df.select('*').rdd.flatMap(lambda x: x).collect()
    return jsonify({'Data': data})


# 7th
# Find the average volume over the period
@app.route("/average_of_stock_volume")
def average_of_stock_volume_over_period():
    query = "select company,AVG(Volume) as Average_of_stock from stocks_data group by company"
    df = spark.sql(query)
    data = df.select('*').rdd.flatMap(lambda x: x).collect()
    return jsonify({'Data': data})


# 8th
# Find which stock has higher average volume
@app.route('/stock_higher_avg_volume')
def stock_higher_avg_volume():
    query = "SELECT company, AVG(Volume) FROM stocks_data GROUP BY company ORDER BY AVG(Volume) DESC LIMIT(1)"
    pdf = spark.sql(query)
    data = (pdf.select('*').rdd.flatMap(lambda x: x).collect())
    return jsonify({'Data': data})


# 9th
# Find the highest and lowest prices for a stock over the period of time
@app.route("/highest_and_lowest_price")
def stock_highest_and_lowest_price():
    query = "select company, max(High) as high_price, min(Low) as low_price from stocks_data group by company"
    df = spark.sql(query)
    data = df.select('*').rdd.flatMap(lambda x: x).collect()
    return jsonify({'Data': data})


if __name__ == "__main__":
    app.run(debug=True, port=5005)
