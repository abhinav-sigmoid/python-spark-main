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

#1
# On each of the days find which stock has moved maximum %age wise in both directions (+ve, -ve)
@app.route('/movement')
def stock_max_movement():
    query1 = "select High, Low, Volume, Date, stock_name, ((Close - Open)/Close)*100 as Percent from stocks"
    pdf = spark.sql(query1)

    pdf.createOrReplaceTempView("new_table")

    query2 = "SELECT mt.stock_name as min_stock, mt.Date, mt.Percent as minPerc FROM new_table mt INNER JOIN (SELECT Date, MIN(Percent) AS MinPercent FROM new_table GROUP BY Date) t ON mt.Date = t.Date AND mt.Percent = t.MinPercent"

    query3 = "SELECT mt.stock_name as max_stock, mt.Date, mt.Percent as maxPerc FROM new_table mt INNER JOIN (SELECT Date, MAX(Percent) AS MinPercent FROM new_table GROUP BY Date) t ON mt.Date = t.Date AND mt.Percent = t.MinPercent"

    new_pdf = spark.sql(query2)
    new_pdf.createOrReplaceTempView("t1")

    new_pdf1 = spark.sql(query3)
    new_pdf1.createOrReplaceTempView("t2")
    query4 = "select t1.Date,t1.min_stock,t1.minPerc,t2.max_stock,t2.maxPerc from t1 join t2 on t1.Date = t2.Date"
    df = spark.sql(query4)
    return jsonify(json.loads(df.toPandas().to_json(orient="table",index=False)))


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
    # new_table = spark.sql(" SELECT Date,company,Open,Close , Close - LAG(Open,1,NULL) OVER (PARTITION BY company ORDER BY Date) as gap FROM stocks")
    # new_table.createOrReplaceTempView("max_min_table")
    query = "SELECT stock_name, Date, Open, Close , Close- LAG(Open, 1, null) OVER (PARTITION BY stock_name ORDER BY Date) " \
            "as diff FROM stocks "
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
