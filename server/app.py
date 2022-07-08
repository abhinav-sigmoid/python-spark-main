import flask

import sys

from flask import jsonify

sys.path.append("../")

app = flask.Flask(__name__)

from pyspark.sql import SparkSession


spark = SparkSession \
    .builder \
    .appName("Reading CSV") \
    .master("local[3]") \
    .getOrCreate()

df = spark.read.options(header=True).csv('../data')
df.createOrReplaceTempView('data')


@app.route("/")
def home():
    return jsonify(df.collect())


@app.route("/std_dv_each_stock")
def std_dv_each_stock():
    # Query No - 5
    std_dv_each_stock = spark.sql("select company,STDDEV(open) as STD from data group by company")
    return jsonify(std_dv_each_stock.collect())


@app.route("/mean_each_stock")
def mean_each_stock()
    # Query No - 6
    mean_each_stock = spark.sql("select company,avg(open) as Mean from data group by company")
    return jsonify(mean_each_stock.collect())

def
    avg_volume_stock = spark.sql("select company, avg(Volume) as Average from data group by company")
avg_volume_stock.show()

# @app.route("/")

if __name__ == "__main__":
    app.run(debug=True, port=5005)
