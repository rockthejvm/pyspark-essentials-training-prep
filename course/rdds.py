
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

# SparkSession is the entry point for the HIGH-LEVEL API (DataFrames, Spark SQL)
spark = SparkSession. \
    builder. \
    appName("Joins"). \
    master("local"). \
    getOrCreate()

# SparkContext the entry point for the LOW-LEVEL API (RDDs)
sc = spark.sparkContext

# we can build RDDs out of local collections
numbers = range(1, 1000000) # range of numbers
numbers_rdd = sc.parallelize(numbers) # RDD of numbers

# read a file "manually"
rows = open("../data/stocks/aapl.csv").read().split("\n") # a list of strings (rows in the original file)
tokens = [row.split(",") for row in rows] # a list of arrays [stock name, date, price]
stocks = [(token[0], token[1], token[2]) for token in tokens] # a list of tuples
stocks_rdd = sc.parallelize(stocks) # an RDD of tuples

def split_row(row):
    return row.split(",")

# read a file in parallel
stocks_rdd_v2 = sc.textFile("../data/stocks/aapl.csv"). \
    map(split_row). \
    filter(lambda tokens: float(tokens[2]) > 15)
    # chain transformations with map, filter, flatMap

# RDD API != DF API

# read from a DF
stocks_df = spark.read.csv("../data/stocks").\
    withColumnRenamed("_c0", "company"). \
    withColumnRenamed("_c1", "date"). \
    withColumnRenamed("_c2", "price")

stocks_rdd_v3 = stocks_df.rdd # an RDD of all the rows in the DF
prices_rdd = stocks_rdd_v3.map(lambda row: row.price)

# RDD to DF
# condition: the RDD must contain Spark Rows (data structures conforming to a schema)
stocks_df_v2 = spark.createDataFrame(stocks_rdd_v3)

"""
Use cases for RDDs
- the computations that cannot work on DFs/Spark SQL API
- very custom perf optimizations
"""

# RDD transformations
# map, filter, flatMap

# distinct
company_names_rdd = stocks_rdd_v3.map(lambda row: row.company).distinct()

# counting
total_entries = stocks_rdd_v3.count() # action - the RDD must be evaluated

# min and max
aapl_stocks_rdd = stocks_rdd_v3.filter(lambda row: row.company == "AAPL").map(lambda row: row.price)
max_aapl = aapl_stocks_rdd.max()

# reduce
sum_prices = aapl_stocks_rdd.reduce(lambda x,y: x + y) # can use ANY Python function here

# grouping
grouped_stocks_rdd = stocks_rdd_v3.groupBy(lambda row: row.company) # can use ANY grouping criterion as a Python function
# grouping is expensive

if __name__ == '__main__':
    print(company_names_rdd.collect())
