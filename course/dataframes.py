
from pyspark.sql import SparkSession
from pyspark.sql.types import *

# spark session = interact with the main Spark API (DataFrames)
spark = SparkSession.\
    builder. \
    appName("Introduction to Spark"). \
    master("local"). \
    getOrCreate()

def demo_first_df():
    # read a DataFrame from a file
    first_df = spark.read. \
        format("json"). \
        option("inferSchema", "true"). \
        load("../data/cars")

    # print a DF as an ASCII table
    first_df.show()
    # show the DF's structure i.e.
    first_df.printSchema()


def demo_manual_schema():
    # specify a schema manually
    cars_schema = StructType([
        StructField("Name", StringType()),
        StructField("Acceleration", DoubleType()),
        StructField("Cylinders", LongType()),
        StructField("Displacement", DoubleType()),
        StructField("Horsepower", LongType()),
        StructField("Miles_per_Gallon", DoubleType()),
        StructField("Origin", StringType()),
        StructField("Weight_in_lbs", LongType()),
        StructField("Year", StringType()),
    ])

    # reading a DF with a manual schema
    cars_manual_schema_df = spark.read. \
        format("json"). \
        schema(cars_schema). \
        load("../data/cars")
    cars_manual_schema_df.show()
    print(cars_manual_schema_df.count()) # the number of rows in the DF


"""
    Exercises
    Read another file from the data dir e.g. movies
    - print its schema
    - movies.count()
"""

if __name__ == '__main__':
    demo_manual_schema()