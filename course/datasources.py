
from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession. \
    builder. \
    appName("Data Sources"). \
    master("local"). \
    config("spark.jars", "../jars/postgresql-42.2.19.jar"). \
    config("spark.sql.legacy.timeParserPolicy", "LEGACY"). \
    getOrCreate()


def demo_json():
    # read a DF in json format
    cars_df = spark.read. \
        format("json"). \
        option("inferSchema", "true"). \
        option("mode", "failFast"). \
        option("path", "../data/cars"). \
        load()

    cars_df_v2 = spark.read. \
        format("json"). \
        options(mode="failFast", path="../data/cars", inferSchema="true"). \
        load()
    # .load(path) === .option("path", path).load()

    # writing a DF in json format
    cars_df.write. \
        format("json"). \
        mode("overwrite"). \
        option("path", "../data/cars_dupe"). \
        save()
    # Writing modes: overwrite, append, ignore, errorIfExists
    # applicable to all file formats

    cars_schema = StructType([
        StructField("Name", StringType()),
        StructField("Acceleration", DoubleType()),
        StructField("Cylinders", LongType()),
        StructField("Displacement", DoubleType()),
        StructField("Horsepower", LongType()),
        StructField("Miles_per_Gallon", DoubleType()),
        StructField("Origin", StringType()),
        StructField("Weight_in_lbs", LongType()),
        StructField("Year", DateType()),
    ])

    # JSON flags
    cars_df_v3 = spark.read. \
        schema(cars_schema). \
        option("dateFormat", "YYYY-MM-dd"). \
        option("allowSingleQuotes", "true"). \
        option("compression", "uncompressed"). \
        json("../data/cars") # equivalent to .format(...).option("path",...).load()


# CSV
def demo_csv():
    stocks_schema = StructType([
        StructField("company", StringType()),
        StructField("date", DateType()),
        StructField("price", DoubleType())
    ])

    # CSV flags
    stocks_df = spark.read. \
        schema(stocks_schema). \
        option("dateFormat", "MMM d YYYY"). \
        option("header", "false"). \
        option("sep", ","). \
        option("nullValue", ""). \
        csv("../data/stocks") # same as .option("path", "...").format("csv").load()

# Parquet = binary data, high compression, low CPU usage, very fast
# also contains the schema
# the default data format in Spark

    stocks_df.write.save("../data/stocks_parquet")


def demo_text():
    # each row is a value in a DF with a SINGLE column ("value")
    text_df = spark.read.text("../data/lipsum")
    text_df.show()


# reading data from external JDBC (Postgres)
driver = "org.postgresql.Driver"
url = "jdbc:postgresql://localhost:5432/rtjvm"
user = "docker"
password = "docker"

def demo_postgres():
    employees_df = spark.read. \
        format("jdbc"). \
        option("driver", driver). \
        option("url", url). \
        option("user", user). \
        option("password", password). \
        option("dbtable", "public.employees"). \
        load()

    employees_df.show()


"""
Exercise: read the movies DF, then write it as
- tab-separated "CSV"
- parquet
- table "public.movies" in the Postgres DB

Exercise #2: find a way to read the people-1m dataFrame.
Then write it as JSON.
"""
def writing_movies():
    movies_df = spark.read.json("../data/movies")

    # tab-separated
    movies_df.write.\
        format("csv").\
        option("sep", "\t").\
        save("../data/movies_tsv")

    # parquet
    movies_df.write.save("../data/movies_parquet")

    # table in the DB
    movies_df.write.\
        format("jdbc").\
        option("driver", driver). \
        option("url", url). \
        option("user", user). \
        option("password", password). \
        option("dbtable", "public.movies").\
        save()

def demo_read_people():
    people_df = spark.read.\
        format("csv").\
        option("sep", ":").\
        load("../data/people-1m")

    people_df.write.json("../data/people-json")

if __name__ == '__main__':
    demo_csv()
