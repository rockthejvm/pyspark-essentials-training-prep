from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession. \
    builder. \
    appName("Joins"). \
    master("local"). \
    config("spark.jars", "../jars/postgresql-42.2.19.jar"). \
    getOrCreate()

movies_df = spark.read.json("../data/movies")

def demo_literal_values():
    meaning_of_life_df = movies_df.select(col("Title"), lit(42).alias("MOL"))
    meaning_of_life_df.show()

def demo_booleans():
    drama_filter = movies_df.Major_Genre == "Drama" # column object of TYPE boolean
    good_rating_filter = movies_df.IMDB_Rating > 7.0
    # can use & (and), | (or), ~ (not)
    good_drama_filter = good_rating_filter & drama_filter

    # can use boolean column objects as arguments to filter
    good_dramas_df = movies_df.filter(good_drama_filter).select("Title")

    # can add the col object as a column/property for every row
    movies_with_good_drama_condition_df = movies_df.select(col("Title"), good_drama_filter.alias("IsItAGoodDrama"))
    # can filter using the true/false value of a column
    good_dramas_df_v2 = movies_with_good_drama_condition_df.filter("IsItAGoodDrama")

    # negation
    bad_drama_filter = ~good_drama_filter
    bad_dramas = movies_df.select(col("Title"), bad_drama_filter)

def demo_numerical_ops():
    movies_avg_ratings_df = movies_df.select(
        col("Title"),
        (col("Rotten_Tomatoes_Rating") / 10 + col("IMDB_Rating")) / 2
    )
    # can use ==, >=, >, <, <= to obtain boolean col objects


# Pearson correlation - for numerical fields
# a number [-1, 1]
# is an "action" (the DF must be evaluated)
rating_correlation = movies_df.stat.corr("IMDB_Rating", "Rotten_Tomatoes_Rating")

def demo_string_ops():
    movies_df.select(initcap(col("Title"))) # capitalize initials of every word in the string
    # upper(...), lower(...) to uppercase/lowercase
    movies_df.filter(col("Title").contains("love"))


cars_df = spark.read.json("../data/cars")
def demo_regexes():
    regexString = "volkswagen|vw"
    vw_df = cars_df.select(
        col("Name"),
        regexp_extract(col("Name"), regexString, 0).alias("regex_extract")
    ).filter(col("regex_extract") != "")

    vw_df.show()

    vw_new_name_df = vw_df.select(
        col("Name"),
        regexp_replace(col("Name"), regexString, "Volkswagen").alias("replacement")
    )
    vw_new_name_df.show()

"""
Exercise
    Filter the cars DF, return all cars whose name contains either element of the list
    - contains function
    - regexes
"""

def get_car_names():
    return ["Volkswagen", "Mercedes-Benz", "Ford"]




if __name__ == '__main__':
    demo_regexes()