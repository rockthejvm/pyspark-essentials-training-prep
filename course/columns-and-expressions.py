
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession. \
    builder. \
    appName("Columns and Expressions"). \
    master("local"). \
    getOrCreate()

cars_df = spark.read.json("../data/cars")

# column objects = data structures that contain (name, type, other metadata) about a particular column
first_column = col("Name")

# selecting (projection) with a column object
car_names_df = cars_df.select(first_column)

# select using just the col names
car_names_df_v2 = cars_df.select("Name", "Horsepower")

# col objects can be transformed into "new" column objects
car_weights_df = cars_df.select(first_column, (col("Weight_in_lbs") / 2.2).alias("Weight_in_kg"))

# select the same thing in MANY ways
car_weights_df_v2 = cars_df.select(
    first_column,
    col("Weight_in_lbs"),
    (col("Weight_in_lbs") / 2.2).alias("Weight_in_kg"),
    expr("Weight_in_lbs / 2.2").alias("Weight_in_kg_2"),
    expr("Weight_in_lbs / 2.2 as Weight_in_kg_3")
)

# select + expr = selectExpr
# the strings you pass are valid SQL expressions
car_weights_df_v3 = cars_df.selectExpr(
    "Name",
    "Weight_in_lbs",
    "Weight_in_lbs / 2.2 as Weight_in_kg"
)

# add a column
cars_with_kg_df = cars_df.withColumn("Weight_in_kg", col("Weight_in_lbs") / 2.2)
cars_with_kg_df_v2 = cars_df.withColumn("Weight_in_kg", expr("Weight_in_lbs / 2.2"))

# rename columns
cars_with_pounds_df = cars_df.withColumnRenamed("Weight_in_lbs", "Weight in pounds")
# careful with col names and special chars, e.g. spaces - use backticks ``
cars_pounds_df = cars_with_pounds_df.selectExpr("`Weight in pounds`")

# remove columns
cars_without_engine_data_df = cars_df.drop("Cylinders", "Displacement")

# filtering
european_cars_df = cars_df.filter(col("Origin") != "USA")
#                                 ^^^^^^^^^^^^^^^^^^^^^^ col object of type boolean
american_cars_df = cars_df.filter("Origin = 'USA'")

# filter chaining
american_powerful_cars_df = cars_df.\
    filter(col("Origin") == "USA").\
    filter(col("Horsepower") > 150)

american_powerful_cars_df_v2 = cars_df.filter((col("Origin") == "USA") & (col("Horsepower") > 150)) # chain boolean col objects
american_powerful_cars_df_v3 = cars_df.filter("Origin = 'USA' and Horsepower > 150")

# union = stitching DFs together (all DFs must have the same schema)
more_cars_df = spark.read.json("../data/cars") # a different Df
total_cars_df = cars_df.union(more_cars_df)

# distinct values
origins_df = cars_df.select("Origin").distinct()

"""
Exercises
    1. Read the movies DF and select a few columns that are interesting
    2. Create another col called Total_Profit = US_Gross + Worldwide_Gross + DVD_Sales
    3. Select all COMEDY with IMDB rating > 6
    
    Bonus: use multiple versions
"""

# 1
movies_df = spark.read.format("json").option("inferSchema", "true").load("../data/movies")
# title, genre, release date, IMDB rating
movies_ex1 = movies_df.selectExpr("Title", "Major_Genre", "Release_Date", "IMDB_Rating")
movies_ex1_v2 = movies_df.select("Title", "Major_Genre", "Release_Date", "IMDB_Rating")
movies_ex1_v3 = movies_df.select(col("Title"), col("Major_Genre"), col("Release_Date"), col("IMDB_Rating"))
movies_ex1_v4 = movies_df.select(expr("Title"), expr("Major_Genre"), expr("Release_Date"), expr("IMDB_Rating"))

# 2
movies_profit_df = movies_df.withColumn("Total_Profit", expr("US_DVD_Sales + US_Gross + Worldwide_Gross"))
movies_profit_df_erick = movies_df.select(
    "Title",
    "US_Gross",
    "Worldwide_Gross",
    "US_DVD_Sales",
    expr("US_Gross + Worldwide_Gross + US_DVD_Sales as Total_Profit")
)
movies_profit_df_v2 = movies_df.selectExpr(
    "Title",
    "US_Gross",
    "Worldwide_Gross",
    "US_DVD_Sales",
    "US_Gross + Worldwide_Gross + US_DVD_Sales as Total_Profit"
)
movies_profit_df_v3 = movies_df.select(
    col("Title"),
    col("US_Gross"),
    col("Worldwide_Gross"),
    col("US_DVD_Sales"),
    expr("US_Gross + Worldwide_Gross + US_DVD_Sales as Total_Profit")
)
# mix & match

# 3
at_least_mediocre_comedies_df = movies_df.\
    filter("Major_Genre = 'Comedy' and IMDB_Rating > 6").\
    select("Title", "IMDB_Rating")

at_least_mediocre_comedies_df_jackson = movies_df.\
    filter((col("IMDB_rating") > 6) & (upper(col("Major_Genre")) == "COMEDY"))

at_least_mediocre_comedies_df_daniel = movies_df.\
    filter(col("IMDB_rating") > 6).\
    filter(col("Major_Genre") == "Comedy")


if __name__ == '__main__':
    at_least_mediocre_comedies_df.show()
