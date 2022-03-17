
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession. \
    builder. \
    appName("Joins"). \
    master("local"). \
    config("spark.jars", "../jars/postgresql-42.2.19.jar"). \
    config("spark.sql.legacy.timeParserPolicy", "LEGACY"). \
    getOrCreate()


movies_df = spark.read.json("../data/movies")

# dates
# convert from a string
movies_with_release_dates_df = movies_df.select(
    col("Title"),
    to_date(col("Release_Date"), "dd-MMM-YY").alias("Actual_Release")
)

# date operations
enriched_movies_df = movies_with_release_dates_df.\
    withColumn("Today", current_date()).\
    withColumn("Right_Now", current_timestamp()).\
    withColumn("Movie_Age", datediff(col("Today"), col("Actual_Release")) / 365)

# check for empty date
no_release_known_df = movies_with_release_dates_df.filter(col("Actual_Release").isNull())

# hypothetical
movies_with_2_formats = movies_df.select(col("Title"), col("Release_Date")).\
    withColumn("Date_F1", to_date(col("Release_Date"), "dd-MM-YYYY")).\
    withColumn("Date_F2", to_date(col("Release_Date"), "YYYY-MM-dd")).\
    withColumn("Actual_Date", coalesce(col("Date_F1"), col("Date_F2")))

# structures
movies_struct_df = movies_df.\
    select(col("Title"), struct(col("US_Gross"), col("Worldwide_Gross"), col("US_DVD_Sales")).alias("Profit")) .\
    select(col("Title"), col("Profit").getField("US_Gross").alias("US_Profit"))

# structures - SQL expression strings
movies_struct_df_v2 = movies_df.\
    selectExpr("Title", "(US_Gross, Worldwide_Gross, US_DVD_Sales) as Profit").\
    selectExpr("Title", "Profit.US_Gross as US_Profit")

# very nested data structures
movies_struct_df_v3 = movies_df.\
    selectExpr("Title", "((IMDB_Rating, Rotten_Tomatoes_Rating) as Rating, (US_Gross, Worldwide_Gross, US_DVD_Sales) as Profit) as Success").\
    selectExpr("Title", "Success.Rating.IMDB_Rating as IMDB")

# arrays
movies_with_words_df = movies_df.select(col("Title"), split(col("Title"), " |,").alias("Title_Words"))
#                                                     ^^^^^^^^^^^^^^^^^^^^^^^^^ col object of type ARRAY[String]
# you can have nested arrays

# array operations
array_ops_df = movies_with_words_df.select(
    col("Title"),
    expr("Title_Words[0]"), # the first element in the array
    size(col("Title_Words")), # the length of the array
    array_contains(col("Title_Words"), "Love")
    # a bunch of array_(...) functions
)


if __name__ == '__main__':
    array_ops_df.show()