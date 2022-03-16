from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession. \
    builder. \
    appName("Aggregations"). \
    master("local"). \
    getOrCreate()

# support DF
movies_df = spark.read.json("../data/movies")

# counting
all_movies_count_df = movies_df.selectExpr("count(*)")
genres_count_df = movies_df.select(count(col("Major_Genre"))) # counts all values which are not null
genres_count_df_v2 = movies_df.selectExpr("count(Major_Genre)") # same
# count() on a DF RETURNS A NUMBER
genres_count_number = movies_df.select("Major_Genre").count() # includes nulls

# count distinct
unique_genres_df = movies_df.select(countDistinct(col("Major_Genre")))
unique_genres_df_v2 = movies_df.selectExpr("count(DISTINCT Major_Genre)")

# math aggregations
# min/max
max_rating_df = movies_df.select(max(col("IMDB_Rating")))
max_rating_df_v2 = movies_df.selectExpr("max(IMDB_Rating)")

# sum values in a column
us_industry_total_df = movies_df.select(sum(col("US_Gross")))
us_industry_total_df_v2 = movies_df.selectExpr("sum(US_Gross)")

# avg
avg_rt_rating_df = movies_df.select(avg(col("Rotten_Tomatoes_Rating")))

# mean/standard dev
rt_stats_df = movies_df.select(
    mean(col("Rotten_Tomatoes_Rating")),
    stddev(col("Rotten_Tomatoes_Rating"))
)

# Grouping
# nulls are also considered
count_by_genre_df = movies_df.\
    groupBy(col("Major_Genre")).\
    count()

avg_rating_by_genre_df = movies_df.\
    groupBy(col("Major_Genre")).\
    avg("IMDB_Rating")

# multiple aggregations
aggregations_by_genre_df = movies_df.\
    groupBy(col("Major_Genre")).\
    agg(
        # use strings here for column names
        count("*").alias("N_Movies"),
        avg("IMDB_Rating").alias("Avg_Rating")
    )

# sorting
best_movies_df = movies_df.orderBy(col("IMDB_Rating").desc())
# sorting works for numerical, strings (lexicographic), dates

# put nulls first or last
proper_worst_movies_df = movies_df.orderBy(col("IMDB_Rating").asc_nulls_last())

"""
Exercises
    1. Sum up ALL the profits of ALL the movies in the dataset
    2. Count how many distinct directors we have
    3. Show the mean+stddev for US gross revenue
    4. Compute the average IMDB rating + average US gross PER DIRECTOR
    5. Show the average difference between IMDB rating and Rotten Tomatoes rating
"""

if __name__ == '__main__':
    aggregations_by_genre_df.show()
