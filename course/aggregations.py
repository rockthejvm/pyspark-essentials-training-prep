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

# 1
# careful with nulls
all_profits_df = movies_df.selectExpr("sum(Worldwide_Gross + US_DVD_Sales - Production_Budget) as Total_Profit")
all_profits_df_v2 = movies_df.selectExpr("sum(Worldwide_Gross) + sum(US_DVD_Sales) - sum(Production_Budget) as Total_Profit")

# 2
unique_director_count_df = movies_df.select(countDistinct(col("Director")))
dist_director_df = movies_df.selectExpr("count(Distinct Director)")

# 3
US_Gross_stats_df = movies_df.select(
    mean(col("US_Gross")),
    stddev(col("US_Gross"))
)

# 4
aggregations_by_director_df_lai = movies_df.\
    groupBy(col("Director")).\
    agg(
        # use strings here for column names
        avg("US_Gross").alias("Avg_US_Gross"),
        avg("IMDB_Rating").alias("Avg_Rating")
    )

# 5
# normalize the columns compared
rotten_imdb_avg_diff_jackson = movies_df.\
    select((avg(col("IMDB_rating") * 10 - col("Rotten_Tomatoes_Rating"))).\
           alias("Avg_IMDB_RT_Ppt_Diff"))

# careful of avg(difference) vs difference of avgs
avg_diff_two_ratings_df = movies_df.agg(
        avg("IMDB_Rating").alias("Avg_IMDB")
        ,avg("Rotten_Tomatoes_Rating").alias("Avg_Rotten")
).withColumn("Ratings_diff", col("Avg_IMDB") * 10 - col("Avg_Rotten"))

if __name__ == '__main__':
    all_profits_df.show()
    all_profits_df_v2.show()
