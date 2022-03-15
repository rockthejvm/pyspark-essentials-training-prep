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


if __name__ == '__main__':
    rt_stats_df.show()
