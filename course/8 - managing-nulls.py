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

# select the first non-null column out of many cols
first_rating_df = movies_df.select(
    col("Title"),
    col("Rotten_Tomatoes_Rating"),
    col("IMDB_Rating"),
    coalesce(col("Rotten_Tomatoes_Rating"), col("IMDB_Rating") * 10).alias("First_Rating")
)

# checking for null - isNull(), isNotNull()
no_rt_rating_df = movies_df.filter(col("Rotten_Tomatoes_Rating").isNull()).select("Title", "IMDB_Rating")

# nulls when sorting
best_movies_df = movies_df.select("Title", "IMDB_Rating").orderBy(col("IMDB_Rating").desc_nulls_last())

# replace nulls
movies_zero_rating_df = movies_df.na.fill(0, ["IMDB_Rating", "Rotten_Tomatoes_Rating"]) # replacing nulls with 0

# replace nulls with certain values for certain columns
movies_zero_rating_df_v2 = movies_df.na.fill({
    "IMDB_Rating": 0,
    "Rotten_Tomatoes_Rating": 10,
    "Director": "Unknown"
})

# complex ops, as SQL expression strings
# ifnull, nvl, nullif, nvl2
movies_null_ops_df = movies_df.selectExpr(
    "Title",
    "IMDB_Rating",
    "Rotten_Tomatoes_Rating",
    "ifnull(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as ifnull", # ifnull == coalesce for 2 columns
    "nvl(Rotten_Tomatoes_Rating, IMDB_Rating * 10)", # nvl == ifnull
    "nullif(Rotten_Tomatoes_Rating, IMDB_Rating * 10)", # nullif returns null if the two values are EQUAL, else first value
    "nvl2(Rotten_Tomatoes_Rating, IMDB_Rating * 10, 0.0)" # nvl2(c1, c2, v) == if (c1 != null) c2 else v
)


if __name__ == '__main__':
    first_rating_df.show()
