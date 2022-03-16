
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession. \
    builder. \
    appName("Joins"). \
    master("local"). \
    getOrCreate()


guitars_df = spark.read.json("../data/guitars")
guitar_players_df = spark.read.json("../data/guitarPlayers")
bands_df = spark.read.json("../data/bands")

# inner joins = all rows from the "left" and "right" DF for which the condition is true
join_condition = guitar_players_df.band == bands_df.id
guitarists_bands_df = guitar_players_df.join(bands_df, join_condition, "inner")
#                     ^^ "left" DF            ^^ "right" DF

# differentiate the "name" column - use the reference from the original DF
guitarists_bands_upper_df = guitarists_bands_df.select(upper(bands_df.name))

# left outer = everything in the inner join + all the rows in the LEFT DF that were not matched (with nulls in the cols for the right DF)
guitar_players_df.join(bands_df, join_condition, "left_outer")

# right outer = everything in the inner join + all the rows in the RIGHT DF that were not matched (with nulls in the cols for the left DF)
guitar_players_df.join(bands_df, join_condition, "right_outer")

# full outer join = everythin in the inner join + all the rows in BOTH DFs that were not matched (with nulls in the other DF's cols)
guitar_players_df.join(bands_df, join_condition, "outer")

# join on a single column
# guitar_players_df.join(bands_df, "id")

# left semi joins = everything in the LEFT DF for which there is a row in the right DF for which the condition is true
# more like a filter
# equivalent SQL: select * from guitar_players WHERE EXISTS (...)
guitar_players_df.join(bands_df, join_condition, "left_semi")

# left anti joins = everything in the LEFT DF for which there is __NO__ row in the right DF for which the condition is true
guitar_players_df.join(bands_df, join_condition, "left_anti").show()

# joins are WIDE transformations**

"""
Exercises
Read the tables in the Postgres database: employees, salaries, dept_emp
1. show all employees and their max salary over time
2. show all employees who were never managers
3. for every employee, find the difference between their salary (current/latest) and 
    the max salary of their job/department
"""

if __name__ == '__main__':
    pass