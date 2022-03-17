from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession. \
    builder. \
    appName("Data Sources"). \
    master("local"). \
    getOrCreate()

cars_df = spark.read.json("../data/cars")

# store as a Spark table
cars_df.createOrReplaceTempView("cars")

# select American cars
# regular DF API
american_cars_df = cars_df.filter(col("Origin") == "USA").select(col("Name"))
# run SQL queries on top of DFs known to Spark under a certain name
american_cars_df_v2 = spark.sql("select Name from cars where Origin = 'USA'") # returns a DF

# store DFs as Spark tables (files known to Spark)
cars_df.write.mode("overwrite").saveAsTable("cars")

"""
Exercises: replicate the exercises in the Joins lesson with Spark SQL.
Read the tables in the Postgres database: employees, salaries, dept_emp
1. show all employees and their max salary over time
2. show all employees who were never managers
3. for every employee, find the difference between their salary (current/latest) and 
    the max salary of their department (departments table)
"""

driver = "org.postgresql.Driver"
url = "jdbc:postgresql://localhost:5432/rtjvm"
user = "docker"
password = "docker"


def read_table(table_name):
    return spark.read. \
        format("jdbc"). \
        option("driver", driver). \
        option("url", url). \
        option("user", user). \
        option("password", password). \
        option("dbtable", "public." + table_name). \
        load()

employees_df = read_table("employees")
salaries_df = read_table("salaries")
dept_managers_df = read_table("dept_manager")
dept_emp_df = read_table("dept_emp")
departments_df = read_table("departments")


if __name__ == '__main__':
    american_cars_df_v2.show()
