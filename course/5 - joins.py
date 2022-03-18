
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession. \
    builder. \
    appName("Joins"). \
    master("local"). \
    config("spark.jars", "../jars/postgresql-42.2.19.jar"). \
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

# 1
max_salary_per_emp_df = salaries_df.groupBy("emp_no").agg(max("salary").alias("max_salary"))
emp_max_salary_df = employees_df.join(max_salary_per_emp_df, "emp_no")

# 2
manager_condition = employees_df.emp_no == dept_managers_df.emp_no
emp_never_managers_df = employees_df.join(dept_managers_df, manager_condition, "left_anti")

# 3
# salary for every employee WITHIN their department
# join_condition = salaries_df.emp_no == dept_emp_df.emp_no
# employee_dept_salary_df = salaries_df.join(dept_emp_df, join_condition, "inner")
#
# # max salary for every department
# max_dept_salary_df = employee_dept_salary_df.groupBy(col("dept_no")).agg(max("salary").alias("Max_Dept_Salary"))
#
#
# join_condition = max_dept_salary_df.dept_no == employee_dept_salary_df.dept_no
# employee_dept_salary_max_df = max_dept_salary_df.join(employee_dept_salary_df, join_condition, "inner")
#
# employee_dept_salary_diff_df = employee_dept_salary_max_df.withColumn("salary_dff", expr("Max_Dept_Salary-salary"))

"""
Max salary per department:

    select max(s.salary) maxs, d.dept_name dname
    from salaries s, departments d, employees e, dept_emp de
    where
        e.emp_no = de.emp_no
    and d.dept_no = de.dept_no
    and s.emp_no = e.emp_no
    group by d.dept_name
"""
max_salaries_df = dept_emp_df.\
    join(salaries_df, "emp_no").\
    groupBy("dept_no").\
    agg(max("salary").alias("max_salary"))
#  dept_no, max_salary

# latest salary date for every employee
latest_salary_date_df = salaries_df.\
    groupBy("emp_no").\
    agg(max("from_date").alias("from_date"))

# get latest salary (number) for every employee
latest_salaries_df = latest_salary_date_df.\
    join(salaries_df, ["emp_no", "from_date"])
# "emp_no", "from_date", "salary"

# diff between current salary - max salary of department
diff_in_salary_df = employees_df.\
    join(latest_salaries_df, "emp_no").\
    join(dept_emp_df, "emp_no").\
    join(max_salaries_df, "dept_no") .\
    selectExpr("first_name", "last_name", "emp_no", "dept_no", "max_salary - salary as salary_diff")


if __name__ == '__main__':
    diff_in_salary_df.show()