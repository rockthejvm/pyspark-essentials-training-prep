
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

from time import sleep

spark = SparkSession.\
    builder. \
    appName("Spark Job Anatomy"). \
    master("local"). \
    getOrCreate()


# dataframe of numbers
ds1 = spark.range(1, 100000000)
# ds1.show() # action => a job

# narrow transformation
doubled = ds1.select(col("id") * 2)
# doubled.show()

# wide transformation
doubled_repartitioned = doubled.repartition(23) # shuffle
# doubled_repartitioned.count()

# something bigger
ds2 = spark.range(1, 100000000, 2)
ds3 = ds1.repartition(7)
ds4 = ds2.repartition(9)
ds5 = ds3.selectExpr("id * 5 as id")
joined = ds5.join(ds4, "id")
total = joined.selectExpr("sum(id)")
# total.show()

if __name__ == '__main__':
    sleep(10000000)
