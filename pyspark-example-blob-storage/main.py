
# Import the necessary modules
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Create a SparkSession
#spark = SparkSession.builder \
#   .appName("My App") \
#   .getOrCreate()

#spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()

spark = SparkSession.builder \
    .master("spark://localhost:7077") \
    .appName("My Spark Application") \
    .getOrCreate()

rdd = spark.sparkContext.parallelize(range(1, 100))

print("THE SUM IS HERE: ", rdd.sum())
# Stop the SparkSession
spark.stop()