from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("ScalaVersionExample").getOrCreate()

# Get the Scala version
scala_version = spark.sparkContext.version
print("Scala version:", scala_version)

# Stop the Spark session
spark.stop()