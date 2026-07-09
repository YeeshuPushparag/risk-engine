# /app/test_spark.py

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("test").getOrCreate()

print("Spark Version:", spark.version)
print("Count:", spark.range(100).count())

spark.stop()