from pyspark.sql import SparkSession
import time

spark = SparkSession.builder.appName("test").getOrCreate()

print("Spark Version:", spark.version)

time.sleep(300)