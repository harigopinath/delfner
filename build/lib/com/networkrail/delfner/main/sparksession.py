from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .getOrCreate())


df = spark.range(10)

df.show()
