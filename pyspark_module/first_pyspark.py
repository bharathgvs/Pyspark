from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("First Pyspark Program").master("local") \
        .getOrCreate()

df = spark.read.option("header", "true").option("inferSchema", "true").csv("personal_transactions.csv")

df.show(10, truncate=0)