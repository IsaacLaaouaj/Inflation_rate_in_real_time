from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("InflationStream").getOrCreate()
df = spark.readStream.format("kafka")\
    .option("kafka.bootstrap.servers", "kafka:9092")\
    .option("subscribe", "precios")\
    .load()

# Transformación (limpieza)
transformed = df.selectExpr("CAST(value AS STRING)")\
    .withColumn("precio", regexp_extract("value", r"(\d+,\d+)", 1))

query = transformed.writeStream \
    .format("console") \
    .start()

query.awaitTermination()
