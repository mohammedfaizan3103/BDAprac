from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, sum, window
from pyspark.sql.types import *

spark = SparkSession.builder.appName("SlidingWindow").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

schema = StructType([
    StructField("InvoiceNo", StringType()),
    StructField("StockCode", StringType()),
    StructField("Description", StringType()),
    StructField("Quantity", IntegerType()),
    StructField("InvoiceDate", StringType()),
    StructField("UnitPrice", DoubleType()),
    StructField("CustomerID", StringType()),
    StructField("Country", StringType())
])

df = spark.readStream.option("header", True).schema(schema).csv("retail_stream")

df = df.withColumn("eventTime", to_timestamp(col("InvoiceDate")))
df = df.withColumn("TotalPrice", col("Quantity") * col("UnitPrice"))

result = df.withWatermark("eventTime", "1 day") \
    .groupBy(window(col("eventTime"), "10 minutes", "5 minutes"), col("Country")) \
    .agg(sum("TotalPrice").alias("Revenue"))

query = result.writeStream.outputMode("append").format("console").start()
query.awaitTermination()
