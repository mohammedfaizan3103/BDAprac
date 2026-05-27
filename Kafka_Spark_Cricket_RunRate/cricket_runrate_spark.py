from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum as _sum, count
from pyspark.sql.types import StructType, IntegerType

spark = SparkSession.builder \
    .appName("CricketRunRate") \
    .master("local[*]") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.driver.host", "127.0.0.1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

schema = StructType() \
    .add("over", IntegerType()) \
    .add("ball", IntegerType()) \
    .add("runs", IntegerType())

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "127.0.0.1:9092") \
    .option("subscribe", "cricket_runrate") \
    .load()

json_df = df.selectExpr("CAST(value AS STRING)")
parsed = json_df.select(from_json(col("value"), schema).alias("data"))
final = parsed.select("data.*")

stats = final.agg(
    _sum("runs").alias("total_runs"),
    count("runs").alias("balls")
)

result = stats.withColumn("overs", col("balls")/6) \
              .withColumn("run_rate", col("total_runs")/(col("balls")/6))

query = result.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
