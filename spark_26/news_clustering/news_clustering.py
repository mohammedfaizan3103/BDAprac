from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF
from pyspark.ml.clustering import KMeans
from pyspark.ml import Pipeline
import time

spark = SparkSession.builder \
    .appName("NewsClustering") \
    .getOrCreate()

data = spark.read.csv("news.csv", header=True, inferSchema=True)

tokenizer = Tokenizer(inputCol="text", outputCol="words")

remover = StopWordsRemover(inputCol="words", outputCol="filtered")

hashingTF = HashingTF(inputCol="filtered", outputCol="rawFeatures", numFeatures=100)

idf = IDF(inputCol="rawFeatures", outputCol="features")

kmeans = KMeans(k=3, seed=1)

pipeline = Pipeline(stages=[tokenizer, remover, hashingTF, idf, kmeans])

model = pipeline.fit(data)

predictions = model.transform(data)

predictions.select("text","prediction").show(truncate=False)

print("Keeping Spark UI active for 3 minutes to inspect DAG...")
time.sleep(180)

spark.stop()
