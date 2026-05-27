
from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF, StringIndexer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline

spark = SparkSession.builder.appName("SparkSentimentAnalysisDemo").getOrCreate()

data = spark.read.csv("reviews.csv", header=True, inferSchema=True)
data = data.repartition(16)

label_indexer = StringIndexer(inputCol="sentiment", outputCol="label")
tokenizer = Tokenizer(inputCol="text", outputCol="words")
remover = StopWordsRemover(inputCol="words", outputCol="filtered")
hashingTF = HashingTF(inputCol="filtered", outputCol="rawFeatures", numFeatures=1000)
idf = IDF(inputCol="rawFeatures", outputCol="features")
lr = LogisticRegression(maxIter=20)

pipeline = Pipeline(stages=[label_indexer, tokenizer, remover, hashingTF, idf, lr])

model = pipeline.fit(data)
predictions = model.transform(data)

predictions.select("text","prediction","probability").show(20, truncate=False)
input("Press Enter to stop Spark...")
spark.stop()
