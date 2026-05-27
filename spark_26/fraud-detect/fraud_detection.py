from pyspark.sql import SparkSession
import time
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml import Pipeline

spark = SparkSession.builder \
    .appName("FraudDetection") \
    .getOrCreate()

data = spark.read.csv("transactions.csv", header=True, inferSchema=True)

location_index = StringIndexer(inputCol="location", outputCol="locationIndex")
merchant_index = StringIndexer(inputCol="merchant", outputCol="merchantIndex")
device_index = StringIndexer(inputCol="device", outputCol="deviceIndex")

assembler = VectorAssembler(
    inputCols=["amount","locationIndex","merchantIndex","deviceIndex"],
    outputCol="features"
)

rf = RandomForestClassifier(labelCol="fraud", featuresCol="features")

pipeline = Pipeline(stages=[location_index, merchant_index, device_index, assembler, rf])

model = pipeline.fit(data)

predictions = model.transform(data)

predictions.select("amount","location","fraud","prediction").show()
time.sleep(180)


spark.stop()
