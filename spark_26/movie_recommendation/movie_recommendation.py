from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator

spark = SparkSession.builder \
    .appName("MovieRecommendation") \
    .getOrCreate()

# Load ratings data
ratings = spark.read.csv("ratings.csv", header=True, inferSchema=True)
ratings = ratings.select("userId","movieId","rating")

# Split data
(training, test) = ratings.randomSplit([0.8,0.2])

# ALS model
als = ALS(
    userCol="userId",
    itemCol="movieId",
    ratingCol="rating",
    coldStartStrategy="drop",
    nonnegative=True
)

model = als.fit(training)

# Predictions
predictions = model.transform(test)

# Evaluate model
evaluator = RegressionEvaluator(
    metricName="rmse",
    labelCol="rating",
    predictionCol="prediction"
)

rmse = evaluator.evaluate(predictions)

print("Root Mean Square Error =", rmse)

# Recommend movies for users
userRecs = model.recommendForAllUsers(5)

userRecs.show(5, False)

import time
time.sleep(120)

spark.stop()
