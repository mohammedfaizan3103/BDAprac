import time
from pyspark import SparkContext

sc = SparkContext(appName="WordCountExample")

# Read text file
text = sc.textFile("file:///home/hduser/spark-26/spark_wordcount/input.txt")

# Word count logic
counts = (text
          .flatMap(lambda line: line.split(" "))
          .map(lambda word: (word, 1))
          .reduceByKey(lambda a, b: a + b))

# Print result
for word, count in counts.collect():
    print(word, count)
time.sleep(180)
sc.stop()



