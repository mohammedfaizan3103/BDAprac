from pyspark import SparkContext

sc = SparkContext(appName="LogAnalysisExample")

logs = sc.textFile("file:///home/hduser/spark-26/log_batch_analysis/server_log.txt")
errors = logs.filter(lambda x: "ERROR" in x)

error_counts = errors.map(lambda x: (x,1)).reduceByKey(lambda a,b:a+b)

top_errors = error_counts.take(10)

print("\nMost Frequent Errors:\n")

for error,count in top_errors:
    print(error,count)

sc.stop()
