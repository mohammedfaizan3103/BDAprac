from pyspark import SparkContext

sc = SparkContext(appName="LogAnalysisExample")

# Force small partitions for small cluster
logs = sc.textFile("server_log.txt").repartition(4)

errors = logs.filter(lambda x: "ERROR" in x)

error_counts = errors.map(lambda x: (x,1)).reduceByKey(lambda a,b: a+b)

top_errors = error_counts.take(10)

print("\nTop Errors:\n")

for error,count in top_errors:
    print(error,count)

sc.stop()
