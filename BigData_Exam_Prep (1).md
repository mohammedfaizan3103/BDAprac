# Big Data Exam Preparation Guide
### Complete Study Material with Programs, Commands & Run Steps

---

## HOW TO USE THIS GUIDE
- Every program is **simple and direct** — gives output, no fluff
- **Run Steps** are given after every program
- When dataset changes, only change the **filename/column names** — logic stays same
- All programs run on **Linux terminal**, no internet needed
- **Local mode is used wherever possible** — no HDFS needed unless specified

---

## THE UNIVERSAL APPROACH FOR ANY QUESTION

### Step 1: Identify what type of program it is
- Counting something (words, records) → flatMap + reduceByKey / GROUP BY + COUNT
- Finding max/min/avg → GROUP BY + MAX/MIN/AVG
- Filtering data → FILTER / WHERE condition
- Machine learning → Pipeline pattern
- Streaming → readStream + window pattern
- Graph → vertices + edges + Graph() pattern

### Step 2: Identify the columns
Professor gives format like `name, age, salary, department` — just replace column names in your template.

### Step 3: Identify the output needed
- "Find count of each word" → reduceByKey / GROUP BY COUNT
- "Find maximum temperature" → MAX()
- "Find top 10" → ORDER BY + LIMIT 10
- "Filter only errors" → filter / WHERE with condition

### Example: New dataset given
```
product_id, product_name, category, price, quantity_sold
```
Question: **"Find total revenue per category"**
```python
df.createOrReplaceTempView("sales")
spark.sql("""
    SELECT category, SUM(price * quantity_sold) as revenue
    FROM sales
    GROUP BY category
    ORDER BY revenue DESC
""").show()
```
Same pattern every time — only column names change.

---

# TOPIC 1: HDFS COMMANDS

> HDFS commands always need Hadoop running. Use `start-dfs.sh` and `start-yarn.sh` first.

```bash
# Start Hadoop
start-dfs.sh
start-yarn.sh

# Check running services
jps
```
Expected output of jps:
```
12345 NameNode
12456 DataNode
12567 SecondaryNameNode
12678 ResourceManager
12789 NodeManager
13000 Jps
```

---

**1. Create a single directory**
```bash
hadoop fs -mkdir /myfolder
```
```
(no output = success)
```

**2. Create nested directories**
```bash
hadoop fs -mkdir -p /user/hduser/data/input
```
```
(no output = success)
```

**3. List root directory**
```bash
hadoop fs -ls /
```
```
Found 3 items
drwxr-xr-x   - hduser supergroup  0 2024-01-01 10:00 /myfolder
drwxr-xr-x   - hduser supergroup  0 2024-01-01 10:01 /user
drwxr-xr-x   - hduser supergroup  0 2024-01-01 10:02 /tmp
```

**4. List a specific folder**
```bash
hadoop fs -ls /user/hduser/data
```
```
Found 2 items
drwxr-xr-x   - hduser supergroup  0 2024-01-01 10:05 /user/hduser/data/input
drwxr-xr-x   - hduser supergroup  0 2024-01-01 10:06 /user/hduser/data/output
```

**5. List recursively (all nested files)**
```bash
hadoop fs -ls -R /user/hduser
```
```
drwxr-xr-x   - hduser supergroup  0 2024-01-01 10:05 /user/hduser/data
-rw-r--r--   1 hduser supergroup  512 2024-01-01 10:06 /user/hduser/data/input/input.txt
```

**6. Upload file from local to HDFS (-put)**
```bash
hadoop fs -put input.txt /user/hduser/data/input/
```
```
(no output = success)
```

**7. Upload using -copyFromLocal (same as -put)**
```bash
hadoop fs -copyFromLocal input.txt /user/hduser/data/input/
```
```
(no output = success)
```

**8. Download file from HDFS to local (-get)**
```bash
hadoop fs -get /user/hduser/data/input/input.txt ./downloaded.txt
```
```
(no output = success, file appears in current local directory)
```

**9. Download using -copyToLocal (same as -get)**
```bash
hadoop fs -copyToLocal /user/hduser/data/input/input.txt ./
```
```
(no output = success)
```

**10. Print file content**
```bash
hadoop fs -cat /user/hduser/data/input/input.txt
```
```
apple banana apple cherry banana apple
hello world hello spark
```

**11. View last part of file**
```bash
hadoop fs -tail /user/hduser/data/input/input.txt
```
```
apple banana apple cherry banana apple
hello world hello spark
```

**12. Copy file within HDFS**
```bash
hadoop fs -cp /user/hduser/data/input/input.txt /myfolder/
```
```
(no output = success)
```

**13. Move file within HDFS**
```bash
hadoop fs -mv /myfolder/input.txt /user/hduser/backup/
```
```
(no output = success)
```

**14. Delete a single file**
```bash
hadoop fs -rm /myfolder/input.txt
```
```
Deleted /myfolder/input.txt
```

**15. Delete a folder and all contents**
```bash
hadoop fs -rm -r /myfolder
```
```
Deleted /myfolder
```

**16. Delete without going to trash**
```bash
hadoop fs -rm -r -skipTrash /user/hduser/old_output
```
```
Deleted /user/hduser/old_output
```

**17. Check file sizes in a folder**
```bash
hadoop fs -du /user/hduser/data
```
```
512    /user/hduser/data/input/input.txt
1024   /user/hduser/data/input/data2.txt
```

**18. Check total size of a folder (summary)**
```bash
hadoop fs -du -s /user/hduser/data
```
```
1536   /user/hduser/data
```

**19. Count files, directories, and bytes**
```bash
hadoop fs -count /user/hduser/data
```
```
           2            3               1536 /user/hduser/data
```
*(format: directories  files  bytes  path)*

**20. Check if a path exists**
```bash
hadoop fs -test -e /user/hduser/data/input.txt && echo "EXISTS" || echo "NOT FOUND"
```
```
EXISTS
```

**21. Change file permissions**
```bash
hadoop fs -chmod 755 /user/hduser/data/input.txt
```
```
(no output = success)
```

**22. Change owner of file**
```bash
hadoop fs -chown hduser:supergroup /user/hduser/data/input.txt
```
```
(no output = success)
```

**23. Merge multiple HDFS output files into one local file**
```bash
hadoop fs -getmerge /user/hduser/output/ ./final_result.txt
```
```
(all part-00000, part-00001 etc. merged into final_result.txt locally)
```

**24. Create an empty file in HDFS**
```bash
hadoop fs -touchz /user/hduser/data/empty.txt
```
```
(no output = success, empty file created)
```

**25. Display HDFS disk usage (human readable)**
```bash
hadoop fs -df -h /
```
```
Filesystem               Size     Used    Available  Use%
hdfs://localhost:9000    20 G     2.5 G   17.5 G     12%
```

**26. Check HDFS health**
```bash
hadoop fsck /
```
```
FSCK started by hduser
Status: HEALTHY
 Number of data-nodes:  1
 Total files:           8
 Total blocks:          8
 Missing blocks:        0
The filesystem under path '/' is HEALTHY
```

**27. Set replication factor for a file**
```bash
hadoop fs -setrep 2 /user/hduser/data/input.txt
```
```
Replication 2 set: /user/hduser/data/input.txt
```

**28. Full cluster report (nodes, storage)**
```bash
hdfs dfsadmin -report
```
```
Configured Capacity: 21474836480 (20 GB)
DFS Remaining: 15798870528 (14.71 GB)
DFS Used%: 15.97%
Live datanodes (1):
  Name: 127.0.0.1:9866
```

### Quick Memory Table

| Command | What it does |
|---|---|
| `-mkdir` | Create folder |
| `-mkdir -p` | Create nested folders |
| `-ls` | List files |
| `-ls -R` | List recursively |
| `-put` / `-copyFromLocal` | Upload to HDFS |
| `-get` / `-copyToLocal` | Download from HDFS |
| `-cat` | Print file content |
| `-tail` | Print last part of file |
| `-cp` | Copy within HDFS |
| `-mv` | Move within HDFS |
| `-rm` | Delete file |
| `-rm -r` | Delete folder |
| `-du` | File sizes |
| `-du -s` | Total folder size |
| `-count` | Count dirs/files/bytes |
| `-chmod` | Change permissions |
| `-chown` | Change owner |
| `-getmerge` | Merge output files to local |
| `-touchz` | Create empty file |
| `-df -h` | Disk usage human readable |
| `-setrep` | Set replication factor |
| `fsck /` | Health check |
| `dfsadmin -report` | Full cluster report |

> **Key rule:** If output folder already exists before running MapReduce or Pig, always delete it first: `hadoop fs -rm -r /output/path`

---

# TOPIC 2: MAPREDUCE

## 2A: Word Count — Test Locally First, Then HDFS

### mapper.py
```python
#!/usr/bin/env python3
import sys

for line in sys.stdin:
    line = line.strip()
    words = line.split()
    for word in words:
        print(word + "\t" + "1")
```

### reducer.py
```python
#!/usr/bin/env python3
import sys

current_word = None
current_count = 0

for line in sys.stdin:
    line = line.strip()
    word, count = line.split("\t", 1)
    count = int(count)

    if current_word == word:
        current_count += count
    else:
        if current_word:
            print(current_word + "\t" + str(current_count))
        current_word = word
        current_count = count

if current_word:
    print(current_word + "\t" + str(current_count))
```

### Steps to Run Locally (no Hadoop needed):
```bash
# Create input
echo "hello world hello spark world" > input.txt

# Test mapper alone
cat input.txt | python3 mapper.py
```
```
hello   1
world   1
hello   1
spark   1
world   1
```
```bash
# Test full pipeline (sort simulates Hadoop shuffle)
cat input.txt | python3 mapper.py | sort | python3 reducer.py
```
```
hello   2
spark   1
world   2
```

### Steps to Run on Hadoop:
```bash
hadoop fs -mkdir -p /wordcount/input
hadoop fs -put input.txt /wordcount/input/
hadoop fs -rm -r /wordcount/output

hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-*.jar \
  -file mapper.py -mapper "python3 mapper.py" \
  -file reducer.py -reducer "python3 reducer.py" \
  -input /wordcount/input/ \
  -output /wordcount/output

hadoop fs -cat /wordcount/output/part-00000
```

---

## 2B: Weather/Temperature Analysis (MapReduce)

### mapper.py
```python
#!/usr/bin/env python3
import sys
import re

for line in sys.stdin:
    val = line.strip()
    year = val[15:19]
    temp = val[87:92]
    quality = val[92:93]
    if temp != "+9999" and re.match("[01459]", quality):
        print(year + "\t" + temp)
```

### reducer.py
```python
#!/usr/bin/env python3
import sys

last_key = None
max_val = -99999

for line in sys.stdin:
    key, val = line.strip().split("\t")
    val = int(val)
    if last_key and last_key != key:
        print(last_key + "\t" + str(max_val))
        last_key = key
        max_val = val
    else:
        last_key = key
        max_val = max(max_val, val)

if last_key:
    print(last_key + "\t" + str(max_val))
```

### Steps to Run Locally:
```bash
# Create sample data
echo "0067011990999991950051507004...9999999N9+02221+99999999999" > ncdc.txt
echo "0067011990999991951051507004...9999999N9+03111+99999999999" >> ncdc.txt

# Test locally
cat ncdc.txt | python3 mapper.py | sort | python3 reducer.py
```
```
1950    +02221
1951    +03111
```

### Steps to Run on Hadoop (Multi-node):
```bash
hadoop fs -mkdir -p /ncdc/input
hadoop fs -put ncdc.txt /ncdc/input/
hadoop fs -rm -r /ncdc/output

hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-*.jar \
  -file mapper.py -mapper "python3 mapper.py" \
  -file reducer.py -reducer "python3 reducer.py" \
  -input /ncdc/input/ \
  -output /ncdc/output

hadoop fs -cat /ncdc/output/part-00000
```

### Memory Trick for NCDC positions:
- Year = characters **15 to 19**
- Temp = characters **87 to 92**
- Quality = character **92**
- Missing temp = **+9999**

---

# TOPIC 3: PIG

## Local vs HDFS Mode

```bash
pig -x local program.pig      # LOCAL — reads from Linux filesystem
pig -x mapreduce program.pig  # HDFS — reads from HDFS
```

In **local mode**, paths are Linux paths like `'input.txt'` or `'/home/hduser/input.txt'`
In **mapreduce mode**, paths are HDFS paths like `'/user/pig/input/input.txt'`

---

## 3A: Word Count — Local Mode

### wordcount.pig
```pig
-- Local mode: use local Linux path
lines = LOAD 'input.txt' AS (line:chararray);

words = FOREACH lines GENERATE FLATTEN(TOKENIZE(line)) AS word;

grouped = GROUP words BY word;

word_counts = FOREACH grouped GENERATE group AS word, COUNT(words) AS count;

-- Output to local folder
STORE word_counts INTO 'wordcount_output';
```

### Steps to Run:
```bash
# 1. Create input
echo "apple banana apple cherry banana apple" > input.txt

# 2. Run in LOCAL mode
pig -x local wordcount.pig

# 3. View output
cat wordcount_output/part-r-00000
```
```
apple   3
banana  2
cherry  1
```

---

## 3B: Max Temperature — Local Mode

### max_temp.pig
```pig
-- Local mode: local file path
raw_data = LOAD 'ncdc_data.txt' AS (line:chararray);

filtered_data = FOREACH raw_data GENERATE
    SUBSTRING(line, 15, 19) AS year,
    (int)SUBSTRING(line, 87, 92) AS temp;

clean_data = FILTER filtered_data BY temp != 9999;

grouped_year = GROUP clean_data BY year;

max_temp = FOREACH grouped_year GENERATE group AS year, MAX(clean_data.temp) AS max_val;

STORE max_temp INTO 'ncdc_output';
```

### Steps to Run:
```bash
# 1. Create sample data
echo "0067011990999991950051507004...9999999N9+02221+99999999999" > ncdc_data.txt
echo "0067011990999991950051512004...9999999N9+02501+99999999999" >> ncdc_data.txt
echo "0067011990999991951051507004...9999999N9+03111+99999999999" >> ncdc_data.txt

# 2. Remove old output if exists
rm -rf ncdc_output

# 3. Run locally
pig -x local max_temp.pig

# 4. View output
cat ncdc_output/part-r-00000
```
```
1950    2501
1951    3111
```

---

## 3C: Pig UDF — Local Mode

### is_good_quality.py
```python
@outputSchema("is_good:boolean")
def is_good(quality):
    if quality is None:
        return False
    valid_codes = [0, 1, 4, 5, 9]
    return quality in valid_codes
```

### filter_weather.pig
```pig
-- Register UDF (local file path)
REGISTER 'is_good_quality.py' USING jython AS myfuncs;

-- Load local file
records = LOAD 'weather_data.txt' AS (year:chararray, temp:int, quality:int);

-- Filter using UDF
filtered_records = FILTER records BY myfuncs.is_good(quality);

DUMP filtered_records;
```

### Steps to Run:
```bash
# Both .pig and .py must be in the same folder
pig -x local filter_weather.pig
```
```
(1901,−139,1)
(1901,−120,1)
(1902,22,1)
```

### Pig Quick Reference

| Operation | Syntax |
|-----------|--------|
| Load file | `LOAD 'path' AS (col:type)` |
| Filter rows | `FILTER rel BY condition` |
| Transform | `FOREACH rel GENERATE expr` |
| Group | `GROUP rel BY field` |
| Count | `COUNT(bag)` |
| Max | `MAX(bag.field)` |
| Order | `ORDER rel BY field ASC/DESC` |
| Print | `DUMP rel` |
| Save | `STORE rel INTO 'path'` |
| Tokenize | `FLATTEN(TOKENIZE(line))` |
| Substring | `SUBSTRING(field, start, end)` |

---

# TOPIC 4: HIVE

> Hive always needs Hadoop/Metastore running. There is no pure local mode for Hive like Pig.

## 4A: Start Hive
```bash
# Start Hadoop first
start-dfs.sh
start-yarn.sh

# Start Hive
hive
```

## 4B: Basic HiveQL
```sql
-- Create database
CREATE DATABASE mydb;
USE mydb;

-- Create table from CSV
CREATE TABLE employees (
    id INT,
    name STRING,
    salary FLOAT,
    department STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- Load local CSV into Hive table
LOAD DATA LOCAL INPATH '/home/hduser/employees.csv' INTO TABLE employees;

-- Basic queries
SELECT * FROM employees;
SELECT name, salary FROM employees WHERE salary > 50000;
SELECT department, COUNT(*) AS emp_count FROM employees GROUP BY department;
SELECT department, AVG(salary) AS avg_sal FROM employees GROUP BY department;
SELECT * FROM employees ORDER BY salary DESC;
```

---

## 4C: Partitioning in Hive
```sql
-- Create partitioned table
CREATE TABLE sales (
    product STRING,
    amount FLOAT,
    customer STRING
)
PARTITIONED BY (year INT, month INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- Enable dynamic partition
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

-- Insert with partition
INSERT INTO TABLE sales PARTITION (year=2024, month=1)
SELECT product, amount, customer FROM raw_sales
WHERE year=2024 AND month=1;

-- Query with partition filter (fast — skips other partitions)
SELECT * FROM sales WHERE year=2024 AND month=1;

-- Show all partitions
SHOW PARTITIONS sales;
```

---

## 4D: Bucketing in Hive
```sql
SET hive.enforce.bucketing = true;

CREATE TABLE bucketed_users (
    id INT,
    name STRING,
    age INT
)
CLUSTERED BY (id) INTO 4 BUCKETS
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

INSERT INTO bucketed_users SELECT * FROM users;

SELECT * FROM bucketed_users WHERE id = 101;
```

### Partitioning vs Bucketing

| Feature | Partitioning | Bucketing |
|---------|-------------|-----------|
| Splits data by | Column value | Hash of column |
| Good for | Filtering (WHERE clause) | Joins, sampling |
| Creates | Separate folders per value | Fixed number of files |
| Example column | date, country, category | user_id, order_id |

---

## 4E: Hive UDF (know concept)
```sql
ADD JAR /path/to/myudf.jar;
CREATE TEMPORARY FUNCTION my_upper AS 'com.example.UpperUDF';
SELECT my_upper(name) FROM employees;
```

---

# TOPIC 5: SPARK CORE (RDDs) — Local Mode

## 5A: Word Count — Local Mode

### wordcount.py
```python
from pyspark import SparkContext

# local[*] = use all CPU cores on this machine, no cluster needed
sc = SparkContext("local[*]", "WordCount")

# Read local file
text = sc.textFile("input.txt")

counts = (text
    .flatMap(lambda line: line.split(" "))
    .map(lambda word: (word, 1))
    .reduceByKey(lambda a, b: a + b))

for word, count in counts.collect():
    print(word, count)

sc.stop()
```

### Steps to Run:
```bash
echo "spark is fast spark is powerful" > input.txt
spark-submit wordcount.py
```
```
spark 2
is 2
fast 1
powerful 1
```

---

## 5B: RDD Transformations — Local Mode

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("RDDDemo").getOrCreate()
sc = spark.sparkContext

rdd = sc.parallelize([1, 2, 3, 4, 5])

# ---- NARROW TRANSFORMATIONS (no shuffle) ----

# map: transform each element
squared = rdd.map(lambda x: x * x)
print("Squared:", squared.collect())           # [1, 4, 9, 16, 25]

# filter: keep matching elements
evens = rdd.filter(lambda x: x % 2 == 0)
print("Evens:", evens.collect())               # [2, 4]

# flatMap: split and flatten
sentences = sc.parallelize(["Hello World", "Spark is great"])
words = sentences.flatMap(lambda line: line.split())
print("Words:", words.collect())               # ['Hello', 'World', 'Spark', 'is', 'great']

# mapPartitions: operate per partition
rdd2 = sc.parallelize([1, 2, 3, 4, 5], 2)
result = rdd2.mapPartitions(lambda part: [sum(part)])
print("Partition sums:", result.collect())     # [3, 12]

# mapPartitionsWithIndex
result2 = rdd2.mapPartitionsWithIndex(lambda idx, part: [(idx, sum(part))])
print("With index:", result2.collect())        # [(0,3), (1,12)]

# ---- WIDE TRANSFORMATIONS (cause shuffle) ----

pairs = sc.parallelize([("a", 1), ("b", 2), ("a", 3)])

# reduceByKey
reduced = pairs.reduceByKey(lambda x, y: x + y)
print("ReduceByKey:", reduced.collect())       # [('a', 4), ('b', 2)]

# groupByKey
grouped = pairs.groupByKey().mapValues(list)
print("GroupByKey:", grouped.collect())        # [('a',[1,3]), ('b',[2])]

# sortByKey
sorted_rdd = pairs.sortByKey()
print("Sorted:", sorted_rdd.collect())         # [('a',1),('a',3),('b',2)]

# join
rdd1 = sc.parallelize([("cat", 1), ("dog", 2)])
rdd2 = sc.parallelize([("cat", "white"), ("dog", "black")])
joined = rdd1.join(rdd2)
print("Joined:", joined.collect())             # [('cat',(1,'white')),('dog',(2,'black'))]

# ---- ACTIONS ----
rdd = sc.parallelize([1, 2, 3, 4, 5])
print("collect:", rdd.collect())               # [1, 2, 3, 4, 5]
print("count:", rdd.count())                   # 5
print("sum:", rdd.sum())                       # 15
print("first:", rdd.first())                   # 1
print("take(3):", rdd.take(3))                 # [1, 2, 3]
print("reduce:", rdd.reduce(lambda a, b: a + b))  # 15

sc.stop()
```

### Steps to Run:
```bash
spark-submit rdd_demo.py
```

---

# TOPIC 6: SPARK SQL — Local Mode

## 6A: Spark SQL Basics

### spark_sql.py
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("SparkSQL").getOrCreate()

# Read local CSV
df = spark.read.csv("employees.csv", header=True, inferSchema=True)

df.show()
df.printSchema()

# Register as temp view for SQL queries
df.createOrReplaceTempView("employees")

spark.sql("SELECT * FROM employees").show()
spark.sql("SELECT name, salary FROM employees WHERE salary > 50000").show()
spark.sql("SELECT department, COUNT(*) as count FROM employees GROUP BY department").show()
spark.sql("SELECT department, AVG(salary) as avg_salary FROM employees GROUP BY department").show()

# DataFrame API (same results as SQL above)
df.select("name", "salary").where(df.salary > 50000).show()
df.groupBy("department").count().show()
df.orderBy("salary", ascending=False).show()

spark.stop()
```

### Steps to Run:
```bash
spark-submit spark_sql.py
```

---

## 6B: E-commerce Analysis

### ecommerce_analysis.py
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, count

spark = SparkSession.builder.master("local[*]").appName("Ecommerce").getOrCreate()

# Adapt columns to whatever the professor gives
df = spark.read.csv("orders.csv", header=True, inferSchema=True)
df.createOrReplaceTempView("orders")

# Total revenue per product
spark.sql("""
    SELECT product, SUM(amount) as total_revenue
    FROM orders
    GROUP BY product
    ORDER BY total_revenue DESC
""").show()

# Top customers
spark.sql("""
    SELECT customer_id, COUNT(*) as order_count, SUM(amount) as total_spent
    FROM orders
    GROUP BY customer_id
    ORDER BY total_spent DESC
    LIMIT 10
""").show()

spark.stop()
```

---

# TOPIC 7: SPARK MINI PROJECTS — Local Mode

## 7A: Fraud Detection

### fraud_detection.py
```python
from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml import Pipeline

spark = SparkSession.builder.master("local[*]").appName("FraudDetection").getOrCreate()

# Load local CSV
data = spark.read.csv("transactions.csv", header=True, inferSchema=True)

# Encode string columns to numbers
location_index = StringIndexer(inputCol="location", outputCol="locationIndex")
merchant_index = StringIndexer(inputCol="merchant", outputCol="merchantIndex")
device_index = StringIndexer(inputCol="device", outputCol="deviceIndex")

# Combine all feature columns into one vector
assembler = VectorAssembler(
    inputCols=["amount", "locationIndex", "merchantIndex", "deviceIndex"],
    outputCol="features"
)

# Random Forest classifier
rf = RandomForestClassifier(labelCol="fraud", featuresCol="features")

# Pipeline: chain all steps
pipeline = Pipeline(stages=[location_index, merchant_index, device_index, assembler, rf])
model = pipeline.fit(data)

predictions = model.transform(data)
predictions.select("amount", "location", "fraud", "prediction").show()

spark.stop()
```

### Steps to Run:
```bash
spark-submit fraud_detection.py
```

---

## 7B: Movie Recommendation

### movie_recommendation.py
```python
from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator

spark = SparkSession.builder.master("local[*]").appName("MovieRecommendation").getOrCreate()

# Load local CSV (columns: userId, movieId, rating)
ratings = spark.read.csv("ratings.csv", header=True, inferSchema=True)
ratings = ratings.select("userId", "movieId", "rating")

(training, test) = ratings.randomSplit([0.8, 0.2])

als = ALS(userCol="userId", itemCol="movieId", ratingCol="rating",
          coldStartStrategy="drop", nonnegative=True)
model = als.fit(training)

predictions = model.transform(test)
evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating",
                                 predictionCol="prediction")
rmse = evaluator.evaluate(predictions)
print("RMSE =", rmse)

model.recommendForAllUsers(5).show(5, False)

spark.stop()
```

### Steps to Run:
```bash
spark-submit movie_recommendation.py
```

---

## 7C: News Clustering

### news_clustering.py
```python
from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF
from pyspark.ml.clustering import KMeans
from pyspark.ml import Pipeline

spark = SparkSession.builder.master("local[*]").appName("NewsClustering").getOrCreate()

# Load local CSV (column: text)
data = spark.read.csv("news.csv", header=True, inferSchema=True)

tokenizer = Tokenizer(inputCol="text", outputCol="words")
remover = StopWordsRemover(inputCol="words", outputCol="filtered")
hashingTF = HashingTF(inputCol="filtered", outputCol="rawFeatures", numFeatures=100)
idf = IDF(inputCol="rawFeatures", outputCol="features")
kmeans = KMeans(k=3, seed=1)

pipeline = Pipeline(stages=[tokenizer, remover, hashingTF, idf, kmeans])
model = pipeline.fit(data)
predictions = model.transform(data)

predictions.select("text", "prediction").show(truncate=False)

spark.stop()
```

---

## 7D: Sentiment Analysis

### sentiment_analysis.py
```python
from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF, StringIndexer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline

spark = SparkSession.builder.master("local[*]").appName("SentimentAnalysis").getOrCreate()

# Load local CSV (columns: text, sentiment)
data = spark.read.csv("reviews.csv", header=True, inferSchema=True)

label_indexer = StringIndexer(inputCol="sentiment", outputCol="label")
tokenizer = Tokenizer(inputCol="text", outputCol="words")
remover = StopWordsRemover(inputCol="words", outputCol="filtered")
hashingTF = HashingTF(inputCol="filtered", outputCol="rawFeatures", numFeatures=1000)
idf = IDF(inputCol="rawFeatures", outputCol="features")
lr = LogisticRegression(maxIter=20)

pipeline = Pipeline(stages=[label_indexer, tokenizer, remover, hashingTF, idf, lr])
model = pipeline.fit(data)
predictions = model.transform(data)

predictions.select("text", "prediction", "probability").show(20, truncate=False)

spark.stop()
```

---

## 7E: Log Batch Analysis

### log_analysis.py
```python
from pyspark import SparkContext

# local[*] = local mode
sc = SparkContext("local[*]", "LogAnalysis")

# Read local log file
logs = sc.textFile("server_log.txt")

errors = logs.filter(lambda x: "ERROR" in x)
error_counts = errors.map(lambda x: (x, 1)).reduceByKey(lambda a, b: a + b)
top_errors = error_counts.take(10)

print("\nTop Errors:\n")
for error, count in top_errors:
    print(error, count)

sc.stop()
```

### Steps to Run all Mini Projects:
```bash
# All projects run the same way — local mode, no cluster needed
spark-submit fraud_detection.py
spark-submit movie_recommendation.py
spark-submit news_clustering.py
spark-submit sentiment_analysis.py
spark-submit log_analysis.py
```

---

# TOPIC 8: SPARK STRUCTURED STREAMING — Local Mode

## Key Concept
- **Tumbling Window** = fixed, non-overlapping windows (each record in exactly one window)
- **Sliding Window** = overlapping windows (a record can appear in multiple windows)
- Console output shows **Batch: 0, Batch: 1, Batch: 2...** as new files arrive

## Setup: Create Stream Folder
```bash
mkdir retail_stream
```

---

## 8A: Tumbling Window

### tumbling_window.py
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, sum, window
from pyspark.sql.types import *

# local[*] = local mode streaming
spark = SparkSession.builder.master("local[*]").appName("TumblingWindow").getOrCreate()
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

# Read from local streaming folder
df = spark.readStream.option("header", True).schema(schema).csv("retail_stream")

df = df.withColumn("eventTime", to_timestamp(col("InvoiceDate")))
df = df.withColumn("TotalPrice", col("Quantity") * col("UnitPrice"))

# Tumbling window: 1 day window, no overlap
result = df.withWatermark("eventTime", "1 day") \
    .groupBy(window(col("eventTime"), "1 day"), col("Country")) \
    .agg(sum("TotalPrice").alias("Revenue"))

# Console output shows Batch: 0, Batch: 1, etc.
query = result.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
```

---

## 8B: Sliding Window

### sliding_window.py
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, sum, window
from pyspark.sql.types import *

spark = SparkSession.builder.master("local[*]").appName("SlidingWindow").getOrCreate()
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

# Sliding window: 10 min window slides every 5 min (records appear in multiple windows)
result = df.withWatermark("eventTime", "1 day") \
    .groupBy(window(col("eventTime"), "10 minutes", "5 minutes"), col("Country")) \
    .agg(sum("TotalPrice").alias("Revenue"))

query = result.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
```

### Steps to Run Streaming:
```bash
# Terminal 1: Create folder and start Spark
mkdir retail_stream
spark-submit tumbling_window.py

# Terminal 2: Copy files one by one to simulate stream
for file in retail_30_days/*.csv
do
    cp "$file" retail_stream/
    sleep 2
done
```

### What You Will See in Console:
```
-------------------------------------------
Batch: 0
-------------------------------------------
+------------------------------------------+-------+--------+
|window                                    |Country|Revenue |
+------------------------------------------+-------+--------+
|{2010-12-01 00:00:00, 2010-12-02 00:00:00}|UK     |1234.50 |
+------------------------------------------+-------+--------+

-------------------------------------------
Batch: 1
-------------------------------------------
|{2010-12-01 00:00:00, 2010-12-02 00:00:00}|Germany|890.00  |
```

### Tumbling vs Sliding Summary

| Feature | Tumbling | Sliding |
|---------|----------|---------|
| Window overlap | No | Yes |
| `window()` call | `window(col, "1 day")` | `window(col, "10 minutes", "5 minutes")` |
| Record appears in | 1 window | Multiple windows |
| Use case | Daily totals | Moving averages |

---

# TOPIC 9: KAFKA + SPARK STREAMING

## 9A: Start Kafka (always same steps)
```bash
# Terminal 1: ZooKeeper (start first, always)
cd /home/hduser/kafka/kafka_2.13-3.6.1
bin/zookeeper-server-start.sh config/zookeeper.properties

# Terminal 2: Kafka Broker
bin/kafka-server-start.sh config/server.properties

# Terminal 3: Create topic
bin/kafka-topics.sh --create \
    --topic my_topic \
    --bootstrap-server 127.0.0.1:9092 \
    --partitions 1 \
    --replication-factor 1

# List topics (verify)
bin/kafka-topics.sh --list --bootstrap-server 127.0.0.1:9092

# Quick test — manual producer
bin/kafka-console-producer.sh --topic my_topic --bootstrap-server 127.0.0.1:9092

# Quick test — manual consumer
bin/kafka-console-consumer.sh --topic my_topic --bootstrap-server 127.0.0.1:9092 --from-beginning
```

---

## 9B: Cricket Run Rate (Kafka + Spark — Local Mode)

### cricket_producer.py
```python
from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers='127.0.0.1:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

over = 1
ball = 1

while True:
    data = {"over": over, "ball": ball, "runs": random.choice([0, 1, 2, 3, 4, 6])}
    print("Ball:", data)
    producer.send("cricket_runrate", value=data)
    producer.flush()

    ball += 1
    if ball > 6:
        ball = 1
        over += 1
    time.sleep(1)
```

### cricket_spark.py
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum as _sum, count
from pyspark.sql.types import StructType, IntegerType

# local[*] = local mode, no cluster needed
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

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "127.0.0.1:9092") \
    .option("subscribe", "cricket_runrate") \
    .load()

# Parse JSON from Kafka value
json_df = df.selectExpr("CAST(value AS STRING)")
parsed = json_df.select(from_json(col("value"), schema).alias("data"))
final = parsed.select("data.*")

# Calculate run rate
stats = final.agg(_sum("runs").alias("total_runs"), count("runs").alias("balls"))
result = stats.withColumn("overs", col("balls") / 6) \
              .withColumn("run_rate", col("total_runs") / (col("balls") / 6))

query = result.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
```

### Steps to Run Cricket Project:
```bash
# Install kafka-python if not installed
pip install kafka-python

# Terminal 1: ZooKeeper
cd /home/hduser/kafka/kafka_2.13-3.6.1
bin/zookeeper-server-start.sh config/zookeeper.properties

# Terminal 2: Kafka
bin/kafka-server-start.sh config/server.properties

# Terminal 3: Create topic
bin/kafka-topics.sh --create --topic cricket_runrate \
    --bootstrap-server 127.0.0.1:9092 --partitions 1 --replication-factor 1

# Terminal 4: Run Spark (local mode)
spark-submit \
    --master local[*] \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 \
    cricket_spark.py

# Terminal 5: Run producer
python3 cricket_producer.py
```

### Expected Output in Spark Terminal:
```
-------------------------------------------
Batch: 0
-------------------------------------------
+----------+-----+------------------+------------------+
|total_runs|balls|overs             |run_rate          |
+----------+-----+------------------+------------------+
|9         |6    |1.0               |9.0               |
+----------+-----+------------------+------------------+

-------------------------------------------
Batch: 1
-------------------------------------------
|15        |12   |2.0               |7.5               |
```

---

# TOPIC 10: SPARK GRAPHX (Scala — spark-shell)

> GraphX only works in Scala. Run in spark-shell (which is local by default).

## Start:
```bash
spark-shell
# This starts in local mode automatically
```

---

## 10A: Connected Components
```scala
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

val vertices = sc.parallelize(Seq(
  (1L, "A"), (2L, "B"), (3L, "C"),
  (4L, "D"), (5L, "E"), (6L, "F")
))

val edges = sc.parallelize(Seq(
  Edge(1L, 2L, 1),
  Edge(2L, 3L, 1),
  Edge(4L, 5L, 1)
))

val graph = Graph(vertices, edges)

val cc = graph.connectedComponents().vertices
cc.collect.foreach(println)
```
```
(1,1)
(2,1)
(3,1)
(4,4)
(5,4)
(6,6)
```
Node 6 is isolated (its own component). Components: {A,B,C}, {D,E}, {F}

---

## 10B: Multiple Connected Components
```scala
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

val vertices: RDD[(VertexId, String)] = sc.parallelize(Seq(
  (1L, "A"), (2L, "B"), (3L, "C"), (4L, "D"),
  (5L, "E"), (6L, "F"), (7L, "G"),
  (8L, "H"), (9L, "I"), (10L, "J")
))

val edges: RDD[Edge[Int]] = sc.parallelize(Seq(
  Edge(1L, 2L, 1), Edge(2L, 3L, 1), Edge(3L, 4L, 1),   // Component 1: A-B-C-D
  Edge(5L, 6L, 1), Edge(6L, 7L, 1),                     // Component 2: E-F-G
  Edge(8L, 9L, 1)                                         // Component 3: H-I
  // Node 10 (J) is isolated
))

val graph = Graph(vertices, edges)
val cc = graph.connectedComponents().vertices
val result = vertices.join(cc)

result.collect.foreach {
  case (id, (name, comp)) =>
    println(s"Node: $name -> Component: $comp")
}
```
```
Node: A -> Component: 1
Node: B -> Component: 1
Node: C -> Component: 1
Node: D -> Component: 1
Node: E -> Component: 5
Node: F -> Component: 5
Node: G -> Component: 5
Node: H -> Component: 8
Node: I -> Component: 8
Node: J -> Component: 10
```

---

## 10C: Triangle Count
```scala
import org.apache.spark.graphx._

val edges = sc.parallelize(Seq(
  Edge(1L, 2L, 1),
  Edge(2L, 3L, 1),
  Edge(3L, 1L, 1),   // closes triangle: 1-2-3
  Edge(3L, 4L, 1)    // node 4 has no triangle
))

val graph = Graph.fromEdges(edges, 1)

val triangles = graph.triangleCount().vertices
triangles.collect.foreach {
  case (id, count) => println(s"Node $id -> Triangles: $count")
}
```
```
Node 1 -> Triangles: 1
Node 2 -> Triangles: 1
Node 3 -> Triangles: 1
Node 4 -> Triangles: 0
```

---

## 10D: PageRank — Airport Dataset
```scala
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

val vertices: RDD[(VertexId, String)] = sc.parallelize(Seq(
  (1L, "DEL"), (2L, "MUM"), (3L, "BLR"),
  (4L, "HYD"), (5L, "CHN"), (6L, "KOL")
))

val edges: RDD[Edge[Int]] = sc.parallelize(Seq(
  Edge(1L, 2L, 1), Edge(1L, 3L, 1),
  Edge(2L, 3L, 1), Edge(3L, 1L, 1),
  Edge(4L, 3L, 1), Edge(5L, 3L, 1),
  Edge(6L, 3L, 1), Edge(2L, 4L, 1),
  Edge(4L, 5L, 1), Edge(5L, 2L, 1)
))

val graph = Graph(vertices, edges)

val ranks = graph.pageRank(0.0001).vertices
val result = vertices.join(ranks)

result.collect.foreach {
  case (id, (airport, rank)) =>
    println(s"Airport: $airport  Rank: $rank")
}
```
```
Airport: BLR  Rank: 2.18   <- highest, most flights land here
Airport: MUM  Rank: 1.42
Airport: DEL  Rank: 1.15
Airport: HYD  Rank: 0.89
Airport: CHN  Rank: 0.85
Airport: KOL  Rank: 0.40
```

---

# TOPIC 11: OPTIMIZATION TECHNIQUES

## 11A: Spark Partitioning — Local Mode
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("Partitioning").getOrCreate()
sc = spark.sparkContext

rdd = sc.textFile("data.txt")
print("Default partitions:", rdd.getNumPartitions())

# repartition = full shuffle, use to increase partitions
rdd2 = rdd.repartition(8)
print("After repartition:", rdd2.getNumPartitions())

# coalesce = no full shuffle, use to reduce partitions (faster)
rdd3 = rdd.coalesce(2)
print("After coalesce:", rdd3.getNumPartitions())

# DataFrame partitioning
df = spark.read.csv("data.csv", header=True)
df.repartition(4).write.csv("output/")
```

## 11B: Hive Partitioning & Bucketing
```sql
-- PARTITIONING: separate folders per value — fast for WHERE filters
CREATE TABLE logs (
    user_id INT,
    action STRING
)
PARTITIONED BY (log_date STRING)
STORED AS ORC;

-- BUCKETING: hash into fixed files — fast for JOINs and sampling
CREATE TABLE users_bucketed (
    user_id INT,
    name STRING
)
CLUSTERED BY (user_id) INTO 8 BUCKETS
STORED AS ORC;

-- ORC = compressed columnar format, always faster than TEXTFILE
```

## 11C: Spark SQL Optimization
```python
# Cache a DataFrame you use multiple times
df.cache()

# Broadcast join: small table sent to all workers, avoids shuffle
from pyspark.sql.functions import broadcast
result = large_df.join(broadcast(small_df), "key")

# Filter early — reduce data before grouping
df.filter(df.country == "India").groupBy("city").count().show()

# coalesce to reduce output files without full shuffle
df.coalesce(1).write.csv("output/")
```

---

# QUICK REFERENCE: WHEN DATASET CHANGES

## Universal Template — Spark
```python
# Step 1: Load the new file
df = spark.read.csv("NEW_FILE.csv", header=True, inferSchema=True)

# Step 2: Always check schema first
df.printSchema()
df.show(5)

# Step 3: Replace column names in your logic
# Example: dataset has "price" instead of "amount"
df.groupBy("category").agg(sum("price").alias("total")).show()
```

## Universal Template — Pig (Local)
```pig
-- Step 1: Change only the filename and column names
data = LOAD 'new_file.txt' AS (col1:chararray, col2:int, col3:float);
-- All logic (FILTER, GROUP, FOREACH, STORE) stays exactly the same
```

## Universal Template — MapReduce
```python
# Mapper: just emit (key, value) based on new columns
# If dataset is CSV with header:
# year,temperature,quality  →  emit year as key, temperature as value

# Reducer: aggregation logic stays same (max, sum, count)
```

---

# EXAM DAY CHEAT SHEET

## Pattern Memory

| Topic | Pattern |
|-------|---------|
| MapReduce | mapper emits `key\tvalue` → sort → reducer aggregates |
| Pig | LOAD → FILTER → FOREACH → GROUP → FOREACH(agg) → STORE/DUMP |
| Hive | CREATE TABLE → LOAD DATA → SELECT with GROUP BY |
| Spark Core | parallelize → transform (map/filter/flatMap) → action (collect/count) |
| Spark ML | StringIndexer → VectorAssembler → Model → Pipeline.fit() → transform() |
| Streaming | readStream → withWatermark → groupBy(window) → writeStream(console) |
| Kafka+Spark | readStream(kafka) → from_json → select → writeStream(console) |
| GraphX | parallelize vertices & edges → Graph() → cc/pageRank/triangleCount → collect |

## Local Mode Quick Reference

| Tool | Local Mode Command |
|------|-------------------|
| MapReduce test | `cat input.txt \| python3 mapper.py \| sort \| python3 reducer.py` |
| Pig | `pig -x local program.pig` |
| Spark | `spark-submit program.py` (uses local[*] in code) |
| GraphX | `spark-shell` (local by default) |
| Hive | Always needs Hadoop — `hive` |
| Kafka | Always needs ZooKeeper + Kafka broker |

## Common Errors & Fixes

| Error | Fix |
|-------|-----|
| Output folder exists (Pig/MapReduce) | `rm -rf output_folder` or `hadoop fs -rm -r /hdfs/output` |
| Cannot run multiple SparkContexts | Use `SparkSession.builder.getOrCreate()` not `SparkContext()` directly |
| Kafka not connecting | Start ZooKeeper FIRST, then Kafka broker |
| py4j / JavaPackage error | Use `spark-submit` instead of `python3` |
| No module named kafka | `pip install kafka-python` |
| Pig UDF not found | Keep `.py` and `.pig` files in the same folder |
| Spark streaming no output | Copy files into stream folder AFTER starting spark-submit |

---

*Remember: dataset changes, but the pattern stays the same. Identify the type → adapt column names → run.*
