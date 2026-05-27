KAFKA + SPARK CRICKET RUN RATE PROJECT (FULL INSTRUCTIONS)

====================================
PREREQUISITES
====================================
- Kafka installed at: /home/hduser/kafka/kafka_2.13-3.6.1
- Spark installed at: /usr/local/spark
- Python 3 installed
- Internet required (first-time Spark package download)

====================================
STEP 1: START KAFKA
====================================
Open TWO terminals:

Terminal 1:
cd /home/hduser/kafka/kafka_2.13-3.6.1
bin/zookeeper-server-start.sh config/zookeeper.properties

Terminal 2:
cd /home/hduser/kafka/kafka_2.13-3.6.1
bin/kafka-server-start.sh config/server.properties

====================================
STEP 2: VERIFY KAFKA
====================================
Run:
bin/kafka-topics.sh --list --bootstrap-server 127.0.0.1:9092

(No error should appear)

====================================
STEP 3: CREATE TOPIC
====================================
bin/kafka-topics.sh --create \
--topic cricket_runrate \
--bootstrap-server 127.0.0.1:9092 \
--partitions 1 \
--replication-factor 1

====================================
STEP 4: INSTALL PYTHON LIBRARY
====================================
pip install kafka-python

====================================
STEP 5: RUN SPARK STREAMING
====================================
Navigate to project folder:

cd <your_project_folder>

Run:

spark-submit \
--master local[*] \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 \
cricket_runrate_spark.py

NOTE:
- DO NOT use start-all.sh
- Spark runs in LOCAL mode

====================================
STEP 6: RUN PRODUCER
====================================
Open new terminal:

python3 cricket_runrate_producer.py

====================================
STEP 7: OBSERVE OUTPUT
====================================
Spark terminal will show:

total_runs, balls, overs, run_rate

====================================
COMMON ERRORS & FIXES
====================================

1. Spark Bind Error:
Fix already included in code (127.0.0.1)

2. Topic not found:
Create topic again

3. Kafka not running:
Restart ZooKeeper + Kafka

4. Python module error:
pip install kafka-python

====================================
FINAL RESULT
====================================
Real-time cricket match analytics system using Kafka and Spark.
