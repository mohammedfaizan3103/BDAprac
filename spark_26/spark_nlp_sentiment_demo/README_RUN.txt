


3. Run Spark job:

/usr/local/spark/bin/spark-submit \
--master spark://172.16.4.183:7077 \
sentiment_analysis.py

4. Open Spark UI:
http://172.16.4.183:4040

Then go to:
Jobs → DAG Visualization

Dataset size ≈ 60,000 rows so Spark job runs long enough to observe DAG stages.
