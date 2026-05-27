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
    data = {
        "over": over,
        "ball": ball,
        "runs": random.choice([0,1,2,3,4,6])
    }

    print("Ball:", data)
    producer.send("cricket_runrate", value=data)
    producer.flush()

    ball += 1
    if ball > 6:
        ball = 1
        over += 1

    time.sleep(1)
