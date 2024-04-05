import time
import json
from kafka import KafkaProducer


def json_serializer(data):
    return json.dumps(data).encode('utf-8')


server = 'localhost:9092'

producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=json_serializer
)

producer.bootstrap_connected()


t0 = time.time()

topic_name = 'test-topic'

for i in range(3):
    message = {'number': i+1}
    producer.send(topic_name, value=message)
    print(f"Sent: {message}")
    time.sleep(0.05)

producer.flush()
t3 = time.time()
print(f'took {(t3 - t0):.2f} seconds')