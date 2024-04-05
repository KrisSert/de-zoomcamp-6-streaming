import time
import json
from kafka import KafkaProducer
import pandas as pd
import uuid


# initialize the target server (kafka broker) and create producer
def json_serializer(data):
    return json.dumps(data).encode('utf-8')

server = 'localhost:9092'
producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=json_serializer
)
producer.bootstrap_connected()

t0 = time.time()


# read taxi data, send to topic
topic_name = 'green-trips'

file_path = 'green_tripdata_2019-10.csv.gz'
columns = [
    'lpep_pickup_datetime',
    'lpep_dropoff_datetime',
    'PULocationID',
    'DOLocationID',
    'passenger_count',
    'trip_distance',
    'tip_amount'
]

df_green = pd.read_csv(file_path, compression='gzip', usecols=columns)



for row in df_green.itertuples(index=False):
    row_dict = {col: getattr(row, col) for col in row._fields}
    message = {topic_name: row_dict}
    producer.send(topic_name, value=message)


producer.flush()
t3 = time.time()
print(f'took {(t3 - t0):.2f} seconds')
