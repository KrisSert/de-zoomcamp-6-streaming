from kafka import KafkaConsumer

# Create a Kafka consumer without specifying a consumer group ID
consumer = KafkaConsumer(
    'green-trips',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest'
)

# Start consuming messages
for message in consumer:
    print(message.value.decode('utf-8'))

# Close the consumer when done
consumer.close()