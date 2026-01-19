# This is the code that will go to Kafka Topic, fetch the data and display on our terminal

from kafka import KafkaConsumer
import json # used to deserialize the data

# --- Configuration matching the Producer ---


consumer = KafkaConsumer(
    'stock_analysis',       # Topic where the consumer will extract data
    bootstrap_servers=['localhost:9094'], # The host of the Kafka Cluster to connect to
    auto_offset_reset='earliest', # instruct the consumer to pick the earliest data that is being stored
    enable_auto_commit=True, # means some of the actions of the consumer will be stored automatically
    group_id='my-consumer-group', # Defines a consumer group. Allows this consumer to be stored in that group
    value_deserializer=lambda x: json.loads(x.decode('utf-8')) # where Kafka consumer deserializes the message stored in Topic
)

print("Starting Kafka consumer. Waiting for messages on topic 'stock_analysis'....")

# After setting up the consumer, we will read the message from the consumer setup

for message in consumer:

    data = message.value

    # Print the received data

    print(f" Value (Deserialized): {data}")

consumer.close()
print("Kafka consumer closed.")