from kafka import KafkaProducer
import json


topic = "stock_analysis" # topic is like a table inside Kafka where records are stored


def init_producer():
    producer = KafkaProducer(
        bootstrap_servers = 'localhost:9094', # where we call the connection to kafka server
        value_serializer = lambda v: json.dumps(v).encode('utf-8')
    )

    return producer