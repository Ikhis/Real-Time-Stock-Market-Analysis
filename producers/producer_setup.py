"""
Kafka Producer Configuration Module.

This module defines:
    - Kafka topic name
    - Producer initialization function

Used to create a reusable Kafka producer instance for sending messages.
"""

from kafka import KafkaProducer
import json


# Kafka topic where stock data will be published
topic = "stock_analysis"


def init_producer():
    """
    Initialize and return a Kafka producer instance.

    Configuration:
        - Connects to Kafka broker running on localhost:9094
        - Serializes messages to JSON and encodes as UTF-8

    Returns:
        KafkaProducer: Configured producer instance

    Notes:
        - Assumes Kafka broker is accessible at the given address.
        - Value serializer ensures Python dictionaries are converted
          into JSON format before sending.
    """
    producer = KafkaProducer(
        bootstrap_servers='localhost:9094',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    return producer