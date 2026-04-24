"""
Kafka Producer Pipeline for Stock Data.

This script:
    - Fetches stock data from an external API
    - Transforms the raw JSON into structured records
    - Sends each record to a Kafka topic using a producer

Pipeline flow:
    API -> Extraction -> Transformation -> Kafka Producer -> Topic

Modules Used:
    - extract.py: Handles API connection and data extraction
    - producer_setup.py: Initializes Kafka producer and topic config
"""

from extract import connect_to_api, extract_json
from producer_setup import init_producer, topic
import time


def main():
    """
    Entry point for the Kafka producer pipeline.

    This function orchestrates the full data flow:
        1. Fetch stock data from API
        2. Extract and structure relevant fields
        3. Initialize Kafka producer
        4. Send each record to Kafka topic with a delay
        5. Flush and close producer to ensure delivery

    Returns:
        None

    Notes:
        - Introduces a delay (2 seconds) between messages to simulate streaming.
        - Uses print for basic feedback; logging could be used for production.
        - Assumes Kafka broker and topic are already configured and available.
    """
    response = connect_to_api()  # stores the result of the connect_to_api function

    data = extract_json(response)  # response is then passed as an argument to the extract_json function

    producer = init_producer()  # calls the producer function and saves the content inside producer variable

    for stock in data:
        result = {
            'date': stock['date'],
            'symbol': stock['symbol'],
            'open': stock['open'],
            'low': stock['low'],
            'high': stock['high'],
            'close': stock['close']
        }

        producer.send(topic, result)  # sends the result dictionary to Kafka topic
        print(f'Data sent to {topic} topic')

        time.sleep(2)  # delays the latency of data moving into kafka

    producer.flush()  # makes sure all data move successfully into Kafka cluster
    producer.close()  # closes any connection open when data was being moved to Kafka

    return None


if __name__ == '__main__':
    """
    Script execution guard.

    Ensures that the main() function runs only when this file is executed
    directly, and not when it is imported as a module.
    """
    main()