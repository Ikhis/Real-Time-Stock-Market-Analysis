from extract import connect_to_api, extract_json
from producer_setup import init_producer, topic
import time
# First import the relevant functions from the extract.py module

def main(): # entry point of our python application
    response = connect_to_api() # stores the result of the connect_to_api function

    data = extract_json(response) # response is then passed as an argument to the extract_json function

    producer = init_producer() # calls the producer function and saves the content inside producer variable

   
    for stock in data:
        result = {
            'data': stock['date'],
            'symbol': stock['symbol'],
            'open': stock['open'],
            'low': stock['low'],
            'high': stock['high'],
            'close': stock['close']
        }

        producer.send(topic, result) # sends the result dictionary to Kafka topic
        print(f'Data sent to {topic} topic')

        time.sleep(2) # delays the latency of data moving into kafka

    producer.flush() # makes sure all data move successfully into Kafka cluster
    producer.close() # closes any connection open when data was being moved to Kafka



    return None


if __name__ == '__main__': # main function call
    main()