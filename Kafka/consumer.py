from confluent_kafka import Consumer, Producer
import json

# Kafka settings
KAFKA_BROKER = 'localhost:9092'  # Kafka broker address
TOPIC_RAW_DATA = 'parquet-stream'  # Kafka topic name
TOPIC_COMPILE_DURATION = 'compile_duration_sort'  # Kafka topic name for sorted durations
TOPIC_QUERY_COUNTER = 'query_counter'  # Kafka topic name for query counters
MAX_MESSAGES = 10  # Maximum number of messages to keep in memory

def create_consumer(topic, group_id):
    """Create a Confluent Kafka Consumer."""
    return Consumer({
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': group_id,
        'auto.offset.reset': 'earliest',  # Start reading from the beginning
        'enable.auto.commit': True       # Automatically commit offsets
    }, logger=None)

def main():
    # Kafka Consumer settings
    consumer_raw_data = create_consumer(TOPIC_RAW_DATA, 'raw_data')
    consumer_compile_duration = create_consumer(TOPIC_COMPILE_DURATION, 'live_analytics')
    consumer_query_counter = create_consumer(TOPIC_QUERY_COUNTER, 'live_analytics')

    # Subscribe consumers to their respective topics
    consumer_raw_data.subscribe([TOPIC_RAW_DATA])
    consumer_compile_duration.subscribe([TOPIC_COMPILE_DURATION])
    consumer_query_counter.subscribe([TOPIC_QUERY_COUNTER])

    # Kafka Producer settings
    producer = Producer({
        'bootstrap.servers': KAFKA_BROKER
    })

    print(f"Listening for messages on topic '{TOPIC_RAW_DATA}'...")

    try:
        while True:
            # Poll messages from consumers
            raw_msg = consumer_raw_data.poll(timeout=1.0)
            compile_msg = consumer_compile_duration.poll(timeout=1.0)
            query_msg = consumer_query_counter.poll(timeout=1.0)

            # Handle messages from the raw data topic
            if raw_msg is not None and not raw_msg.error():
                message_value = json.loads(raw_msg.value().decode('utf-8'))
                print(f"Received from {TOPIC_RAW_DATA}: {message_value}")

            # Handle messages from the compile duration topic
            if compile_msg is not None and not compile_msg.error():
                message_value = json.loads(compile_msg.value().decode('utf-8'))
                print(f"Received from {TOPIC_COMPILE_DURATION}: {message_value}")

            # Handle messages from the query counter topic
            if query_msg is not None and not query_msg.error():
                message_value = json.loads(query_msg.value().decode('utf-8'))
                print(f"Received from {TOPIC_QUERY_COUNTER}: {message_value}")

            # Simulate sending a message (if needed)
            # Example: Sending processed data back to Kafka
            example_data = {"example_key": "example_value"}
            producer.produce(TOPIC_RAW_DATA, value=json.dumps(example_data))
            producer.flush()
            
    except KeyboardInterrupt:
        print("\nStopping consumer...")
    finally:
        # Close consumers and producer
        consumer_raw_data.close()
        consumer_compile_duration.close()
        consumer_query_counter.close()
        producer.flush()

if __name__ == '__main__':
    main()
