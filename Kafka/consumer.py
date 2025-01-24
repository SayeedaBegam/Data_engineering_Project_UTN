from kafka import KafkaConsumer, KafkaProducer
import json
from collections import deque

# Kafka settings
KAFKA_BROKER = 'localhost:9092'  # Address of the Kafka broker
TOPIC_NAME = 'parquet-stream'    # Kafka topic name
TOPIC_COMPILE_DURATION = 'compile_duration_sort' # topic for compile_duration
MAX_MESSAGES = 10  # Maximum number of messages to keep in memory

def main():
    # Kafka Consumer settings
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset='earliest',  # Read data from the beginning
        enable_auto_commit=True,       # Automatically commit offsets
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))  # Deserialize in JSON format
    )

    # Kafka Producer settings
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

    print(f"Listening for messages on topic '{TOPIC_NAME}'...")

    # Use a deque to store compile_duration_ms values
    message_queue = deque()

    try:
        for message in consumer:
            data = message.value
            compile_duration = data.get('compile_duration_ms')

            # Skip if compile_duration_ms is missing or not a number
            if compile_duration is None or not isinstance(compile_duration, (int, float)):
                print(f"Skipping invalid message: {data}")
                continue

            # Insert the new compile_duration_ms value in sorted order (descending)
            inserted = False
            for i, value in enumerate(message_queue):
                if compile_duration > value:
                    message_queue.insert(i, compile_duration)
                    inserted = True
                    break

            if not inserted:
                message_queue.append(compile_duration)

            # If queue exceeds MAX_MESSAGES, remove the smallest value
            if len(message_queue) > MAX_MESSAGES:
                removed_value = message_queue.pop()
                print(f"Queue exceeded max size. Removed smallest value: {removed_value}")

            # Print the updated queue
            print(f"Updated queue: {list(message_queue)}")

            # Produce the sorted values back to the topic
            for duration in message_queue:
                producer.send(TOPIC_COMPILE_DURATION, value={"compile_duration_ms": duration})

            producer.flush()

    except KeyboardInterrupt:
        print("\nStopping consumer...")
    finally:
        consumer.close()


if __name__ == '__main__':
    main()
