import streamlit as st
from kafka import KafkaConsumer
import json
import time
from collections import deque

# Kafka settings
KAFKA_BROKER = 'localhost:9092'  # Kafka broker address
TOPIC_NAME = 'parquet-stream'    # Kafka topic name
MAX_MESSAGES = 10  # Maximum number of messages to keep in memory

# Streamlit Dashboard
def main():
    st.title("Real-time Compile Duration Dashboard")
    st.write("Listening to Kafka topic:", TOPIC_NAME)

    # Create a placeholder for updating the list dynamically
    list_placeholder = st.empty()

    # Kafka Consumer settings
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

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

            list_placeholder.write(list(message_queue))

            time.sleep(1)

    except KeyboardInterrupt:
        st.write("Stopping Streamlit Dashboard...")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
