import streamlit as st
from confluent_kafka import Consumer, KafkaError
import json
import time

# Kafka settings
KAFKA_BROKER = 'localhost:9092'  # Kafka broker address
TOPIC_RAW_DATA = 'parquet-stream'  # Kafka topic name
TOPIC_COMPILE_DURATION = 'compile_duration_sort'  # Kafka topic name for sorted durations
TOPIC_QUERY_COUNTER = 'query_counter'  # Kafka topic name for query counters

def create_consumer(topic, group_id):
    """Create a Confluent Kafka Consumer."""
    return Consumer({
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': group_id,
        'auto.offset.reset': 'earliest',  # Start reading from the beginning
        'enable.auto.commit': False       # Automatically commit offsets
    })

# Streamlit app layout
st.title('Kafka Data Consumer Viewer')

# Create Kafka consumers for each topic
consumer_raw_data = create_consumer(TOPIC_RAW_DATA, 'raw_data')
consumer_compile_duration = create_consumer(TOPIC_COMPILE_DURATION, 'live_analytics')
consumer_query_counter = create_consumer(TOPIC_QUERY_COUNTER, 'live_analytics')

# Subscribe consumers to their respective topics
consumer_raw_data.subscribe([TOPIC_RAW_DATA])
consumer_compile_duration.subscribe([TOPIC_COMPILE_DURATION])
consumer_query_counter.subscribe([TOPIC_QUERY_COUNTER])

# Create placeholders to display data
raw_data_placeholder = st.empty()
compile_duration_placeholder = st.empty()
query_counter_placeholder = st.empty()

# Connection status messages
try:
    st.success("Kafka connection successful!")
    while True:
        # Poll messages from consumers
        raw_msg = consumer_raw_data.poll(timeout=1.0)
        compile_msg = consumer_compile_duration.poll(timeout=1.0)
        query_msg = consumer_query_counter.poll(timeout=1.0)

        # Handle messages from the raw data topic
        if raw_msg is not None and not raw_msg.error():
            message_value = json.loads(raw_msg.value().decode('utf-8'))
            raw_data_placeholder.write(f"Received from {TOPIC_RAW_DATA}:")
            raw_data_placeholder.dataframe([message_value])  # Display as DataFrame

        elif raw_msg and raw_msg.error():
            if raw_msg.error().code() == KafkaError._PARTITION_EOF:
                st.warning(f"End of partition reached for {TOPIC_RAW_DATA}")
            else:
                st.error(f"Error in consuming from {TOPIC_RAW_DATA}: {raw_msg.error()}")

        # Handle messages from the compile duration topic
        if compile_msg is not None and not compile_msg.error():
            message_value = json.loads(compile_msg.value().decode('utf-8'))
            compile_duration_placeholder.write(f"Received from {TOPIC_COMPILE_DURATION}:")
            compile_duration_placeholder.dataframe([message_value])  # Display as DataFrame

        elif compile_msg and compile_msg.error():
            if compile_msg.error().code() == KafkaError._PARTITION_EOF:
                st.warning(f"End of partition reached for {TOPIC_COMPILE_DURATION}")
            else:
                st.error(f"Error in consuming from {TOPIC_COMPILE_DURATION}: {compile_msg.error()}")

        # Handle messages from the query counter topic
        if query_msg is not None and not query_msg.error():
            message_value = json.loads(query_msg.value().decode('utf-8'))
            query_counter_placeholder.write(f"Received from {TOPIC_QUERY_COUNTER}:")
            query_counter_placeholder.dataframe([message_value])  # Display as DataFrame

        elif query_msg and query_msg.error():
            if query_msg.error().code() == KafkaError._PARTITION_EOF:
                st.warning(f"End of partition reached for {TOPIC_QUERY_COUNTER}")
            else:
                st.error(f"Error in consuming from {TOPIC_QUERY_COUNTER}: {query_msg.error()}")

        time.sleep(3)

except KeyboardInterrupt:
    st.warning("Streamlit app interrupted. Stopping consumers...")
except Exception as e:
    st.error(f"An error occurred: {str(e)}")
finally:
    # Close consumers
    consumer_raw_data.close()
    consumer_compile_duration.close()
    consumer_query_counter.close()
