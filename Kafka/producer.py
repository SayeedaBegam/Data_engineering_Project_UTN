import pandas as pd
from confluent_kafka import Producer, Consumer
import json
import pyarrow.parquet as pq
import time
from collections import deque

# Start zookeper server: bin/zookeeper-server-start.sh config/zookeeper.properties
# Start Kafka server: bin/kafka-server-start.sh config/server.properties
# Kafka configuration
KAFKA_BROKER = 'localhost:9092'  # Kafka broker address
TOPIC_RAW_DATA = 'parquet-stream'  # Kafka topic name
TOPIC_COMPILE_DURATION = 'compile_duration_sort'  # Kafka topic name for sorted durations
TOPIC_QUERY_COUNTER = 'query_counter'  # Kafka topic name for sorted durations
MAX_MESSAGES = 10  # Maximum number of messages to keep in memory

def send_to_kafka(producer, topic, chunk):
    """Send data to Kafka"""
    for record in chunk.to_dict(orient='records'):
        # Convert Timestamp to ISO 8601 string format
        record = {k: v.isoformat() if isinstance(v, pd.Timestamp) else v for k, v in record.items()}
        producer.produce(topic, key=None, value=json.dumps(record))
    producer.flush()

def stream_parquet_to_kafka(parquet_file, batch_size):
    """
    Stream the specified Parquet file to Kafka in batches

    Args:
        parquet_file (str): Path to the Parquet file
        batch_size (int): Batch size for each Kafka send operation
    """
    # Configure Kafka producer
    producer_config = {
        'bootstrap.servers': KAFKA_BROKER,
        'linger.ms': 10,
    }
    producer = Producer(producer_config)

    message_queue = deque()
    query_counter = {
        'total': 0,
        'aborted': 0,
        'cached': 0
    }

    print(f"Streaming Parquet file '{parquet_file}' to Kafka topic '{TOPIC_RAW_DATA}' with batch size {batch_size}...")
    df = pd.read_parquet(parquet_file)
    df = df.sort_values(by='arrival_timestamp').reset_index(drop=True)
    df['arrival_timestamp'] = pd.to_datetime(df['arrival_timestamp'])
    df['batch_id'] = (df.index // batch_size)  # BatchIDs used to group data
    for batch_id, batch in df.groupby('batch_id'):
        try:
            send_to_kafka(producer, TOPIC_RAW_DATA, batch)
            print(f"Batch {batch_id} sent to Kafka successfully.")
            analyze_compile_time(producer, batch, message_queue)
            analyze_query_counter(producer, batch, query_counter)
        except Exception as e:
            print(f"Error: {e}")
        print("Finished streaming data to Kafka.")
        # Adds delay based on time differences between batches
        if batch_id < df['batch_id'].max():
            # Use time difference between batches to simulate real time
            curr_batch_end = batch['arrival_timestamp'].iloc[-1]
            next_batch_start = df[df['batch_id'] == batch_id + 1]['arrival_timestamp'].iloc[0]
            delay_stream(curr_batch_end, next_batch_start)

    producer.flush()
    print("Batch over")

def delay_stream(batch_start, next_batch_start):
    scaling_factor = 6480 / 4  # Scaling factor to compress 3 months of data in 20 mins
    time_diff = (next_batch_start - batch_start).total_seconds()
    delay = time_diff / scaling_factor
    min_delay = 0.25
    time.sleep(max(delay, min_delay))

def analyze_compile_time(producer, batch, message_queue):
    try:
        compile_durations = batch['compile_duration_ms'].dropna().tolist()
        for duration in compile_durations:
            if duration is None or not isinstance(duration, (int, float)):
                continue

            # Insert the new compile_duration_ms value in sorted order (descending)
            inserted = False
            for i, value in enumerate(message_queue):
                if duration > value:
                    message_queue.insert(i, duration)
                    inserted = True
                    break

            if not inserted:
                message_queue.append(duration)
            # If queue exceeds MAX_MESSAGES, remove the smallest value
            if len(message_queue) > MAX_MESSAGES:
                message_queue.pop()

            # Produce the sorted values back to the topic
            producer.produce(TOPIC_COMPILE_DURATION, value=json.dumps({"compile_duration_ms": list(message_queue)}))
    except Exception as e:
        print(f"Error: {e}")

def analyze_query_counter(producer, batch, counter):
    try:
        aborted = 0
        cached = 0
        total = 0

        for _, row in batch.iterrows():
            total += 1
            if row['was_cached'] == 1:
                cached += 1
            if row['was_aborted'] == 1:
                aborted += 1

        counter['total'] += total
        counter['aborted'] += aborted
        counter['cached'] += cached
        producer.produce(TOPIC_QUERY_COUNTER, value=json.dumps(counter))
    except Exception as e:
        print(f"Error: {e}")

def main():
    parquet_file = 'sample_0.001.parquet'  # Parquet file name
    batch_size = 2  # Batch size

    stream_parquet_to_kafka(parquet_file, batch_size)

if __name__ == '__main__':
    main()