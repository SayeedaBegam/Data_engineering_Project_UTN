import pandas as pd
from confluent_kafka import Producer, Consumer
from confluent_kafka.admin import AdminClient, NewTopic
import json
import pyarrow.parquet as pq
import time
from collections import deque
from ksql import KSQLAPI
import requests
import ddb_wrappers as ddb
import duckdb
DUCKDB_FILE = "data.duckdb"


# Start zookeper server: bin/zookeeper-server-start etc/kafka/zookeeper.properties
# Start Kafka server: bin/kafka-server-start etc/kafka/server.properties
# Start ksql db: bin/ksql-server-start etc/ksqldb/ksql-server.properties
# 
# # Kafka configuration
KAFKA_BROKER = 'localhost:9092'  # Kafka broker address
TOPIC_RAW_DATA = 'parquet_stream'  # Kafka topic name
TOPIC_CLEAN_DATA = 'clean_data'
TOPIC_QUERY_METRICS = 'query_metrics'  # Kafka topic name for sorted durations
TOPIC_COMPILE_METRICS = 'compile_metrics'  # Kafka topic name for sorted durations
TOPIC_LEADERBOARD= 'leaderboard'
TOPIC_STRESS_INDEX = 'stressindex'

LEADERBOARD_COLUMNS = ['instance_id','query_id','user_id','arrival_timestamp','compile_duration_ms']
QUERY_COLUMNS = ['instance_id','was_aborted','was_cached','query_type']
COMPILE_COLUMNS = ['instance_id','num_joins','num_scans','num_aggregations','mbytes_scanned', 'mbytes_spilled']
STRESS_COLUMNS = ['instance_id','was_aborted','arrival_timestamp',
                  'compile_duration_ms','execution_duration_ms',
                  'queue_duration_ms', 'mbytes_scanned','mbytes_spilled']

QUERY_METRIC_COLUMNS = [
'query_type',
'num_permanent_tables_accessed',
'num_external_tables_accessed',
'num_system_tables_accessed',
'read_table_ids',
'write_table_ids',
'mbytes_scanned',
'mbytes_spilled',
'num_joins',
'num_scans',
'num_aggregations']

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
    #producer2 = Producer(producer_config)
    #producer3 = Producer(producer_config)

    print(f"Streaming Parquet file '{parquet_file}' to Kafka topic '{TOPIC_RAW_DATA}' with batch size {batch_size}...")
    df = pd.read_parquet(parquet_file)
    df = df.sort_values(by='arrival_timestamp').reset_index(drop=True)
    df['arrival_timestamp'] = pd.to_datetime(df['arrival_timestamp'])
    df['batch_id'] = (df.index // batch_size)  # BatchIDs used to group data
    type_cast_batch(df)
    for batch_id, batch in df.groupby('batch_id'):
        try:
            send_to_kafka(producer, TOPIC_RAW_DATA, batch)
            print(f"Batch {batch_id} sent to Kafka successfully.")
            #ddb.write_to_topic(batch,TOPIC_LEADERBOARD,producer,LEADERBOARD_COLUMNS)
            ddb.write_to_topic(batch,TOPIC_QUERY_METRICS,producer,QUERY_COLUMNS)
            #ddb.write_to_topic(batch,TOPIC_COMPILE_METRICS,producer,COMPILE_COLUMNS)
            #ddb.write_to_topic(batch,TOPIC_STRESS_INDEX,producer,STRESS_COLUMNS)

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

def type_cast_batch(batch):
    """
    Type casts a Pandas DataFrame according to the predefined schema.
    
    :param batch: Pandas DataFrame containing the required columns.
    :return: Type-casted DataFrame
    """
    # Define the expected data types
    dtype_mapping = {
        "instance_id": "Int64",
        "cluster_size": "float64",
        "user_id": "Int64",
        "database_id": "Int64",
        "query_id": "Int64",
        "arrival_timestamp": "datetime64[ns]",
        "compile_duration_ms": "float64",
        "queue_duration_ms": "Int64",
        "execution_duration_ms": "Int64",
        "feature_fingerprint": "string",
        "was_aborted": "boolean",
        "was_cached": "boolean",
        "cache_source_query_id": "float64",
        "query_type": "string",
        "num_permanent_tables_accessed": "float64",
        "num_external_tables_accessed": "float64",
        "num_system_tables_accessed": "float64",
        "read_table_ids": "string",
        "write_table_ids": "float64",
        "mbytes_scanned": "float64",
        "mbytes_spilled": "float64",
        "num_joins": "Int64",
        "num_scans": "Int64",
        "num_aggregations": "Int64",
        "batch_id": "Int64",
    }

    # Convert data types
    for column, dtype in dtype_mapping.items():
        if dtype == "datetime64[ns]":
            batch[column] = pd.to_datetime(batch[column], errors='coerce')  # Handle invalid timestamps
        else:
            batch[column] = batch[column].astype(dtype)


def main():
    parquet_file = 'sample_0.001.parquet'  # Parquet file name
    batch_size = 2  # Batch size

    stream_parquet_to_kafka(parquet_file, batch_size)

if __name__ == '__main__':
    main()
