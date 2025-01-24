import pandas as pd
from kafka import KafkaProducer
import json
import pyarrow.parquet as pq

# Kafka configuration
KAFKA_BROKER = 'localhost:9092'  # Kafka broker address
TOPIC_NAME = 'parquet-stream'    # Kafka topic name


def send_to_kafka(producer, topic, chunk):
    """Send data to Kafka"""
    for record in chunk.to_dict(orient='records'):
        # Convert Timestamp to ISO 8601 string format
        record = {k: v.isoformat() if isinstance(v, pd.Timestamp) else v for k, v in record.items()}
        producer.send(topic, value=record)
    producer.flush()


def stream_parquet_to_kafka(parquet_file, batch_size):
    """
    Stream the specified Parquet file to Kafka in batches

    Args:
        parquet_file (str): Path to the Parquet file
        batch_size (int): Batch size for each Kafka send operation
    """
    # Configure Kafka producer
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serialize to JSON format
    )

    print(f"Streaming Parquet file '{parquet_file}' to Kafka topic '{TOPIC_NAME}' with batch size {batch_size}...")
    try:
        # Read the Parquet file in batches
        with pq.ParquetFile(parquet_file) as parquet_file_obj:
            for i, batch in enumerate(parquet_file_obj.iter_batches(batch_size=batch_size)):
                chunk = batch.to_pandas()  # Convert to Pandas DataFrame
                send_to_kafka(producer, TOPIC_NAME, chunk)
                print(f"Batch {i} sent to Kafka successfully.")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        producer.close()
    print("Finished streaming data to Kafka.")


def main():
    # Sample input
    parquet_file = 'sample_0.001.parquet'  # Parquet file name (or CSV?)
    batch_size = 5  # Batch size

    stream_parquet_to_kafka(parquet_file, batch_size)


if __name__ == '__main__':
    main()
