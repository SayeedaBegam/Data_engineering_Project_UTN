import pandas as pd
import duckdb
import json
import os
from confluent_kafka import Producer, Consumer

# Kafka Configuration
KAFKA_BROKER = 'localhost:9092'
RAW_DATA_TOPIC = 'raw_data'  # Input topic from producer
CLEAN_DATA_TOPIC = 'clean_data1'  # Output topic after cleaning
CSV_FILE = "final_cleaned_data.csv"

# Kafka Producer Configuration
producer = Producer({'bootstrap.servers': KAFKA_BROKER})

# Kafka Consumer Configuration
consumer = Consumer({
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': 'data-cleaning-group',
    'auto.offset.reset': 'earliest'
})


def clean_and_process_message(message):
    """
    Cleans a single Kafka message using DuckDB and writes it to Kafka and CSV.
    """
    try:
        # Convert JSON message to DataFrame
        message_value = json.loads(message.value().decode('utf-8'))
        df = pd.DataFrame([message_value])  # Convert to Pandas DataFrame

        # Convert "NULL" strings to actual NaN values
        df.replace("NULL", None, inplace=True)

        # Convert numeric fields correctly
        numeric_columns = [
            "instance_id", "cluster_size", "user_id", "database_id", "query_id",
            "compile_duration_ms", "queue_duration_ms", "execution_duration_ms",
            "num_permanent_tables_accessed", "num_external_tables_accessed",
            "num_system_tables_accessed", "mbytes_scanned", "mbytes_spilled",
            "num_joins", "num_scans", "num_aggregations"
        ]

        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # Use DuckDB for cleaning
        con = duckdb.connect(database=':memory:')
        con.register('df', df)

        cleaned_df = con.execute("""
            SELECT 
                COALESCE(CAST(instance_id AS BIGINT), -1) AS instance_id,
                COALESCE(CAST(cluster_size AS BIGINT), 0) AS cluster_size,
                COALESCE(CAST(user_id AS BIGINT), -1) AS user_id,
                COALESCE(CAST(database_id AS BIGINT), -1) AS database_id,
                COALESCE(CAST(query_id AS BIGINT), -1) AS query_id,
                
                COALESCE(feature_fingerprint, 'UNKNOWN') AS feature_fingerprint,
                COALESCE(cache_source_query_id, 'UNKNOWN') AS cache_source_query_id,

                LOWER(TRIM(query_type)) AS query_type,

                COALESCE(NULLIF(arrival_timestamp, ''), '1970-01-01')::TIMESTAMP AS arrival_timestamp,

                COALESCE(CAST(compile_duration_ms AS BIGINT), 0) AS compile_duration_ms,
                COALESCE(CAST(queue_duration_ms AS BIGINT), 0) AS queue_duration_ms,
                COALESCE(CAST(execution_duration_ms AS BIGINT), 0) AS execution_duration_ms,
                COALESCE(CAST(num_permanent_tables_accessed AS BIGINT), 0) AS num_permanent_tables_accessed,
                COALESCE(CAST(num_external_tables_accessed AS BIGINT), 0) AS num_external_tables_accessed,
                COALESCE(CAST(num_system_tables_accessed AS BIGINT), 0) AS num_system_tables_accessed,
                COALESCE(CAST(mbytes_scanned AS BIGINT), 0) AS mbytes_scanned,
                COALESCE(CAST(mbytes_spilled AS BIGINT), 0) AS mbytes_spilled,
                COALESCE(CAST(num_joins AS BIGINT), 0) AS num_joins,
                COALESCE(CAST(num_scans AS BIGINT), 0) AS num_scans,
                COALESCE(CAST(num_aggregations AS BIGINT), 0) AS num_aggregations,

                COALESCE(was_aborted, FALSE) AS was_aborted,
                COALESCE(was_cached, FALSE) AS was_cached,

                COALESCE(read_table_ids, 'UNKNOWN') AS read_table_ids,
                COALESCE(write_table_ids, 'UNKNOWN') AS write_table_ids
            FROM df
        """).fetchdf()

        # Append to CSV
        file_exists = os.path.isfile(CSV_FILE)
        cleaned_df.to_csv(CSV_FILE, mode='a', header=not file_exists, index=False)

        # Send cleaned data to Kafka
        json_payload = cleaned_df.to_json(orient='records')
        producer.produce(CLEAN_DATA_TOPIC, value=json_payload)
        producer.flush()

        print(f"\n✅ Cleaned Message Sent to Kafka: {json_payload}")
        print(f"📂 Cleaned data appended to '{CSV_FILE}'")

    except Exception as e:
        print(f"❌ Error processing message: {e}")


def consume_and_process():
    """
    Continuously consumes data from Kafka, cleans it, and pushes back to Kafka.
    """
    consumer.subscribe([RAW_DATA_TOPIC])
    print("\n🔄 Waiting for Kafka messages...")

    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue  # Keep waiting for messages

        if msg.error():
            print(f"❌ Kafka Error: {msg.error()}")
            continue

        print(f"\n📥 Received message: {msg.value().decode('utf-8')}")
        clean_and_process_message(msg)


if __name__ == "__main__":
    consume_and_process()
