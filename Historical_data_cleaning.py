import pandas as pd
import duckdb
import json
import os
from confluent_kafka import Consumer

# Kafka Configuration
KAFKA_BROKER = 'localhost:9092'
RAW_DATA_TOPIC = 'parquet_stream'  # Kafka topic
DUCKDB_FILE = "cleaned_data.duckdb22"  # DuckDB database file
CSV_FILE = '/Users/orsuvinay/Documents/python/new/final_cleaned_data.csv'

# Kafka Consumer Configuration
consumer = Consumer({
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': 'data-cleaning-group',
    'auto.offset.reset': 'earliest'
})

# Remove existing CSV file (if needed)
if os.path.exists(CSV_FILE):
    os.remove(CSV_FILE)


def clean_and_store_message(message):
    """
    Cleans a single Kafka message using DuckDB and writes it to DuckDB and CSV.
    """
    try:
        # Convert JSON message to DataFrame
        message_value = json.loads(message.value().decode('utf-8'))
        df = pd.DataFrame([message_value])  # Convert to Pandas DataFrame

        # Convert "NULL" strings and None to actual NaN values
        df.replace(["NULL", None, ""], pd.NA, inplace=True)

        # ✅ Convert arrival_timestamp to TIMESTAMP
        if "arrival_timestamp" in df.columns:
            df["arrival_timestamp"] = pd.to_datetime(df["arrival_timestamp"], errors="coerce")
            df["arrival_timestamp"].fillna(pd.Timestamp("1970-01-01 00:00:00"), inplace=True)

        # ✅ Convert numeric fields correctly
        numeric_columns = [
            "instance_id", "cluster_size", "user_id", "database_id", "query_id",
            "compile_duration_ms", "queue_duration_ms", "execution_duration_ms",
            "num_permanent_tables_accessed", "num_external_tables_accessed",
            "num_system_tables_accessed", "mbytes_scanned", "mbytes_spilled",
            "num_joins", "num_scans", "num_aggregations"
        ]
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)

        # Convert read_table_ids and write_table_ids to proper strings
        df["read_table_ids"] = df["read_table_ids"].astype(str).fillna("[]")
        df["write_table_ids"] = df["write_table_ids"].astype(str).fillna("[]")

        # ✅ Remove rows where both read_table_ids & write_table_ids are empty or NA
        df = df[~((df["read_table_ids"] == "[]") | (df["read_table_ids"] == "<NA>"))]
        df = df[~((df["write_table_ids"] == "[]") | (df["write_table_ids"] == "<NA>"))]

        # ✅ Convert boolean fields correctly
        boolean_columns = ["was_aborted", "was_cached"]
        for col in boolean_columns:
            if col in df.columns:
                df[col] = df[col].fillna(False).astype(bool)

        # ✅ Open DuckDB connection
        with duckdb.connect(DUCKDB_FILE) as con:
            con.execute("""
                CREATE TABLE IF NOT EXISTS cleaned_data (
                    instance_id BIGINT DEFAULT -1,
                    cluster_size BIGINT DEFAULT 0,
                    user_id BIGINT DEFAULT -1,
                    database_id BIGINT DEFAULT -1,
                    query_id BIGINT DEFAULT -1,
                    feature_fingerprint VARCHAR DEFAULT 'UNKNOWN',
                    cache_source_query_id VARCHAR DEFAULT 'UNKNOWN',
                    query_type VARCHAR DEFAULT 'UNKNOWN',
                    arrival_timestamp TIMESTAMP DEFAULT '1970-01-01 00:00:00',
                    compile_duration_ms BIGINT DEFAULT 0,
                    queue_duration_ms BIGINT DEFAULT 0,
                    execution_duration_ms BIGINT DEFAULT 0,
                    num_permanent_tables_accessed BIGINT DEFAULT 0,
                    num_external_tables_accessed BIGINT DEFAULT 0,
                    num_system_tables_accessed BIGINT DEFAULT 0,
                    mbytes_scanned BIGINT DEFAULT 0,
                    mbytes_spilled BIGINT DEFAULT 0,
                    num_joins BIGINT DEFAULT 0,
                    num_scans BIGINT DEFAULT 0,
                    num_aggregations BIGINT DEFAULT 0,
                    was_aborted BOOLEAN DEFAULT FALSE,
                    was_cached BOOLEAN DEFAULT FALSE,
                    read_table_ids VARCHAR DEFAULT '[]',
                    write_table_ids VARCHAR DEFAULT '[]'
                )
            """)

            # ✅ Fix: Explicitly CAST arrival_timestamp to TIMESTAMP
            con.execute("""
                INSERT INTO cleaned_data (
                    instance_id, cluster_size, user_id, database_id, query_id,
                    feature_fingerprint, cache_source_query_id, query_type,
                    arrival_timestamp, compile_duration_ms, queue_duration_ms,
                    execution_duration_ms, num_permanent_tables_accessed,
                    num_external_tables_accessed, num_system_tables_accessed,
                    mbytes_scanned, mbytes_spilled, num_joins, num_scans, num_aggregations,
                    was_aborted, was_cached, read_table_ids, write_table_ids
                ) 
                SELECT 
                    instance_id, cluster_size, user_id, database_id, query_id,
                    feature_fingerprint, cache_source_query_id, query_type,
                    CAST(arrival_timestamp AS TIMESTAMP),  -- ✅ Force conversion to TIMESTAMP
                    compile_duration_ms, queue_duration_ms, execution_duration_ms, 
                    num_permanent_tables_accessed, num_external_tables_accessed, 
                    num_system_tables_accessed, mbytes_scanned, mbytes_spilled, 
                    num_joins, num_scans, num_aggregations, was_aborted, was_cached,
                    read_table_ids, write_table_ids
                FROM df
            """)

        # ✅ Append to CSV
        df.to_csv(CSV_FILE, mode='a', header=not os.path.exists(CSV_FILE), index=False)

        print(f"\n✅ Cleaned Data Stored in DuckDB & CSV")
        print(f"📂 Cleaned data appended to '{CSV_FILE}'")

    except Exception as e:
        print(f"❌ Error processing message: {e}")


def consume_and_process():
    """
    Continuously consumes data from Kafka, cleans it, and stores it in DuckDB.
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
        clean_and_store_message(msg)


if __name__ == "__main__":
    consume_and_process()
