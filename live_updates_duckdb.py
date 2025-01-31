import pandas as pd
import json
import time
import requests
import duckdb
from confluent_kafka import Consumer

# Kafka Configuration
KAFKA_BROKER = 'localhost:9092'
TOPIC_RAW_DATA = 'parquet_stream'  # Kafka topic

# DuckDB Configuration
DUCKDB_FILE = "live_cleaned_data.duckdb001"

# Dashboard API Endpoint (Set when available)
DASHBOARD_API_URL = None  # Set when dashboard API is ready

# Define table update intervals (in seconds)
UPDATE_INTERVALS = {
    "live_query_metrics": 1,
    "live_leaderboard": 1,
    "live_compile_metrics": 1,
    "live_stress_index": 1
}

# Define the required columns for each table
TABLE_COLUMNS = {
    "live_query_metrics": ['instance_id', 'was_aborted', 'was_cached', 'query_type', 'arrival_timestamp'],
    "live_leaderboard": ['instance_id', 'query_id', 'user_id', 'arrival_timestamp', 'compile_duration_ms'],
    "live_compile_metrics": ['instance_id', 'arrival_timestamp', 'num_joins', 'num_scans', 'num_aggregations', 'mbytes_spilled'],
    "live_stress_index": ['instance_id', 'was_aborted', 'arrival_timestamp', 'compile_duration_ms',
                          'execution_duration_ms', 'queue_duration_ms', 'mbytes_scanned', 'mbytes_spilled']
}

# Initialize Kafka Consumer
consumer = Consumer({
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': 'live-data-group',
    'auto.offset.reset': 'earliest'
})

consumer.subscribe([TOPIC_RAW_DATA])  # Subscribe to Kafka topic

# Track last update times
last_update_time = {table: 0 for table in UPDATE_INTERVALS}

print("\n🔄 Waiting for Kafka messages...")

# Connect to DuckDB
conn = duckdb.connect(DUCKDB_FILE)

# Create tables if they don’t exist
for table, columns in TABLE_COLUMNS.items():
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {table} (
            {', '.join([col + ' STRING' for col in columns])}  
        )
    """)

# Fix Schema: Ensure `arrival_timestamp` is part of `live_instance_data`
conn.execute("""
    CREATE TABLE IF NOT EXISTS live_instance_data (
        instance_id INT,
        cluster_id STRING,
        cluster_count INT,
        category STRING,
        arrival_timestamp TIMESTAMP  -- ✅ Ensure timestamp is included
    )
""")

# Fix Schema: Ensure `global_instances` includes timestamps
conn.execute("""
    CREATE TABLE IF NOT EXISTS global_instances (
        instance_id INT,
        cluster_id STRING,
        cluster_count INT,
        arrival_timestamp TIMESTAMP
    )
""")


def basic_cleaning(df, table_name):
    """Performs standard data cleaning before advanced processing."""
    required_columns = TABLE_COLUMNS[table_name]
    df = df[required_columns].copy()

    # Replace NULL values
    df.replace({"NULL": pd.NA, "NaN": pd.NA, "": pd.NA}, inplace=True)

    # Convert timestamp properly
    if "arrival_timestamp" in df.columns:
        df["arrival_timestamp"] = pd.to_datetime(df["arrival_timestamp"], errors="coerce")
        df["arrival_timestamp"].fillna(pd.Timestamp("1970-01-01"), inplace=True)

    # Convert numeric fields safely
    numeric_columns = [
        "instance_id", "query_id", "user_id", "compile_duration_ms",
        "execution_duration_ms", "queue_duration_ms", "num_joins",
        "num_scans", "num_aggregations", "mbytes_spilled", "mbytes_scanned"
    ]
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(float)

    # Convert boolean values properly
    boolean_columns = ["was_aborted", "was_cached"]
    for col in boolean_columns:
        if col in df.columns:
            df[col] = df[col].astype(bool)

    return df


def advanced_cleaning(df):
    """Cleans data by categorizing instance-cluster mappings & ensuring timestamps exist."""
    print("\n🔹 BEFORE CLEANING 🔹")
    print(df)

    # Ensure `arrival_timestamp` exists
    if "arrival_timestamp" not in df.columns:
        df["arrival_timestamp"] = pd.Timestamp.now()  # Use current time if missing

    # Count clusters per instance
    instance_counts = df.groupby("instance_id")["cluster_id"].nunique().reset_index()
    instance_counts.columns = ["instance_id", "cluster_count"]

    # Categorize instances
    def categorize_instance(cluster_count):
        if cluster_count <= 2:
            return "Local"
        elif cluster_count <= 5:
            return "Regional"
        else:
            return "Global"

    instance_counts["category"] = instance_counts["cluster_count"].apply(categorize_instance)

    # Merge category labels back into the original DataFrame
    df = df.merge(instance_counts, on="instance_id", how="left")

    # Store Global instances separately
    global_instances = df[df["category"] == "Global"].copy()
    df = df[df["category"] != "Global"]  # Keep only Local & Regional instances

    store_in_duckdb("global_instances", global_instances)
    print("\n✅ AFTER CLEANING ✅")
    print(df)

    return df


def store_in_duckdb(table_name, df):
    """Store cleaned data in DuckDB."""
    try:
        conn.execute(f"INSERT INTO {table_name} SELECT * FROM df")
        print(f"✅ Stored {table_name} in DuckDB.")
    except Exception as e:
        print(f"❌ Error storing data in DuckDB: {e}")


def clean_old_data():
    """Deletes old or incorrect records from DuckDB."""
    try:
        conn.execute("DELETE FROM live_compile_metrics WHERE arrival_timestamp IS NULL OR arrival_timestamp = '1970-01-01'")
        conn.execute("DELETE FROM live_stress_index WHERE arrival_timestamp IS NULL OR arrival_timestamp = '1970-01-01'")
        print("✅ Deleted incorrect timestamps from DuckDB")
    except Exception as e:
        print(f"❌ Error cleaning old data: {e}")


# Periodic cleanup scheduler (runs every 24 hours)
last_cleanup_time = time.time()

while True:
    msg = consumer.poll(timeout=1.0)

    if msg is None:
        continue

    if msg.error():
        print(f"❌ Kafka Error: {msg.error()}")
        continue

    try:
        # Convert message to Pandas DataFrame
        message_value = json.loads(msg.value().decode('utf-8'))
        df = pd.DataFrame([message_value])

        # Ensure `arrival_timestamp` is added
        if "arrival_timestamp" not in df.columns:
            df["arrival_timestamp"] = pd.Timestamp.now()

        # Apply advanced cleaning only on instance-cluster data
        if "cluster_id" in df.columns:
            cleaned_instance_df = advanced_cleaning(df)
            store_in_duckdb("live_instance_data", cleaned_instance_df)

        # Apply basic cleaning for each table and store in DuckDB
        current_time = time.time()
        for table_name, update_interval in UPDATE_INTERVALS.items():
            if current_time - last_update_time[table_name] >= update_interval:
                cleaned_df = basic_cleaning(df, table_name)
                store_in_duckdb(table_name, cleaned_df)
                last_update_time[table_name] = current_time

        # Run cleanup every 24 hours
        if current_time - last_cleanup_time >= 864:
            clean_old_data()
            last_cleanup_time = current_time

    except Exception as e:
        print(f"❌ Error processing message: {e}")
