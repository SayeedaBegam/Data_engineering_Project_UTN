import pandas as pd
import json
import time
import duckdb
from confluent_kafka import Producer

# Kafka settings
KAFKA_BROKER = 'localhost:9092'  # Update if using a remote Kafka broker
TOPIC_RAW_DATA = 'raw_data'  # Updated Kafka topic name

# Path to your Parquet file
PARQUET_FILE_PATH = '/Users/orsuvinay/Documents/DBProject/redshift-downloads/redset/serverless/sample_0.001.parquet'

def load_parquet_with_duckdb(file_path):
    """Load Parquet file using DuckDB for better performance."""
    try:
        print(f"📂 Loading data from Parquet file: {file_path}")
        df = duckdb.query(f"SELECT * FROM '{file_path}'").to_df()  # Load using DuckDB
        print("✅ Data loaded successfully!")
        return df
    except Exception as e:
        print(f"❌ Error loading Parquet file: {e}")
        return pd.DataFrame()  # Return empty DataFrame if loading fails

def convert_timestamp(value):
    """Convert pandas Timestamp to ISO format string."""
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    return value

def preprocess_data(df):
    """Preprocess DataFrame to ensure it's JSON-serializable and Kafka-compatible."""
    if df.empty:
        print("⚠️ DataFrame is empty after loading!")
        return df

    # Fill NaN values with 'NULL'
    df = df.fillna('NULL')

    # Ensure correct data types
    for col in df.columns:
        if df[col].dtype == 'bool':  # Convert boolean
            df[col] = df[col].astype(bool)
        elif df[col].dtype == 'float64':  # Ensure float columns stay as float
            df[col] = df[col].astype(float)
        elif df[col].dtype == 'int64':  # Ensure integer columns stay as int
            df[col] = df[col].astype(int)
        elif df[col].dtype == 'object':  # Convert objects to string
            df[col] = df[col].astype(str)

    return df

def send_parquet_to_kafka(producer, data_batch):
    """Send Parquet data to Kafka."""
    for message in data_batch:
        # Convert message to JSON format
        message = {key: convert_timestamp(value) for key, value in message.items()}
        json_message = json.dumps(message)

        try:
            producer.produce(TOPIC_RAW_DATA, value=json_message)
            producer.flush()
            print(f"✅ Sent to Kafka topic '{TOPIC_RAW_DATA}': {json_message}")
        except Exception as e:
            print(f"❌ Error sending message to Kafka: {e}")

        # Introduce delay between messages
        time.sleep(1)  # Reduced delay for efficiency
        print("⏳ Waiting for 1 second before sending the next record...")

def main():
    # Load data using DuckDB
    df = load_parquet_with_duckdb(PARQUET_FILE_PATH)
    
    if df.empty:
        print("⚠️ No data found. Exiting...")
        return

    # Preprocess data for Kafka
    df = preprocess_data(df)

    # Convert DataFrame rows to list of dictionaries
    data_batch = df.to_dict(orient='records')
    print(f"📦 Prepared {len(data_batch)} records to send to Kafka topic '{TOPIC_RAW_DATA}'.")

    # Initialize Kafka producer
    producer = Producer({'bootstrap.servers': KAFKA_BROKER})

    # Send Parquet data to Kafka
    send_parquet_to_kafka(producer, data_batch)

if __name__ == '__main__':
    main()
