import pandas as pd
from confluent_kafka import Producer, Consumer
from confluent_kafka.admin import AdminClient, NewTopic, ConfigResource
import json
import pyarrow.parquet as pq
import time
from collections import deque
from ksql import KSQLAPI
import requests
import duckdb
import os



DUCKDB_FILE = "data.duckdb"

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

def write_to_topic(batch, topic, producer, list_columns):
    try:
        if not isinstance(batch, pd.DataFrame):
            raise ValueError("Expected 'batch' to be a pandas DataFrame.")

        # Select only relevant columns
        selected_columns = batch[list_columns]

        if selected_columns.empty:
            print(f"Warning: No relevant columns found for topic '{topic}'.")
            return
        
        # Convert to a list of dictionaries (each row as a JSON object)
        json_payloads = selected_columns.to_dict(orient='records')

        # Send each record individually
        for record in json_payloads:
            producer.produce(topic, value=json.dumps(record))

    except Exception as e:
        print(f"Error writing to topic '{topic}': {e}")

    finally:
        producer.flush()

def write_all_to_topic(batch, topic, producer):
    try:
        # Ensure batch is a pandas DataFrame
        if not isinstance(batch, pd.DataFrame):
            raise ValueError("Expected 'batch' to be a pandas DataFrame.")
        # Convert to JSON format
        json_payload = batch.to_json(orient='records')

        # Produce the message to Kafka
        producer.produce(topic, value=json_payload)

    except Exception as e:
        print(f"Error creating topic '{topic}': {e}")

    finally:
        producer.flush()

def create_kafka_topic(topic_name, num_partitions, replication_factor):
    """
    Creates a Kafka topic if it doesn't already exist.
    
    :param topic_name: Name of the topic to create.
    :param num_partitions: Number of partitions for the topic.
    :param replication_factor: Replication factor for the topic.
    Partitions ≥ number of consumers to maximize parallelism.
    Replication factor ≤ number of brokers (you can’t replicate to more brokers than exist).
    Balance load by distributing partitions evenly.
    """
    try:
        # Kafka Admin Client configuration
        admin_client = AdminClient({'bootstrap.servers': 'localhost:9092'})

        # Check if the topic already exists
        topics = admin_client.list_topics(timeout=10).topics
        if topic_name in topics:
            print(f"Topic '{topic_name}' already exists.")
            return

        # Define the new topic
        new_topic = NewTopic(topic=topic_name, num_partitions=num_partitions, replication_factor=replication_factor)

        # Create the topic
        admin_client.create_topics([new_topic])
        print(f"Topic '{topic_name}' created successfully.")
    except Exception as e:
        print(f"Error creating topic '{topic_name}': {e}")

def create_consumer(topic, group_id):
    """Create a Confluent Kafka Consumer."""
    return Consumer({
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': group_id,
        'auto.offset.reset': 'earliest',  # Start reading from the beginning
        'enable.auto.commit': True       # Automatically commit offsets
    }, logger=None)

def create_all_topics():
    create_kafka_topic('durations', num_partitions = 3)
    create_kafka_topic('query_counter', num_partitions = 3)
    create_kafka_topic('ids', num_partitions=3)
    return

def write_clean_data(producer,batch,topic='clean_data'):
    """
    Type casts a Pandas DataFrame's columns to their appropriate formats.
    ARGS
    batch df: Pandas DataFrame with raw data
    topic: where the data is written to
    """
    # Define the expected data types
    dtype_mapping = {
        "instance_id": "string",
        "cluster_size": "Int64",
        "user_id": "string",
        "database_id": "string",
        "query_id": "string",
        "arrival_timestamp": "datetime64[ns]",
        "compile_duration_ms": "Int64",
        "queue_duration_ms": "Int64",
        "execution_duration_ms": "Int64",
        "feature_fingerprint": "string",
        "was_aborted": "boolean",
        "was_cached": "boolean",
        "cache_source_query_id": "string",
        "query_type": "string",
        "num_permanent_tables_accessed": "Int64",
        "num_external_tables_accessed": "Int64",
        "num_system_tables_accessed": "Int64",
        "read_table_ids": "string",
        "write_table_ids": "string",
        "mbytes_scanned": "Int64",
        "mbytes_spilled": "Int64",
        "num_joins": "Int64",
        "num_scans": "Int64",
        "num_aggregations": "Int64",
    }

    # Convert data types
    for column, dtype in dtype_mapping.items():
        if column in df.columns:
            try:
                if dtype == "datetime64[ns]":
                    df[column] = pd.to_datetime(df[column], errors='coerce')
                else:
                    df[column] = df[column].astype(dtype)
            except Exception as e:
                print(f"Warning: Could not cast column '{column}' to {dtype} - {e}")

def purge_kafka_topic(topic_name):

    """
    Purges all messages from a Kafka topic by setting retention.ms to 1ms, 
    waiting, and then resetting it.
    
    Args:
        topic_name (str): Name of the Kafka topic to purge.
        bootstrap_servers (str): Kafka broker connection string.
    """
    try:
        # Step 1: Create Kafka Admin Client
        admin_client = AdminClient({'bootstrap.servers': KAFKA_BROKER})

        # Step 2: Set topic retention to 1ms (forces deletion of messages)
        config_resource = ConfigResource(ConfigResource.Type.TOPIC, topic_name, {'retention.ms': '1'})
        admin_client.alter_configs([config_resource])
        print(f"Retention policy changed to 1ms for topic '{topic_name}', messages will be deleted soon.")

        # Step 3: Wait a few seconds for Kafka to process deletions
        import time
        time.sleep(1)

        # Step 4: Reset retention policy back to default (7 days or desired value)
        config_resource = ConfigResource(ConfigResource.Type.TOPIC, topic_name, {'retention.ms': '604800000'})  # 7 days
        admin_client.alter_configs([config_resource])
        print(f"Retention policy restored for topic '{topic_name}'.")

    except Exception as e:
        print(f"Error modifying retention policy: {e}")

def parquet_to_table(consumer, table, conn, columns,topic):
    """
    Reads messages from a Kafka consumer, extracts data from JSON, writes to Parquet,
    and loads into a DuckDB table.
    
    Args:
        consumer: Kafka consumer instance.
        table: DuckDB table name.
        conn: DuckDB connection.
    """
    data_list = []
    parquet_file = "kafka_data.parquet"
    
    # Keep polling Kafka messages
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            break  # Stop polling when there are no more messages
        
        if msg.error():
            print(f"Kafka Error: {msg.error()}")
            continue  # Skip errors and keep polling

        # Deserialize JSON Kafka message
        message_value = msg.value().decode('utf-8')

        try:
            # Convert JSON string to dictionary
            records = json.loads(message_value)

            if isinstance(records, dict):
                records = [records]

            data_list.extend(records)
        
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON: {e}")

    if not data_list:  # If no data was received, exit function early
        print("No data received from Kafka.")
        return

    df = pd.DataFrame(data_list)

    # Ensure column  (match DuckDB schema)

    df = df[columns]


    # Save as Parquet
    df.to_parquet(parquet_file, index=False)

    # Get absolute path for DuckDB compatibility
    parquet_path = os.path.abspath(parquet_file)

    # Load into DuckDB
    conn.execute(f"COPY {table} FROM '{parquet_path}' (FORMAT PARQUET)")

    print(f"✅ Successfully loaded {len(df)} rows into {table}.")
    purge_kafka_topic(topic)

def check_duckdb_table(table_name, conn):
    """
    Verifies if a table exists and has data in DuckDB.
    
    :param table_name: Name of the table to check.
    :param conn: DuckDB connection object.
    :return: Tuple (exists, row_count)
    """
    try:
        # Check if the table exists
        table_exists = conn.execute(
            f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{table_name}'"
        ).fetchone()[0] > 0

        if not table_exists:
            print(f"Table '{table_name}' does NOT exist in DuckDB.")
            return False, 0

        # Check if the table has data
        row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        
        if row_count > 0:
            print(f"Table '{table_name}' exists and contains {row_count} rows.")
            
            # Optionally show the first few rows
            df_preview = conn.execute(f"SELECT * FROM {table_name}").df()
            print("Table Preview:")
            print(df_preview)
            
        else:
            print(f"Table '{table_name}' exists but is EMPTY.")

        return True, row_count

    except Exception as e:
        print(f"Error checking table '{table_name}': {e}")
        return False, 0

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

def build_leaderboard_compiletime(con):
    '''
    PREREQUISITIES: parquet_to_table(consumer,'LIVE_LEADERBOARD', 
    con,LEADERBOARD_COLUMNS,TOPIC_LEADERBOARD) has already been called
    
    returns dataframe continaing top 10 compile times and their instance_id
    
    ARGS:
        con: duckdb connected cursor
    '''
    time.sleep(.5)
    df1 = con.execute(f"""
    SELECT 
    instance_id, 
    FLOOR(compile_duration_ms / 60000) || ':' || LPAD(FLOOR((compile_duration_ms % 60000) / 1000), 2, '0') AS compile_duration
    FROM LIVE_LEADERBOARD
    ORDER BY compile_duration_ms DESC
    LIMIT 10;
    """).df()
    return df1

def build_leaderboard_user_queries(con):
    '''
    PREREQUISITIES: parquet_to_table(consumer,'LIVE_LEADERBOARD', 
    con,LEADERBOARD_COLUMNS,TOPIC_LEADERBOARD) has already been called
    returns dataframe continaing top 5 user_ids who issued most queries
    ARGS:
        con: duckdb connected cursor
    '''
    time.sleep(.5)
    df =  con.execute(f"""
                       SELECT user_id, COUNT(*) as most_queries
                       FROM LIVE_LEADERBOARD
                       ORDER BY most_queries DESC
                       LIMIT 5;
                       """)
    return df

def build_live_query_counts(con):
    '''
    PREREQUISITIES: parquet_to_table(consumer,'LIVE_LEADERBOARD', 
    con,LEADERBOARD_COLUMNS,TOPIC_QUERY_METRICS) has already been called
    returns dataframe continaing top 5 user_ids who issued most queries
    ARGS:
        con: duckdb connected cursor
    '''

    df = con.execute(
        """
    SELECT 
    COUNT(*) AS total_queries,
    SUM(CASE WHEN was_aborted = TRUE THEN 1 ELSE 0 END) AS aborted_queries,
    SUM(CASE WHEN was_cached = TRUE THEN 1 ELSE 0 END) AS cached_queries
    FROM LIVE_QUERY_METRICS;
    """).df()
    return df

def build_live_query_distribution(con):
    '''
    PREREQUISITIES: parquet_to_table(consumer,'LIVE_QUERY_METRICS', 
    con,QUERY_COLUMNS,TOPIC_QUERY_METRICS) has already been called
    returns dataframe unique instances of query typesa nd the counts
    ARGS:
        con: duckdb connected cursor
    '''
    df = con.execute(
        """
    SELECT 
    COUNT(*) AS total_queries,
    SUM(CASE WHEN was_aborted = TRUE THEN 1 ELSE 0 END) AS aborted_queries,
    SUM(CASE WHEN was_cached = TRUE THEN 1 ELSE 0 END) AS cached_queries
    FROM LIVE_QUERY_METRICS;
    """).df()
    return df


def build_live_compile_metrics(con):
    '''
    PREREQUISITIES: parquet_to_table(consumer,'LIVE_QUERY_METRICS', 
    con,QUERY_COLUMNS,TOPIC_QUERY_METRICS) has already been called
    returns dataframe sum of joins, inserts, and aggregations
    ARGS:
        con: duckdb connected cursor
    '''
    df = con.execute(
        """
    SELECT 
    SUM(num_scans) AS total_scans,
    SUM(num_aggregates) AS total_aggregates,
    SUM(num_join) AS total_joins
    FROM LIVE_COMPILE_METRICS;
    """).df()
    return df


def build_live_compile_metrics(con):
    '''
    PREREQUISITIES: parquet_to_table(consumer,'LIVE_QUERY_METRICS', 
    con,QUERY_COLUMNS,TOPIC_QUERY_METRICS) has already been called
    returns dataframe sum of joins, inserts, and aggregations
    ARGS:
        con: duckdb connected cursor
    '''
    df = con.execute(
        """
    SELECT 
    SUM(num_scans) AS total_scans,
    SUM(num_aggregates) AS total_aggregates,
    SUM(num_join) AS total_joins
    FROM LIVE_COMPILE_METRICS;
    """).df()
    return df


def build_live_spilled_scanned(con):
    '''
    PREREQUISITIES: parquet_to_table(consumer,'LIVE_QUERY_METRICS', 
    con,QUERY_COLUMNS,TOPIC_QUERY_METRICS) has already been called
    returns dataframe sum of joins, inserts, and aggregations
    ARGS:
        con: duckdb connected cursor
    '''
    df = con.execute(
        """
    SELECT 
    SUM(mb_spilled) AS mb_spilled,
    SUM(mb_scanned) AS mb_scanned,
    FROM LIVE_COMPILE_METRICS;
    """).df()
    return df

