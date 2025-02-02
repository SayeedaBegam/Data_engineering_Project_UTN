import duckdb
import time
import threading
import pandas as pd
import streamlit as st
from datetime import datetime, timedelta
from confluent_kafka import Consumer

import os
import json

# Initialize the DuckDB connection
DUCKDB_FILE = 'cleaned_data.duckdb'
con = duckdb.connect(DUCKDB_FILE)
TOPIC_FLAT_TABLES = 'flattened'
FLAT_COLUMNS = ['instance_id','query_id','write_table_ids','read_table_ids','arrival_timestamp','query_type']

#start = '2024-29-02 23:59:00' # first timestamp in the dataset
#end = '2024-03-01 01:00:00'
instance_id = 85
KAFKA_BROKER = 'localhost:9092'


def parquet_to_table(consumer, table, conn, columns,topic):
    """
    Reads messages from a Kafka consumer, extracts data from JSON, writes to Parquet,
    and loads into a DuckDB table.
    
    Args:
        consumer: Kafka consumer instance.
        table: DuckDB table name.
        conn: DuckDB connection.
    """
    #print('hello')
    data_list = []
    parquet_file = f"kafka_data_{table}.parquet"
    
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
                records = [records]  # Ensure list format

            data_list.extend(records)
        
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON: {e}")

    if not data_list:  # If no data was received, exit function early
        print("No data received from Kafka.")
        return

    df = pd.DataFrame(data_list)
    df = df[columns]

    if "arrival_timestamp" in df.columns:
        df["arrival_timestamp"] = pd.to_datetime(df["arrival_timestamp"], errors='coerce')  # Handle parsing errors
    if topic == 'flattened':
        df['read_table_ids'] = df['read_table_ids'].astype(str).str.split(",")
        df = df.explode('read_table_ids', ignore_index=True)

        # Handle None/NaN values before conversion
        df['read_table_ids'] = pd.to_numeric(df['read_table_ids'], errors='coerce')

        # Convert to nullable integer type (allows NaN values)
        df['read_table_ids'] = df['read_table_ids'].astype(pd.Int64Dtype())
    #print(df)
    # Save as Parquet
    df.to_parquet(parquet_file, index=False)
    #time.sleep(4)
    # Get absolute path for DuckDB compatibility
    parquet_path = os.path.abspath(parquet_file)

    # Load into DuckDB
    conn.execute(f"COPY {table} FROM '{parquet_path}' (FORMAT PARQUET)")
    consumer.commit()  # Commit offset to ensure that only new data is written


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
            print("Table Preview:", table_name)
            print(df_preview)
            
        else:
            print(f"Table '{table_name}' exists but is EMPTY.")

        return True, row_count

    except Exception as e:
        print(f"Error checking table '{table_name}': {e}")
        return False, 0

# Define the table creation SQL queries
#inserted for Kafka
create_flattened_table_ids_table = """
    CREATE OR REPLACE TABLE flattened_table_ids (
        instance_id int32,
        query_id int64,
        write_table_id int64,
        read_table_id int64,
        arrival_timestamp timestamp,
        query_type varchar)
"""
create_ingestion_intervals_per_table = """
    CREATE OR REPLACE TABLE ingestion_intervals_per_table (
        instance_id int32,
        query_id int64,
        write_table_id int64,
        current_timestamp timestamp,
        next_timestamp timestamp)
"""
create_output_table = """
    CREATE OR REPLACE TABLE output_table(
        instance_id int32,
        query_id int64,
        query_type varchar,
        write_table_id int64,
        read_table_id int64,
        arrival_timestamp timestamp,
        last_write_table_insert timestamp,
        next_write_table_insert timestamp,
        time_since_last_ingest_ms int64,
        time_to_next_ingest_ms int64
    )
"""

# view to count analytical queries vs. transform queries per table_id
create_view_tables_workload_count = """
    CREATE OR REPLACE VIEW tables_workload_count AS 
    WITH select_count_table AS (        
        SELECT --count select queries by read_table_ids
            instance_id,
            read_table_id AS table_read_by_select,
            COUNT(CASE WHEN query_type = 'select' THEN 1 END) AS select_count
        FROM output_table
        WHERE 1
            AND query_type = 'select'
            --AND instance_id = {instance_id}
        GROUP BY ALL
    ), transform_count_table AS (
        SELECT --count transformation queries by write_table_id
            instance_id,
            write_table_id AS table_transformed,
            COUNT(CASE WHEN query_type IN ('update', 'delete') THEN 1 END) AS transform_count
        FROM output_table
        WHERE 1
            AND query_type IN ('update', 'delete')
            --AND instance_id = {instance_id}
        GROUP BY ALL
    )
    SELECT 
        COALESCE(s.instance_id, t.instance_id) AS instance_id,
        COALESCE(t.table_transformed, s.table_read_by_select) AS table_id,
        t.transform_count,
        s.select_count,
    FROM select_count_table s
    FULL OUTER JOIN transform_count_table t
    ON t.table_transformed = s.table_read_by_select
    --WHERE instance_id = {instance_id}
"""


# Function to create tables if they don't exist
def create_tables():
    con.execute(create_flattened_table_ids_table)
    con.execute(create_ingestion_intervals_per_table)
    con.execute(create_output_table)
    con.execute(create_view_tables_workload_count)

# Function to perform periodic updates
# timestamps over which time period the queries will run
# re-run the queries on 60 second intervals of real time

# Execute table creation (if necessary)
create_tables()

consumer = Consumer({
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': 'analytics',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
        'enable.partition.eof': False,
    })

consumer.subscribe([TOPIC_FLAT_TABLES])

def update_tables_periodically():
    start = datetime.strptime('2024-02-29 23:59:00', '%Y-%m-%d %H:%M:%S')
    end = datetime.strptime('2024-03-01 01:00:00', '%Y-%m-%d %H:%M:%S')

    while True:    
        parquet_to_table(consumer,'flattened_table_ids',con,FLAT_COLUMNS,TOPIC_FLAT_TABLES)
        check_duckdb_table('tables_workload_count',con)
        #check_duckdb_table('output_table',con)

        query_current_max_timestamp = f"""
        SELECT MAX(arrival_timestamp)
        FROM flattened_table_ids
        """
        result = con.execute(query_current_max_timestamp).fetchone()
        con.execute("SELECT * FROM tables_workload_count")
        current_max_timestamp = result[0]  # Extract the value from the tuple
        print(current_max_timestamp)
        if current_max_timestamp  is None:
            continue
        if current_max_timestamp > datetime.strptime('2024-05-31 00:00:00', '%Y-%m-%d %H:%M:%S'):
            break
        
        print(start, 'then:', end)
        #print(con.execute(f"""SELECT {start}""").fetchone()[0])
        # if latest timestamp of inserted data has exceeded one hour after the
        # previous start time = end
        if (current_max_timestamp > end):
            print('hi again')
            # Define the query for inserting new data into the ingestion intervals table
            insert_into_ingestion_intervals_per_table = f"""
                INSERT INTO ingestion_intervals_per_table (
                    instance_id, 
                    query_id, 
                    write_table_id, 
                    current_timestamp, 
                    next_timestamp
                )   
                -- Insert logic based on the query and arrival timestamps
                SELECT
                    t1.instance_id,
                    t1.query_id,
                    t1.write_table_id,
                    t1.arrival_timestamp AS current_timestamp,
                    t2.arrival_timestamp AS next_timestamp
                FROM flattened_table_ids t1
                LEFT JOIN flattened_table_ids t2
                    ON t1.write_table_id = t2.write_table_id
                    AND t1.instance_id = t2.instance_id
                    AND t2.arrival_timestamp > t1.arrival_timestamp
                WHERE 1
                    AND t1.query_type IN ('insert', 'copy')
                    AND t1.arrival_timestamp BETWEEN '{start}' AND '{end}'
                    --AND t1.instance_id = {instance_id}
                    --AND t2.instance_id = {instance_id}
            """

            # Define the query for updating the ingestion intervals table
            update_ingestion_intervals_per_table = f"""
                UPDATE ingestion_intervals_per_table target
                SET next_timestamp = source.next_timestamp
                FROM (
                    SELECT
                        t1.instance_id,
                        t1.query_id,
                        t1.write_table_id,
                        t1.arrival_timestamp AS current_timestamp,
                        t2.arrival_timestamp AS next_timestamp
                    FROM flattened_table_ids t1
                    LEFT JOIN flattened_table_ids t2
                        ON t1.write_table_id = t2.write_table_id
                        AND t1.instance_id = t2.instance_id
                        AND t2.arrival_timestamp > t1.arrival_timestamp
                    WHERE 1
                        AND t1.query_type IN ('insert', 'copy')
                        --AND t1.instance_id = {instance_id}
                ) AS source
                WHERE 1
                    AND target.query_id = source.query_id
                    AND target.instance_id = source.instance_id
                    AND target.next_timestamp IS NULL
                    AND source.next_timestamp IS NOT NULL
                    --AND source.instance_id = {instance_id}
            """

            # Insert data into the output table
            insert_into_output_table = f"""
            INSERT INTO output_table(
                    instance_id,
                    query_id,
                    query_type,
                    write_table_id,
                    read_table_id,
                    arrival_timestamp,
                    last_write_table_insert,
                    next_write_table_insert,
                    time_since_last_ingest_ms,
                    time_to_next_ingest_ms
                    )
            WITH output AS (
            SELECT
                q.instance_id,
                q.query_id,
                q.query_type,
                q.write_table_id,
                q.read_table_id,
                q.arrival_timestamp,
                i.current_timestamp AS last_write_table_insert,
                i.next_timestamp AS next_write_table_insert
            FROM flattened_table_ids q
            LEFT JOIN ingestion_intervals_per_table i
                ON q.write_table_id = i.write_table_id
                AND q.query_id = i.query_id
                AND q.instance_id = i.instance_id
            WHERE 1
                --AND q.instance_id = {instance_id}
                --AND i.instance_id = {instance_id}
                AND q.arrival_timestamp BETWEEN '{start}' AND '{end}'
            ) 
            SELECT 
                o.instance_id,
                o.query_id,
                o.query_type,
                o.write_table_id,
                o.read_table_id,
                o.arrival_timestamp,
                i.last_write_table_insert,
                i.next_write_table_insert,
                EPOCH_MS(o.arrival_timestamp - i.last_write_table_insert) AS time_since_last_ingest_ms,
                EPOCH_MS(i.next_write_table_insert - o.arrival_timestamp) AS time_to_next_ingest_ms   
            FROM output o
            JOIN output AS i
                ON i.query_type IN ('insert','copy') -- will make cartesian product with table i filtered by query_type
                AND o.arrival_timestamp BETWEEN 
                    i.last_write_table_insert --if no ingest query has run before, the query will be filtered out
                    AND COALESCE(i.next_write_table_insert, STRPTIME('31.12.2999', '%d.%m.%Y')::TIMESTAMP) --to handle NULL values
                AND o.instance_id = i.instance_id
                AND (
                    (o.query_type = 'select' AND o.read_table_id = i.write_table_id) -- a select query should match the read with the write table; if a write query hasn't run before, the corresponding select query will be filtered out
                    OR (o.query_type != 'select' AND o.write_table_id = i.write_table_id)  -- all other queries should math the write tables
                )
            WHERE 1
                AND o.query_type NOT IN ('insert', 'copy') -- ingestion queries would show as duplicates therefore filter and add in the union all
                --AND o.instance_id = {instance_id}
                --AND i.instance_id = {instance_id}
                AND o.arrival_timestamp BETWEEN '{start}' AND '{end}'
            -- add back the ingestion queries
            UNION ALL  
            SELECT  
                instance_id,
                query_id,
                query_type,
                write_table_id,
                read_table_id,
                arrival_timestamp,
                last_write_table_insert,
                next_write_table_insert,
                EPOCH_MS(arrival_timestamp - last_write_table_insert) AS time_since_last_ingest_ms,
                EPOCH_MS(next_write_table_insert - arrival_timestamp) AS time_to_next_ingest_ms   
            FROM output
            WHERE 1
                AND query_type IN ('insert', 'copy')
                --AND instance_id = {instance_id}
                AND arrival_timestamp BETWEEN '{start}' AND '{end}'   
            """

            # Execute data insertions and updates
            con.execute(insert_into_ingestion_intervals_per_table)
            con.execute(update_ingestion_intervals_per_table)
            con.execute(insert_into_output_table)
            

            # # Wait for a specific interval before running the updates again
            # time.sleep()  # Update every 10 seconds (or your preferred interval)
            
            # # Start the table update in a separate thread to run in the background
            # update_thread = threading.Thread(target=update_tables_periodically, daemon=True)
            # update_thread.start()

            # st.write("Tables are being updated every 10 seconds with new data as it arrives.")

            start = end
            end = end + timedelta(hours=1)
        #time.sleep(1)

update_tables_periodically()
