import duckdb
import time
import threading
import pandas as pd
import streamlit as st
from datetime import datetime

# Initialize the DuckDB connection
DUCKDB_FILE = 'your_database.duckdb' ### change
con = duckdb.connect(DUCKDB_FILE) ## change

# Define the table creation SQL queries
create_flattened_table_ids_table = """
    CREATE OR REPLACE TABLE IF NOT EXISTS flattened_table_ids (
        instance_id int32,
        query_id int64,
        write_table_id int64,
        read_table_id int64,
        arrival_timestamp timestamp,
        query_type varchar)
"""
create_ingestion_intervals_per_table = """
    CREATE OR REPLACE TABLE IF NOT EXISTS ingestion_intervals_per_table (
        instance_id int32,
        query_id int64,
        write_table_id int64,
        current_timestamp timestamp,
        next_timestamp timestamp)
"""
create_output_table = """
    CREATE OR REPLACE TABLE IF NOT EXISTS output_table(
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

# Function to create tables if they don't exist
def create_tables():
    con.execute(create_flattened_table_ids_table)
    con.execute(create_ingestion_intervals_per_table)
    con.execute(create_output_table)

# Define the query for inserting new data into the ingestion intervals table
insert_into_ingestion_intervals_per_table = """
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
    FROM query_count_per_table t1
    LEFT JOIN query_count_per_table t2
        ON t1.write_table_id = t2.write_table_id
        AND t1.instance_id = t2.instance_id
        AND t2.arrival_timestamp > t1.arrival_timestamp
    WHERE t1.query_type IN ('insert', 'copy')
    AND t1.arrival_timestamp BETWEEN '{start}' AND '{end}'
"""

# Define the query for updating the ingestion intervals table
update_ingestion_intervals_per_table = """
    UPDATE ingestion_intervals_per_table target
    SET next_timestamp = source.next_timestamp
    FROM (
        SELECT
            t1.instance_id,
            t1.query_id,
            t1.write_table_id,
            t1.arrival_timestamp AS current_timestamp,
            t2.arrival_timestamp AS next_timestamp
        FROM query_count_per_table t1
        LEFT JOIN query_count_per_table t2
            ON t1.write_table_id = t2.write_table_id
            AND t1.instance_id = t2.instance_id
            AND t2.arrival_timestamp > t1.arrival_timestamp
        WHERE t1.query_type IN ('insert', 'copy')
    ) AS source
    WHERE target.query_id = source.query_id
    AND target.instance_id = source.instance_id
    AND target.next_timestamp IS NULL
    AND source.next_timestamp IS NOT NULL;
"""

# Insert data into the output table
insert_into_output_table = """
    INSERT INTO output_table (
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
    -- Logic to match queries and ingestion timestamps
    SELECT
        q.instance_id,
        q.query_id,
        q.query_type,
        q.write_table_id,
        q.read_table_id,
        q.arrival_timestamp,
        i.current_timestamp AS last_write_table_insert,
        i.next_timestamp AS next_write_table_insert
    FROM query_count_per_table q
    LEFT JOIN ingestion_intervals_per_table i
        ON q.write_table_id = i.write_table_id
        AND q.query_id = i.query_id
        AND q.instance_id = i.instance_id
    WHERE q.arrival_timestamp BETWEEN '{start}' AND '{end}'
"""

# Function to perform periodic updates
def update_tables_periodically():
    while True:
        # Execute table creation (if necessary)
        create_tables()

        # Execute data insertions and updates
        con.execute(insert_into_ingestion_intervals_per_table)
        con.execute(update_ingestion_intervals_per_table)
        con.execute(insert_into_output_table)
        
        # Wait for a specific interval before running the updates again
        time.sleep(10)  # Update every 10 seconds (or your preferred interval)

# Start the table update in a separate thread to run in the background
update_thread = threading.Thread(target=update_tables_periodically, daemon=True)
update_thread.start()

st.write("Tables are being updated every 10 seconds with new data as it arrives.")

