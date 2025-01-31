import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import os
from datetime import datetime
from numerize import numerize
from confluent_kafka import Consumer, KafkaError
import duckdb
import json
import time
from datetime import datetime, timedelta
import random
import ddb_wrappers as ddb


# Set up the page layout
st.set_page_config(page_title="Redset Dashboard", page_icon="🌍", layout="wide")
st.header("Redset Dashboard")

# Load the CSV file
#csv_file_path = 'organized_sample_0.001.csv'
#df = pd.read_csv(csv_file_path)

# Convert 'arrival_timestamp' to datetime if it's not already
#df['arrival_timestamp'] = pd.to_datetime(df['arrival_timestamp'], errors='coerce')

# Sidebar configuration
st.sidebar.header("Menu")

# 1. Historical View / Live View Toggle
view_mode = st.sidebar.radio("Select View", ("Historical View", "Live View"))

# 2. File Size Display
#file_size = os.path.getsize(csv_file_path)
#st.sidebar.write(f"File Size: {numerize.numerize(file_size)}")  # Corrected usage

# 3. Arrival Time Range Slider (only for Historical View)
#if view_mode == "Historical View":
#    start_date = st.sidebar.date_input("Select Start Date", min_value=df['arrival_timestamp'].min().date(), max_value=df['arrival_timestamp'].max().date())
#    end_date = st.sidebar.date_input("Select End Date", min_value=df['arrival_timestamp'].min().date(), max_value=df['arrival_timestamp'].max().date())

    # Filter data based on selected arrival time range for Historical View
#    df_selection = df[(df['arrival_timestamp'].dt.date >= start_date) & (df['arrival_timestamp'].dt.date <= end_date)]
#else:
    # For Live View, display all records that are recent (for example, last 24 hours)
#    now = datetime.now()
#    one_day_ago = now - pd.Timedelta(days=1)
#    df_selection = df[df['arrival_timestamp'] >= one_day_ago]
# Add fade-in animation for the whole dashboard
# Add fade-in animation for the whole dashboard

st.markdown("""
    <style>
    body {
        animation: fadeIn 1.5s ease-in;
    }
    
    @keyframes fadeIn {
        0% {
            opacity: 0;
        }
        100% {
            opacity: 1;
        }
    }

    /* Section transitions */
    .section {
        animation: fadeUp 0.5s ease-in-out;
    }

    @keyframes fadeUp {
        0% {
            transform: translateY(20px);
            opacity: 0;
        }
        100% {
            transform: translateY(0);
            opacity: 1;
        }
    }

    /* Fun animations on hover */
    .hover-box:hover {
        transform: scale(1.05);
        transition: all 0.3s ease;
    }
    
    </style>
""", unsafe_allow_html=True)



########### Kafka topic to DuckDB ###################
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
COMPILE_COLUMNS = ['instance_id','num_joins','num_scans','num_aggregations', 'mbytes_spilled']
STRESS_COLUMNS = ['instance_id','was_aborted','arrival_timestamp',
                  'compile_duration_ms','execution_duration_ms',
                  'queue_duration_ms', 'mbytes_scanned','mbytes_spilled']


def initialize_duckdb():
    """Create a table in DuckDB if it does not exist"""
    con = duckdb.connect(DUCKDB_FILE)
    '''
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            instance_id BIGINT,
            cluster_size DOUBLE,
            user_id BIGINT,
            database_id BIGINT,
            query_id BIGINT,
            arrival_timestamp TIMESTAMP,
            compile_duration_ms DOUBLE,
            queue_duration_ms BIGINT,
            execution_duration_ms BIGINT,
            feature_fingerprint VARCHAR,
            was_aborted BOOLEAN,
            was_cached BOOLEAN,
            cache_source_query_id DOUBLE,
            query_type VARCHAR,
            num_permanent_tables_accessed DOUBLE,
            num_external_tables_accessed DOUBLE,
            num_system_tables_accessed DOUBLE,
            read_table_ids VARCHAR,
            write_table_ids DOUBLE,
            mbytes_scanned DOUBLE,
            mbytes_spilled DOUBLE,
            num_joins BIGINT,
            num_scans BIGINT,
            num_aggregations BIGINT,
            batch_id BIGINT,
        )
    """)
    '''
    con.execute(f"""
    CREATE TABLE IF NOT EXISTS LIVE_QUERY_METRICS (
        instance_id BIGINT,
        was_aborted BOOLEAN,
        was_cached BOOLEAN,
        query_type VARCHAR
    )
    """)
    con.close()


def create_consumer(topic, group_id):
    """Create a Confluent Kafka Consumer."""
    consumer_t = Consumer({
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': group_id,
        'auto.offset.reset': 'earliest',  # Read from the beginning if no offset found, useful if consumer crashes
        'enable.auto.commit': False        # Disable auto commit, ensure 
    })
    consumer_t.subscribe([topic])
    return consumer_t


def Kafka_topic_to_DuckDB():
    consumer_raw_data = create_consumer(TOPIC_RAW_DATA, 'raw_data')
    #consumer_leaderboard = create_consumer(TOPIC_LEADERBOARD, 'live_analytics')
    consumer_query_counter = create_consumer(TOPIC_QUERY_METRICS, 'live_analytics')
    #consumer_compile = create_consumer(TOPIC_COMPILE_METRICS, 'live_analytics')
    #consumer_stress = create_consumer(TOPIC_STRESS_INDEX, 'live_analytics')
    initialize_duckdb()
    con = duckdb.connect(DUCKDB_FILE)

    print(f"Listening for messages on topic '{TOPIC_RAW_DATA}'...")

    query_counter_table = st.empty()

    try:
        while True:
            ddb.parquet_to_table(consumer_query_counter,'LIVE_QUERY_METRICS',con, QUERY_COLUMNS,TOPIC_QUERY_METRICS)
            # display function 
            live_view_graphs(query_counter_table)
            time.sleep(5)
            ddb.check_duckdb_table('LIVE_QUERY_METRICS',con)

    except KeyboardInterrupt:
        print("\nStopping consumer...")
    finally:
        consumer_raw_data.close()
        #consumer_leaderboard.close()
        consumer_query_counter.close()
        #consumer_compile.close()
        #consumer_stress.close()


########### DuckDB to Dashboard ################
def live_view_graphs(query_counter):
    try:
        # Fetch real-time sorted data
        con = duckdb.connect(DUCKDB_FILE)
        df = ddb.build_live_query_counts(con)
        con.close()
        query_counter.dataframe(df)

    except KeyboardInterrupt:
        st.warning("Stream ended!")

# Show content based on the selected view mode
if view_mode == "Historical View":
    Kafka_topic_to_DuckDB()

elif view_mode == "Live View":
    Kafka_topic_to_DuckDB()

# Footer: Add a custom footer
st.markdown("""
    <footer style="text-align:center; font-size:12px; color:grey; padding-top:20px; border-top: 1px solid #e0e0e0; margin-top:20px;">
        <p>Pipeline Pioneers &copy; 2025 | UTN</p>
    </footer>
""", unsafe_allow_html=True)

# Hide Streamlit elements like the default main menu and header, but not the custom footer
hide_st_style = """
<style>
#MainMenu {visibility:hidden;}
footer
header {visibility:hidden;}
</style>
"""
st.markdown(hide_st_style, unsafe_allow_html=True)

