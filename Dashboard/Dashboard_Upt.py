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


# Sidebar configuration
st.sidebar.header("Menu")

# Historical View / Live View Toggle
view_mode = st.sidebar.radio("Select View", ("Historic View", "Live View"))

# Add custom styling (CSS) for Query Counter and Leaderboard
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
    
    /* Styling for Query Counter Table */
    .query-counter-table {
        width: 100%;
        margin: 20px 0;
        padding: 15px;
        border-collapse: collapse;
        background-color: #f4f4f9;
        border-radius: 8px;
        box-shadow: 0 2px 5px rgba(0, 0, 0, 0.1);
    }

    .query-counter-table th, .query-counter-table td {
        padding: 12px;
        text-align: center;
        font-size: 16px;
        color: #333;
        border: 1px solid #ddd;
    }

    .query-counter-table th {
        background-color: #4CAF50;
        color: white;
        font-weight: bold;
    }

    .query-counter-table td {
        background-color: #f9f9f9;
    }

    .query-counter-table tr:nth-child(even) td {
        background-color: #f1f1f1;
    }

    /* Styling for Leaderboard */
    .leaderboard-container {
        margin-top: 30px;
        padding: 20px;
        background-color: #fff;
        border-radius: 10px;
        box-shadow: 0 4px 8px rgba(0, 0, 0, 0.1);
    }

    .leaderboard-header {
        font-size: 24px;
        font-weight: bold;
        text-align: center;
        margin-bottom: 20px;
        color: #4CAF50;
    }

    .leaderboard-table {
        width: 100%;
        border-collapse: collapse;
        margin-top: 15px;
    }

    .leaderboard-table th, .leaderboard-table td {
        padding: 15px;
        text-align: center;
        font-size: 18px;
        color: #333;
        border: 1px solid #ddd;
    }

    .leaderboard-table th {
        background-color: #4CAF50;
        color: white;
        font-weight: bold;
    }

    .leaderboard-table tr:nth-child(even) td {
        background-color: #f9f9f9;
    }

    .leaderboard-table tr:hover td {
        background-color: #e3f2fd;
        cursor: pointer;
    }

    .leaderboard-table td {
        background-color: #fafafa;
    }

    /* Adding Hover Effect for Leaderboard Rows */
    .leaderboard-table td:hover {
        background-color: #e3f2fd;
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

        # Adding Query Counter Table with Styling
        query_counter.dataframe(df.style.set_table_styles([
            {'selector': 'thead', 'props': [('background-color', '#4CAF50'), ('color', 'white')]},
            {'selector': 'tbody', 'props': [('background-color', '#fafafa')]},
            {'selector': 'tr:nth-child(even)', 'props': [('background-color', '#f9f9f9')]},
            {'selector': 'th', 'props': [('font-size', '16px'), ('font-weight', 'bold')]},
            {'selector': 'td', 'props': [('font-size', '14px')]}
        ]))
    except KeyboardInterrupt:
        st.warning("Stream ended!")


def leaderboard_view(leaderboard_container):
    try:
        # Fetch real-time data for leaderboard
        con = duckdb.connect(DUCKDB_FILE)
        df = ddb.build_leaderboard(con)  # Fetch leaderboard data
        con.close()

        st.markdown("<h3 class='leaderboard-header'>Leaderboard (Top 10)</h3>", unsafe_allow_html=True)
        leaderboard_data = leaderboard_data.style.set_table_styles([
            {'selector': 'thead', 'props': [('background-color', '#4CAF50'), ('color', 'white')]},
            {'selector': 'tbody', 'props': [('background-color', '#fafafa')]},
            {'selector': 'tr:nth-child(even)', 'props': [('background-color', '#f9f9f9')]},
            {'selector': 'th', 'props': [('font-size', '16px'), ('font-weight', 'bold')]},
            {'selector': 'td', 'props': [('font-size', '14px')]}
        ])
        st.dataframe(leaderboard_data)

    except KeyboardInterrupt:
        st.warning("Stream ended!")


def live_query_distribution(query_distribution):
    try:
        # Fetch real-time data for live query distribution
        con = duckdb.connect(DUCKDB_FILE)
        df = ddb.build_live_query_distribution(con)
        con.close()

        # Adding Query Distribution Table with Styling
        st.markdown("<h3>Query Distribution</h3>", unsafe_allow_html=True)
        query_distribution_data = query_distribution_data.style.set_table_styles([
            {'selector': 'thead', 'props': [('background-color', '#4CAF50'), ('color', 'white')]},
            {'selector': 'tbody', 'props': [('background-color', '#fafafa')]},
            {'selector': 'tr:nth-child(even)', 'props': [('background-color', '#f9f9f9')]},
            {'selector': 'th', 'props': [('font-size', '16px'), ('font-weight', 'bold')]},
            {'selector': 'td', 'props': [('font-size', '14px')]}
        ])
        st.dataframe(query_distribution_data)

    except KeyboardInterrupt:
        st.warning("Stream ended!")


def live_compile_metrics(compile_metrics):
    try:
        # Fetch real-time data for live compile metrics
        con = duckdb.connect(DUCKDB_FILE)
        df = ddb.build_live_compile_metrics(con)
        con.close()

        st.markdown("<h3>Compile Metrics</h3>", unsafe_allow_html=True)
        compile_metrics_data = compile_metrics_data.style.set_table_styles([
            {'selector': 'thead', 'props': [('background-color', '#4CAF50'), ('color', 'white')]},
            {'selector': 'tbody', 'props': [('background-color', '#fafafa')]},
            {'selector': 'tr:nth-child(even)', 'props': [('background-color', '#f9f9f9')]},
            {'selector': 'th', 'props': [('font-size', '16px'), ('font-weight', 'bold')]},
            {'selector': 'td', 'props': [('font-size', '14px')]}
        ])
        st.dataframe(compile_metrics_data)

    except KeyboardInterrupt:
        st.warning("Stream ended!")


def live_spilled_scanned_data(spilled_scanned_data):
    try:
        # Fetch real-time data for live spilled and scanned data
        con = duckdb.connect(DUCKDB_FILE)
        df = ddb.build_live_spilled_scanned(con)
        con.close()

        st.markdown("<h3>Spilled and Scanned Data</h3>", unsafe_allow_html=True)
        spilled_scanned_data = spilled_scanned_data.style.set_table_styles([
            {'selector': 'thead', 'props': [('background-color', '#4CAF50'), ('color', 'white')]},
            {'selector': 'tbody', 'props': [('background-color', '#fafafa')]},
            {'selector': 'tr:nth-child(even)', 'props': [('background-color', '#f9f9f9')]},
            {'selector': 'th', 'props': [('font-size', '16px'), ('font-weight', 'bold')]},
            {'selector': 'td', 'props': [('font-size', '14px')]}
        ])
        st.dataframe(spilled_scanned_data)

    except KeyboardInterrupt:
        st.warning("Stream ended!")



# Show content based on the selected view mode
if view_mode == "Historic View":
    # Arrange Query Counter and Leaderboard in Parallel (Side-by-Side) Layout
    col1, col2 = st.columns(2) 

    with col1:
        # Display query counter in the first column
        query_counter_table = st.empty()  # Placeholder for the query counter table
        Kafka_topic_to_DuckDB()  # Call function to update the query counter table

    with col2:
        # Display the leaderboard in the second column
        st.markdown("<h3 class='leaderboard-header'>Leaderboard</h3>", unsafe_allow_html=True)
        leaderboard_data = ddb.build_leaderboard()  
        st.write(leaderboard_data)

elif view_mode == "Live View":
    # Arrange Query Counter and Leaderboard in Parallel (Side-by-Side) Layout
    col1, col2 = st.columns(2)  

    with col1:
        # Display query counter in the first column for live view
        query_counter_table = st.empty()  
        Kafka_topic_to_DuckDB()  

    with col2:
        # Display the leaderboard in the second column for live view
        st.markdown("<h3 class='leaderboard-header'>Leaderboard</h3>", unsafe_allow_html=True)
        leaderboard_data = ddb.build_leaderboard()  
        st.write(leaderboard_data)

# Footer: 
st.markdown("""
    <footer style="text-align:center; font-size:12px; color:grey; padding-top:20px; border-top: 1px solid #e0e0e0; margin-top:20px;">
        <p>Pipeline Pioneers &copy; 2025 | UTN</p>
    </footer>
""", unsafe_allow_html=True)


hide_st_style = """
<style>
#MainMenu {visibility:hidden;}
footer
header {visibility:hidden;}
</style>
"""
st.markdown(hide_st_style, unsafe_allow_html=True)
