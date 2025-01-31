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
import plotly.graph_objects as go
from plotly.subplots import make_subplots

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
    # LEADERBOARD_COLUMNS = ['instance_id','query_id','user_id','arrival_timestamp','compile_duration_ms']
    con.execute(f"""
    CREATE TABLE IF NOT EXISTS LIVE_LEADERBOARD (
        instance_id BIGINT,
        query_id BIGINT,
        user_id BIGINT,
        arrival_timestamp TIMESTAMP,
        compile_duration_ms DOUBLE,
    )
    """)
    # COMPILE_COLUMNS = ['instance_id','num_joins','num_scans','num_aggregations','mbytes_scanned', 'mbytes_spilled']
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS LIVE_COMPILE_METRICS (
            instance_id BIGINT,            
            num_joins BIGINT,
            num_scans BIGINT,
            num_aggregations BIGINT,
            mbytes_scanned DOUBLE,
            mbytes_spilled DOUBLE,
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
    consumer_leaderboard = create_consumer(TOPIC_LEADERBOARD, 'live_analytics')
    consumer_query_counter = create_consumer(TOPIC_QUERY_METRICS, 'live_analytics')
    consumer_compile = create_consumer(TOPIC_COMPILE_METRICS, 'live_analytics')
    #consumer_stress = create_consumer(TOPIC_STRESS_INDEX, 'live_analytics')
    initialize_duckdb()

    con = duckdb.connect(DUCKDB_FILE)

    print(f"Listening for messages on topic '{TOPIC_RAW_DATA}'...")

    query_counter_table = st.empty()

    try:
        while True:
            ddb.parquet_to_table(consumer_query_counter,'LIVE_QUERY_METRICS',con, QUERY_COLUMNS,TOPIC_QUERY_METRICS)
            #ddb.parquet_to_table(consumer_leaderboard,'LIVE_LEADERBOARD',con, LEADERBOARD_COLUMNS,TOPIC_LEADERBOARD)
            #ddb.parquet_to_table(consumer_compile,'LIVE_COMPILE_METRICS',con, COMPILE_COLUMNS,TOPIC_COMPILE_METRICS)
            #fig1 = build_leaderboard_compiletime(con)
            #fig2 = build_leaderboard_user_queries(con)
            fig3 = build_live_query_counts(con)
            fig4 = build_live_query_distribution(con)
            #fig5 = build_live_compile_metrics(con)
            #fig6 = build_live_spilled_scanned(con)

            with query_counter_table:
                #st.plotly_chart(fig1)
                #st.plotly_chart(fig2)
                st.plotly_chart(fig3)
                st.plotly_chart(fig4)
                #st.plotly_chart(fig5)
                #st.plotly_chart(fig6)

            time.sleep(5)
            st.rerun()  # Force Streamlit to re-run so it re-queries and re
            ddb.check_duckdb_table('LIVE_QUERY_METRICS',con)

    except KeyboardInterrupt:
        print("\nStopping consumer...")
    finally:
        consumer_raw_data.close()
        consumer_leaderboard.close()
        consumer_query_counter.close()
        consumer_compile.close()
        #consumer_stress.close()


########### DuckDB to Dashboard ################
import plotly.graph_objects as go

def build_leaderboard_compiletime(con):
    '''
    PREREQUISITES: parquet_to_table(consumer,'LIVE_LEADERBOARD', 
    con,LEADERBOARD_COLUMNS,TOPIC_LEADERBOARD) has already been called
    
    returns dataframe containing top 10 compile times and their instance_id
    '''
    df1 = con.execute(f"""
    SELECT 
    instance_id, 
    FLOOR(compile_duration_ms / 60000) || ':' || LPAD(FLOOR((compile_duration_ms % 60000) / 1000), 2, '0') AS compile_duration
    FROM LIVE_LEADERBOARD
    ORDER BY compile_duration_ms DESC
    LIMIT 10;
    """).df()

    # Visualization: Horizontal Bar Chart using Plotly
    fig = go.Figure()
    fig.add_trace(go.Bar(
        y=df1['instance_id'],
        x=df1['compile_duration'],
        orientation='h',  # Horizontal bar
        marker=dict(color='royalblue'),
        text=df1['compile_duration'],
        textposition='inside'
    ))

    fig.update_layout(
        title='Top 10 Compile Times',
        xaxis_title='Compile Duration (mm:ss)',
        yaxis_title='Instance ID',
        template='plotly_dark'
    )
    #fig.show()
    return fig

def build_leaderboard_user_queries(con):
    '''
    PREREQUISITES: parquet_to_table(consumer,'LIVE_LEADERBOARD', 
    con,LEADERBOARD_COLUMNS,TOPIC_LEADERBOARD) has already been called
    
    returns dataframe containing top 5 user_ids who issued the most queries
    '''
    df = con.execute(f"""
                       SELECT user_id, COUNT(*) as most_queries
                       FROM LIVE_LEADERBOARD
                       GROUP BY user_id
                       ORDER BY most_queries DESC
                       LIMIT 5;
                       """).df()

    # Visualization: Vertical Bar Chart using Plotly
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=df['user_id'],
        y=df['most_queries'],
        marker=dict(color='green'),
        text=df['most_queries'],
        textposition='auto'
    ))

    fig.update_layout(
        title='Top 5 Users by Query Count',
        xaxis_title='User ID',
        yaxis_title='Query Count',
        template='plotly_dark'
    )
    #fig.show()
    return fig


def build_live_query_counts(con):
    '''
    PREREQUISITES: parquet_to_table(consumer,'LIVE_QUERY_METRICS', 
    con,QUERY_COLUMNS,TOPIC_QUERY_METRICS) has already been called
    
    returns dataframe containing total, aborted, and cached query counts
    '''
    df = con.execute(f"""
    SELECT 
    COUNT(*) AS total_queries,
    SUM(CASE WHEN was_aborted = TRUE THEN 1 ELSE 0 END) AS aborted_queries,
    SUM(CASE WHEN was_cached = TRUE THEN 1 ELSE 0 END) AS cached_queries
    FROM LIVE_QUERY_METRICS;
    """).df()

    # Visualization: Pie Chart using Plotly
    fig = go.Figure(data=[go.Pie(
        labels=['Total Queries', 'Aborted Queries', 'Cached Queries'],
        values=[df['total_queries'][0], df['aborted_queries'][0], df['cached_queries'][0]],
        hole=0.3,  # makes it a donut chart
        marker=dict(colors=['#007bff', '#ff4c4c', '#00cc99'])
    )])

    fig.update_layout(
        title='Query Distribution (Total, Aborted, Cached)',
        template='plotly_dark'
    )
    #fig.show()
    return fig

def build_live_query_distribution(con):
    '''
    PREREQUISITES: parquet_to_table(consumer,'LIVE_QUERY_METRICS', 
    con,QUERY_COLUMNS,TOPIC_QUERY_METRICS) has already been called
    
    returns dataframe containing counts of total, aborted, and cached queries
    '''
    df = con.execute(f"""
    SELECT 
    query_type, 
    COUNT(*) AS occurrence_count
    FROM LIVE_QUERY_METRICS
    GROUP BY query_type
    ORDER BY occurrence_count DESC;
    """).df()

    # Visualization: Stacked Bar Chart using Plotly
    fig = go.Figure(data=[
        go.Bar(name='Total Queries', x=['Queries'], y=[df['total_queries'][0]], marker=dict(color='#007bff')),
        go.Bar(name='Aborted Queries', x=['Queries'], y=[df['aborted_queries'][0]], marker=dict(color='#ff4c4c')),
        go.Bar(name='Cached Queries', x=['Queries'], y=[df['cached_queries'][0]], marker=dict(color='#00cc99'))
    ])

    fig.update_layout(
        title='Query Distribution (Total, Aborted, Cached)',
        barmode='stack',
        xaxis_title='Query Type',
        yaxis_title='Query Count',
        template='plotly_dark'
    )
    #fig.show()
    return fig

def build_live_compile_metrics(con):
    '''
    PREREQUISITES: parquet_to_table(consumer,'LIVE_QUERY_METRICS', 
    con,QUERY_COLUMNS,TOPIC_QUERY_METRICS) has already been called
    
    returns dataframe containing sum of scans, aggregates, and joins
    '''
    df = con.execute(f"""
    SELECT 
    SUM(num_scans) AS total_scans,
    SUM(num_aggregates) AS total_aggregates,
    SUM(num_join) AS total_joins
    FROM LIVE_COMPILE_METRICS;
    """).df()

    # Visualization: Stacked Bar Chart using Plotly
    fig = go.Figure(data=[
        go.Bar(name='Scans', x=['Metrics'], y=[df['total_scans'][0]], marker=dict(color='blue')),
        go.Bar(name='Aggregates', x=['Metrics'], y=[df['total_aggregates'][0]], marker=dict(color='orange')),
        go.Bar(name='Joins', x=['Metrics'], y=[df['total_joins'][0]], marker=dict(color='green'))
    ])

    fig.update_layout(
        title='Compile Metrics (Scans, Aggregates, Joins)',
        barmode='stack',
        xaxis_title='Metric Type',
        yaxis_title='Count',
        template='plotly_dark'
    )
    #fig.show()
    return fig


def build_live_spilled_scanned(con):
    '''
    PREREQUISITES: parquet_to_table(consumer,'LIVE_QUERY_METRICS', 
    con,QUERY_COLUMNS,TOPIC_QUERY_METRICS) has already been called
    
    returns dataframe containing sum of spilled and scanned data
    '''
    df = con.execute(f"""
    SELECT 
    SUM(mb_spilled) AS mb_spilled,
    SUM(mb_scanned) AS mb_scanned
    FROM LIVE_COMPILE_METRICS;
    """).df()

    # Visualization: Dual-Axis Bar Chart using Plotly
    fig = go.Figure()

    fig.add_trace(go.Bar(
        x=['Spilled vs Scanned'],
        y=[df['mb_spilled'][0]],
        name='Spilled (MB)',
        marker=dict(color='red')
    ))

    fig.add_trace(go.Bar(
        x=['Spilled vs Scanned'],
        y=[df['mb_scanned'][0]],
        name='Scanned (MB)',
        marker=dict(color='blue')
    ))

    fig.update_layout(
        title='Data Spilled vs Data Scanned',
        barmode='group',
        xaxis_title='Metrics',
        yaxis_title='MB',
        template='plotly_dark'
    )
    #fig.show()
    return fig

if view_mode == "Historical View":
    Kafka_topic_to_DuckDB()

elif view_mode == "Live View":
    Kafka_topic_to_DuckDB()

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
