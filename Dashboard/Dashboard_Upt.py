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
COMPILE_COLUMNS = ['instance_id','num_joins','num_scans','num_aggregations','mbytes_scanned', 'mbytes_spilled']
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
    consumer = Consumer({
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': group_id,
        'auto.offset.reset': 'earliest',  # Read from the beginning if no offset found
        'enable.auto.commit': False,       # Enable automatic commit
        'enable.partition.eof': False,    # Avoid EOF issues
        })
    consumer.subscribe([topic])
    return consumer


def Kafka_topic_to_DuckDB():
    consumer_raw_data = create_consumer(TOPIC_RAW_DATA, 'raw_data')
    consumer_leaderboard = create_consumer(TOPIC_LEADERBOARD, 'live_analytics')
    consumer_query_counter = create_consumer(TOPIC_QUERY_METRICS, 'live_analytics')
    consumer_compile = create_consumer(TOPIC_COMPILE_METRICS, 'live_analytics')
    #consumer_stress = create_consumer(TOPIC_STRESS_INDEX, 'live_analytics')
    initialize_duckdb()

    con = duckdb.connect(DUCKDB_FILE)

    print(f"Listening for messages on topic '{TOPIC_RAW_DATA}'...")
    figure_keys = ["fig1", "fig2", "fig3", "fig4", "fig5", "fig6"]
    for key in figure_keys:
        if key not in st.session_state:
            st.session_state[key] = go.Figure()

    fig1_placeholder = st.empty()
    fig2_placeholder = st.empty()
    fig3_placeholder = st.empty()
    fig4_placeholder = st.empty()
    fig5_placeholder = st.empty()
    fig6_placeholder = st.empty()

    try:
        while True:
            # load new data from Kafka
            ddb.parquet_to_table(consumer_query_counter, 'LIVE_QUERY_METRICS', con, QUERY_COLUMNS, TOPIC_QUERY_METRICS)
            ddb.parquet_to_table(consumer_leaderboard, 'LIVE_LEADERBOARD', con, LEADERBOARD_COLUMNS, TOPIC_LEADERBOARD)
            ddb.parquet_to_table(consumer_compile, 'LIVE_COMPILE_METRICS', con, COMPILE_COLUMNS, TOPIC_COMPILE_METRICS)
            # use session state to prevent flashin dashboard
            st.session_state.fig1 = build_leaderboard_compiletime(con)
            st.session_state.fig2 = build_leaderboard_user_queries(con)
            st.session_state.fig3 = build_live_query_counts(con)
            st.session_state.fig4 = build_live_query_distribution(con)
            st.session_state.fig5 = build_live_compile_metrics(con)
            st.session_state.fig6 = build_live_spilled_scanned(con)
            uniq_id = str(int(time.time()))
            with st.container():
                col1, col2, col3 = st.columns(3)

                with col1:
                    fig1_placeholder.plotly_chart(st.session_state.fig1, use_container_width=True,key=f"fig1_chart_{uniq_id}")
                    fig2_placeholder.plotly_chart(st.session_state.fig2, use_container_width=True,key=f"fig2_chart_{uniq_id}")

                with col2:
                    fig3_placeholder.plotly_chart(st.session_state.fig3, use_container_width=True,key=f"fig3_chart_{uniq_id}")
                    fig4_placeholder.plotly_chart(st.session_state.fig4, use_container_width=True,key=f"fig4_chart_{uniq_id}")

                with col3:
                    fig5_placeholder.plotly_chart(st.session_state.fig5, use_container_width=True,key=f"fig5_chart_{uniq_id}")
                    fig6_placeholder.plotly_chart(st.session_state.fig6, use_container_width=True,key=f"fig6_chart_{uniq_id}")
            # Wait before fetching new updates
            time.sleep(5)

    except KeyboardInterrupt:
        print("\nStopping consumer...")
    finally:
        consumer_raw_data.close()
        consumer_leaderboard.close()
        consumer_query_counter.close()
        consumer_compile.close()
        #consumer_stress.close()


########### DuckDB to Dashboard ################

def build_leaderboard_compiletime(con):
    '''
    PREREQUISITES: parquet_to_table(consumer,'LIVE_LEADERBOARD', 
    con,LEADERBOARD_COLUMNS,TOPIC_LEADERBOARD) has already been called
    
    returns dataframe containing top 10 compile times and their instance_id
    '''
    df1 = con.execute(f"""
    SELECT 
    instance_id, 
    compile_duration_ms as compile_duration,
    FROM LIVE_LEADERBOARD
    ORDER BY compile_duration_ms DESC
    LIMIT 10;
    """).df()

    df1['formatted_compile_time'] = df1['compile_duration'].apply(
        lambda x: f"{int(x // 60000)}:{int((x % 60000) // 1000):02d}"
    )

    # Add rank column for leaderboard display
    df1.insert(0, "Rank", range(1, len(df1) + 1))

    fig = go.Figure(data=[go.Table(
        columnwidth=[5, 10, 10],  # Adjust column widths for better layout
        header=dict(values=["Rank", "Instance ID", "Compile Duration (mm:ss)"],
                    fill_color="royalblue",
                    font=dict(color="white", size=14),
                    align="center"),
        cells=dict(values=[df1["Rank"], df1["instance_id"], df1["formatted_compile_time"]],
                   fill_color="black",
                   font=dict(color="white", size=12),
                   align="center"))
    ])

    fig.update_layout(
        title="Leaderboard: Top 10 Longest Compile Times",
        template="plotly_dark"
    )

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
            query_type, 
            COUNT(*) AS occurrence_count
        FROM LIVE_QUERY_METRICS
        GROUP BY query_type
        ORDER BY occurrence_count DESC;
    """).df()

    # Visualization: Bar Chart using Plotly
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=df['query_type'],
        y=df['occurrence_count'],
        marker=dict(color=px.colors.qualitative.Plotly),  # Dynamic colors
        text=df['occurrence_count'],  # Show counts on bars
        textposition='auto'
    ))

    fig.update_layout(
        title='Query Type Distribution',
        xaxis_title='Query Type',
        yaxis_title='Occurrences',
        template='plotly_dark'
    )

    return fig

def build_live_query_distribution(con):
    '''
    PREREQUISITES: parquet_to_table(consumer,'LIVE_QUERY_METRICS', 
    con,QUERY_COLUMNS,TOPIC_QUERY_METRICS) has already been called.
    
    Generates a Pie Chart representing the distribution of query types.
    '''
    df = con.execute(f"""
        SELECT 
            query_type, 
            COUNT(*) AS occurrence_count
        FROM LIVE_QUERY_METRICS
        GROUP BY query_type
        ORDER BY occurrence_count DESC;
    """).df()

    # Check if the DataFrame is empty
    if df.empty:
        st.warning("No query data available.")
        return go.Figure()  # Return an empty figure

    # Visualization: Pie Chart using Plotly
    fig = go.Figure(data=[go.Pie(
        labels=df['query_type'], 
        values=df['occurrence_count'], 
        hole=0.3,  # Donut Chart effect
        marker=dict(colors=px.colors.qualitative.Plotly),  # Dynamic colors
        textinfo='label+percent'  # Show both label and percentage
    )])

    fig.update_layout(
        title='Query Type Distribution',
        template='plotly_dark'
    )

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
    SUM(num_aggregations) AS total_aggregations,
    SUM(num_joins) AS total_joins
    FROM LIVE_COMPILE_METRICS;
    """).df()

    # Visualization: Stacked Bar Chart using Plotly
    fig = go.Figure(data=[
        go.Bar(name='Scans', x=['Metrics'], y=[df['total_scans'][0]], marker=dict(color='blue')),
        go.Bar(name='Aggregates', x=['Metrics'], y=[df['total_aggregations'][0]], marker=dict(color='orange')),
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
    SUM(mbytes_spilled) AS mb_spilled,
    SUM(mbytes_scanned) AS mb_scanned
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
