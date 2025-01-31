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

# Set up the page layout
st.set_page_config(page_title="Redset Dashboard", page_icon="🌍", layout="wide")
st.header("Redset Dashboard")

# Load the CSV file
csv_file_path = 'organized_sample_0.001.csv'
df = pd.read_csv(csv_file_path)

# Convert 'arrival_timestamp' to datetime if it's not already
df['arrival_timestamp'] = pd.to_datetime(df['arrival_timestamp'], errors='coerce')

# Sidebar configuration
st.sidebar.header("Menu")

# 1. Historical View / Live View Toggle
view_mode = st.sidebar.radio("Select View", ("Historical View", "Live View"))

# 2. File Size Display
file_size = os.path.getsize(csv_file_path)
st.sidebar.write(f"File Size: {numerize.numerize(file_size)}")  # Corrected usage

# 3. Arrival Time Range Slider (only for Historical View)
if view_mode == "Historical View":
    start_date = st.sidebar.date_input("Select Start Date", min_value=df['arrival_timestamp'].min().date(), max_value=df['arrival_timestamp'].max().date())
    end_date = st.sidebar.date_input("Select End Date", min_value=df['arrival_timestamp'].min().date(), max_value=df['arrival_timestamp'].max().date())

    # Filter data based on selected arrival time range for Historical View
    df_selection = df[(df['arrival_timestamp'].dt.date >= start_date) & (df['arrival_timestamp'].dt.date <= end_date)]
else:
    # For Live View, display all records that are recent (for example, last 24 hours)
    now = datetime.now()
    one_day_ago = now - pd.Timedelta(days=1)
    df_selection = df[df['arrival_timestamp'] >= one_day_ago]
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

# Function to display metrics with subtle colors
def initialize_metric_placeholders():
    """Initialize and return placeholders for real-time metric updates."""
    col1, col2, col3, col4 = st.columns(4)
    col5, col6, col7, col8 = st.columns(4)

    return {
        "total_query": col1.empty(),
        "successful_query": col2.empty(),
        "aborted_query": col3.empty(),
        "cached_query": col4.empty(),
        "scan_mbytes": col5.empty(),
        "spilled_mbytes": col6.empty(),
        "total_joins": col7.empty(),
        "total_aggregations": col8.empty()
    }

def display_metrics(df_selection, metric_placeholders):
    """Dynamically update the query metrics in real time with styled boxes."""

    # Fetch latest data
    total_query = len(df_selection)
    successful_query = len(df_selection[df_selection['was_aborted'] == False])
    aborted_query = len(df_selection[df_selection['was_aborted'] == True])
    cached_query = len(df_selection[df_selection['was_cached'] == True])
    total_scan_mbytes = df_selection['mbytes_scanned'].sum()
    total_spilled_mbytes = df_selection['mbytes_spilled'].sum()
    total_joins = df_selection['num_joins'].sum()
    total_aggregations = df_selection['num_aggregations'].sum()

    # Update the placeholders with new values in styled boxes
    metric_placeholders["total_query"].markdown(f"""
        <div style="padding: 20px; background-color: #FFEBEE; border-left: 10px solid #D32F2F; border-radius: 10px; text-align: center; margin-bottom: 20px;">
            <h5>📊 Total Queries</h5>
            <h3>{total_query}</h3>
        </div>
    """, unsafe_allow_html=True)

    metric_placeholders["successful_query"].markdown(f"""
        <div style="padding: 20px; background-color: #E8F5E9; border-left: 10px solid #388E3C; border-radius: 10px; text-align: center; margin-bottom: 20px;">
            <h5>✅ Successful Queries</h5>
            <h3>{successful_query}</h3>
        </div>
    """, unsafe_allow_html=True)

    metric_placeholders["aborted_query"].markdown(f"""
        <div style="padding: 20px; background-color: #FFEBEE; border-left: 10px solid #D32F2F; border-radius: 10px; text-align: center; margin-bottom: 20px;">
            <h5>❌ Aborted Queries</h5>
            <h3>{aborted_query}</h3>
        </div>
    """, unsafe_allow_html=True)

    metric_placeholders["cached_query"].markdown(f"""
        <div style="padding: 20px; background-color: #E3F2FD; border-left: 10px solid #1976D2; border-radius: 10px; text-align: center; margin-bottom: 20px;">
            <h5>💾 Cached Queries</h5>
            <h3>{cached_query}</h3>
        </div>
    """, unsafe_allow_html=True)

    metric_placeholders["scan_mbytes"].markdown(f"""
        <div style="padding: 20px; background-color: #FFF3E0; border-left: 10px solid #F57C00; border-radius: 10px; text-align: center; margin-bottom: 20px;">
            <h5>📊 MBs Scanned</h5>
            <h3>{total_scan_mbytes} MB</h3>
        </div>
    """, unsafe_allow_html=True)

    metric_placeholders["spilled_mbytes"].markdown(f"""
        <div style="padding: 20px; background-color: #FBE9E7; border-left: 10px solid #E53935; border-radius: 10px; text-align: center; margin-bottom: 20px;">
            <h5>💡 MBs Spilled</h5>
            <h3>{total_spilled_mbytes} MB</h3>
        </div>
    """, unsafe_allow_html=True)

    metric_placeholders["total_joins"].markdown(f"""
        <div style="padding: 20px; background-color: #E8F5E9; border-left: 10px solid #2E7D32; border-radius: 10px; text-align: center; margin-bottom: 20px;">
            <h5>🔗 Total Joins</h5>
            <h3>{total_joins}</h3>
        </div>
    """, unsafe_allow_html=True)

    metric_placeholders["total_aggregations"].markdown(f"""
        <div style="padding: 20px; background-color: #F3E5F5; border-left: 10px solid #8E24AA; border-radius: 10px; text-align: center; margin-bottom: 20px;">
            <h5>🔢 Total Aggregations</h5>
            <h3>{total_aggregations}</h3>
        </div>
    """, unsafe_allow_html=True)



# Kafka settings
KAFKA_BROKER = 'localhost:9092'  # Kafka broker address
TOPIC_RAW_DATA = 'parquet_stream'  # Kafka topic name
TOPIC_COMPILE_DURATION = 'compile_duration_sort'  # Kafka topic name for sorted durations
TOPIC_QUERY_COUNTER = 'query_counter'  # Kafka topic name for query counters

# DuckDB database file
DUCKDB_FILE = "data.duckdb"
TABLE_NAME = "raw_data"

def create_consumer(topic, group_id):
    """Create a Confluent Kafka Consumer."""
    return Consumer({
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': group_id,
        'auto.offset.reset': 'earliest',  # Start reading from the beginning
        'enable.auto.commit': False       # Automatically commit offsets
    })

def initialize_duckdb():
    """Create a table in DuckDB if it does not exist"""
    con = duckdb.connect(DUCKDB_FILE)
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
    con.close()


# Create a persistent DuckDB connection
con = duckdb.connect(DUCKDB_FILE)

def insert_into_duckdb(data):
    """Insert Kafka message into DuckDB using direct connection"""
    try:
        df = pd.DataFrame([data])  # Convert data to DataFrame
        con.execute(f"INSERT INTO {TABLE_NAME} SELECT * FROM df")
        print("Data inserted successfully into DuckDB")
    except Exception as e:
        print(f"Error inserting data into DuckDB: {e}")



def fetch_duckdb_data():
    """Fetch the latest data from DuckDB"""
    con = duckdb.connect(DUCKDB_FILE)
    df = con.execute(f"SELECT * FROM {TABLE_NAME} ORDER BY arrival_timestamp DESC LIMIT 100").fetchdf()

    con.close()
    return df

def plot_query_type_pie_chart():
    """Visualize distribution of query types in a pie chart"""
    df = fetch_duckdb_data()
    if df.empty:
        st.warning("No data available yet.")
        return

    # Count the occurrence of each query_type
    query_type_counts = df['query_type'].value_counts().reset_index(name='count')
    query_type_counts.columns = ['Query Type', 'Count']

    # Plot the pie chart
    fig = px.pie(query_type_counts,
                 names='Query Type',
                 values='Count',
                 title="Query Type Distribution")
    st.plotly_chart(fig, use_container_width=True)


def plot_query_type_pie_chart():
    """Create a pie chart for the query type distribution"""
    # Fetch query type counts from the Temp table
    query_counts = con.execute("SELECT query_type, COUNT(*) as count FROM Temp GROUP BY query_type").fetchdf()
    
    # Create the pie chart
    if not query_counts.empty:
        fig = px.pie(query_counts, names='query_type', values='count', title='Query Type Distribution')
        return fig  # Return the figure instead of displaying it here
    else:
        return None  # Return None if no data



def initialize_chart_placeholders():
    """Initialize and return placeholders for real-time chart updates."""
    col1, col2 = st.columns(2)  # Two columns for parallel charts
    col3, col4 = st.columns(2)

    return {
        "recent_queries": col1.empty(),
        "bar_chart": col2.empty(),
        "stress_chart": col3.empty(),
        "pie_chart": col4.empty(),
        "scatter_chart": col1.empty()  # reuse col1, adjust as needed
    }

def update_visualizations(df_latest, recent_queries_placeholder, pie_chart_placeholder, 
                          bar_chart_placeholder, scatter_chart_placeholder, stress_chart_placeholder):
    """Update Streamlit visualizations based on the latest data."""
    
    # Get a unique identifier based on the current time and a random number
    unique_key_suffix = f"{int(time.time())}_{random.randint(0, 1000000)}"

    # Update recent queries
    recent_queries_placeholder.write("### Recent Queries (Last 100)")
    recent_queries_placeholder.dataframe(df_latest)

    # Query 1: Query Type Distribution (Pie Chart)
    query_counts = df_latest['query_type'].value_counts().reset_index(name='count')
    if not query_counts.empty:
        fig_pie = px.pie(query_counts, names='query_type', values='count', title='Query Type Distribution')
        pie_chart_placeholder.plotly_chart(fig_pie, use_container_width=True, key=f"pie_chart_{unique_key_suffix}")

    # Query 2: Total Joins, Scans, Aggregations (Bar Chart)
    query_metrics = df_latest[['num_joins', 'num_scans', 'num_aggregations']].sum()
    if query_metrics.any():
        fig_bar = px.bar(query_metrics, title='Joins, Scans, and Aggregations')
        bar_chart_placeholder.plotly_chart(fig_bar, use_container_width=True, key=f"bar_chart_{unique_key_suffix}")

    # Query 3: Execution Duration Scatter Plot
    execution_data = df_latest[['compile_duration_ms', 'execution_duration_ms']]
    if not execution_data.empty:
        fig_scatter = px.scatter(execution_data, x='compile_duration_ms', y='execution_duration_ms', 
                                 title='Execution vs Compile Time')
        scatter_chart_placeholder.plotly_chart(fig_scatter, use_container_width=True, key=f"scatter_chart_{unique_key_suffix}")

    # Query 4: Stress Level Gauge Chart
    stress_data = df_latest[['execution_duration_ms', 'queue_duration_ms', 'compile_duration_ms']].mean()
    if not stress_data.empty:
        avg_execution = stress_data['execution_duration_ms']
        avg_queue = stress_data['queue_duration_ms']
        avg_compile = stress_data['compile_duration_ms']
        avg_stress = (avg_execution + avg_queue + avg_compile) / 3
        fig_gauge = go.Figure(go.Indicator(
            mode="gauge+number",
            value=avg_stress,
            title={"text": "Average Stress Level (ms)"},
            gauge={"axis": {"range": [0, 1000]}}
        ))
        stress_chart_placeholder.plotly_chart(fig_gauge, use_container_width=True, key=f"stress_chart_{unique_key_suffix}")




# Assume df_latest is the filtered data that you want to pass to the display_metrics function
def historical_live_view_graphs():
    """Consume real-time data from Kafka, store it in DuckDB, and visualize it"""
    st.title('Real-Time Data & Metrics')

    # Kafka Consumer Initialization
    consumer_raw_data = create_consumer(TOPIC_RAW_DATA, 'raw_data_group')
    consumer_raw_data.subscribe([TOPIC_RAW_DATA])


    # Initialize DuckDB (Ensure Table Exists)
    initialize_duckdb()

    # Initialize metric placeholders
    st.markdown("### Query Metrics")
    metric_placeholders = initialize_metric_placeholders()

    st.markdown("### live chart")
    # Streamlit placeholders for data and graphs
    # Initialize chart placeholders using the function
    chart_placeholders = initialize_chart_placeholders()


    try:
        st.success("Connected to Kafka! Streaming Data...")
        while True:
            raw_msg = consumer_raw_data.poll(timeout=1.0)

            if raw_msg is not None and not raw_msg.error():
                message_value = json.loads(raw_msg.value().decode('utf-8'))
                time.sleep(3)
                insert_into_duckdb(message_value)  # Store in DuckDB

                # Fetch real-time sorted data
                df_latest = fetch_duckdb_data()

                # Update metrics
                display_metrics(df_latest, metric_placeholders)


                # Update visualizations
                update_visualizations(df_latest, 
                                      chart_placeholders["recent_queries"], 
                                      chart_placeholders["pie_chart"], 
                                      chart_placeholders["bar_chart"], 
                                      chart_placeholders["scatter_chart"], 
                                      chart_placeholders["stress_chart"])

    except KeyboardInterrupt:
        st.warning("Stream ended!")



# Function to create a stress level indicator (gauge) at the bottom
def stress_level_indicator():
    # Calculate the stress level (for example, based on the average execution duration)
    avg_execution_time = df_selection['execution_duration_ms'].mean()

    # Define the max and min range of the stress level gauge
    max_value = 10000  # Max execution duration (in ms), adjust as necessary
    min_value = 0  # Min execution duration (in ms)

    # Create a gauge chart for the stress level
    fig_stress_level = go.Figure(go.Indicator(
        mode="gauge+number+delta",
        value=avg_execution_time,  # Set the average execution time as the value
        title={"text": "System Stress Level"},
        gauge={
            "axis": {"range": [min_value, max_value]},
            "bar": {"color": "red"},
            "steps": [
                {"range": [0, max_value * 0.33], "color": "green"},
                {"range": [max_value * 0.33, max_value * 0.66], "color": "yellow"},
                {"range": [max_value * 0.66, max_value], "color": "red"}
            ],
        },
        delta={"reference": max_value * 0.5}  # Reference point for delta (could be your desired "normal" value)
    ))
    
    # Display the stress level indicator at the bottom of the page
    st.plotly_chart(fig_stress_level, use_container_width=True)


# Show content based on the selected view mode
if view_mode == "Historical View":
    #display_metrics()  # Show the basic metrics
    historical_live_view_graphs()  # Show the historical view graphs
    stress_level_indicator()  # Add the stress level indicator here

elif view_mode == "Live View":
    #display_metrics()  # Show the basic metrics
    historical_live_view_graphs()  # Show the live view graphs
    stress_level_indicator()  # Add the stress level indicator here

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
