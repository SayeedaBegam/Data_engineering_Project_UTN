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
from sqlalchemy import create_engine

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
def display_metrics():
    total_query = len(df_selection)
    successful_query = len(df_selection[df_selection['was_aborted'] == False])
    aborted_query = len(df_selection[df_selection['was_aborted'] == True])
    cached_query = len(df_selection[df_selection['was_cached'] == True])
    total_scan_mbytes = df_selection['mbytes_scanned'].sum()  # Total MBs scanned
    total_spilled_mbytes = df_selection['mbytes_spilled'].sum()  # Total MBs spilled
    total_joins = df_selection['num_joins'].sum()  # Total joins across all queries
    total_aggregations = df_selection['num_aggregations'].sum()  # Total aggregations across all queries

    # Create a container for the metrics with a subtle color scheme
    st.markdown("### Query Metrics")
    
    # Use st.columns to create layout for metric boxes with a subtle red color and vertical strip
    col1, col2, col3, col4 = st.columns(4)
    
    light_red = "#FFEBEE"  # Light red shade for the box
    dark_red = "#D32F2F"   # Dark red shade for the vertical strip
    
    with col1:
        st.markdown(f"""
            <div style="padding: 20px; background-color: {light_red}; border-left: 10px solid {dark_red}; border-radius: 10px; text-align: center; margin-bottom: 20px;">
                <h5>📊 Total Queries</h5>
                <h3>{total_query}</h3>
            </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown(f"""
            <div style="padding: 20px; background-color: {light_red}; border-left: 10px solid {dark_red}; border-radius: 10px; text-align: center; margin-bottom: 20px;">
                <h5>✅ Successful Queries</h5>
                <h3>{successful_query}</h3>
            </div>
        """, unsafe_allow_html=True)
    
    with col3:
        st.markdown(f"""
            <div style="padding: 20px; background-color: {light_red}; border-left: 10px solid {dark_red}; border-radius: 10px; text-align: center; margin-bottom: 20px;">
                <h5>❌ Aborted Queries</h5>
                <h3>{aborted_query}</h3>
            </div>
        """, unsafe_allow_html=True)
    
    with col4:
        st.markdown(f"""
            <div style="padding: 20px; background-color: {light_red}; border-left: 10px solid {dark_red}; border-radius: 10px; text-align: center; margin-bottom: 20px;">
                <h5>💾 Cached Queries</h5>
                <h3>{cached_query}</h3>
            </div>
        """, unsafe_allow_html=True)

    # New row for additional metrics with space between them
    st.markdown("### Additional Metrics")

    col5, col6 = st.columns(2)
    with col5:
        st.markdown(f"""
            <div style="padding: 20px; background-color: {light_red}; border-left: 10px solid {dark_red}; border-radius: 10px; text-align: center; margin-bottom: 20px;">
                <h5>📊 MBs Scanned</h5>
                <h3>{total_scan_mbytes} MB</h3>
            </div>
        """, unsafe_allow_html=True)
    
    with col6:
        st.markdown(f"""
            <div style="padding: 20px; background-color: {light_red}; border-left: 10px solid {dark_red}; border-radius: 10px; text-align: center; margin-bottom: 20px;">
                <h5>💡 MBs Spilled</h5>
                <h3>{total_spilled_mbytes} MB</h3>
            </div>
        """, unsafe_allow_html=True)

    # Display Total Joins and Aggregations with space between them
    col7, col8 = st.columns(2)
    with col7:
        st.markdown(f"""
            <div style="padding: 20px; background-color: {light_red}; border-left: 10px solid {dark_red}; border-radius: 10px; text-align: center; margin-bottom: 20px;">
                <h5>🔗 Total Joins</h5>
                <h3>{total_joins}</h3>
            </div>
        """, unsafe_allow_html=True)
    
    with col8:
        st.markdown(f"""
            <div style="padding: 20px; background-color: {light_red}; border-left: 10px solid {dark_red}; border-radius: 10px; text-align: center; margin-bottom: 20px;">
                <h5>🔢 Total Aggregations</h5>
                <h3>{total_aggregations}</h3>
            </div>
        """, unsafe_allow_html=True)

# Function to display graphs for Historical View
def historical_view_graphs():
    # Query Type Distribution
    query_type_dist = df_selection.groupby('query_type').size()
    fig_query_type = px.pie(query_type_dist, values=query_type_dist.values, names=query_type_dist.index, title="Query Type Distribution")
    
    # Execution Time Category (Donut Chart)
    bins = [0, 100, 1000, float('inf')]  # Short: 0-100ms, Medium: 101-1000ms, Long: >1000ms
    labels = ['Short', 'Medium', 'Long']
    df_selection['execution_time_category'] = pd.cut(df_selection['execution_duration_ms'], bins=bins, labels=labels, right=False)
    execution_time_dist = df_selection['execution_time_category'].value_counts()
    fig_exec_time_donut = px.pie(execution_time_dist, names=execution_time_dist.index, values=execution_time_dist.values, title="Query Execution Time Distribution", hole=0.3)

    # Hourly Query Count (Line Chart)
    query_count_per_hour = df_selection.groupby(df_selection['arrival_timestamp'].dt.hour).size()
    fig_query_count_hourly = px.line(query_count_per_hour, x=query_count_per_hour.index, y=query_count_per_hour.values, title="Query Count per Hour", labels={'x': 'Hour of Day', 'y': 'Query Count'})

    # Display charts in parallel (side by side)
    col1, col2 = st.columns(2)
    with col1:
        st.plotly_chart(fig_query_type, use_container_width=True, key="fig_query_type")  # Unique key
    with col2:
        st.plotly_chart(fig_exec_time_donut, use_container_width=True, key="fig_exec_time_donut")  # Unique key

    # Display the second row of charts (2 parallel)
    col3, col4 = st.columns(2)
    with col3:
        st.plotly_chart(fig_query_count_hourly, use_container_width=True, key="fig_query_count_hourly")  # Unique key
    with col4:
        st.plotly_chart(fig_query_count_hourly, use_container_width=True, key="fig_query_count_hourly_2")  # Unique key

 

    # Display Cache Hits vs Misses as a Line Chart
    col5, col6 = st.columns(2)
    
    with col5:
        

        # Group by time and count hits and misses per hour
        df_selection['hour'] = df_selection['arrival_timestamp'].dt.hour
        cache_hits_per_hour = df_selection[df_selection['was_cached'] == True].groupby('hour').size()
        cache_misses_per_hour = df_selection[df_selection['was_cached'] == False].groupby('hour').size()

        # Combine cache hits and misses into a single DataFrame for the plot
        cache_stats = pd.DataFrame({
            'Cache Hits': cache_hits_per_hour,
            'Cache Misses': cache_misses_per_hour
        }).fillna(0)  # Fill missing values with 0 for hours with no data

        # Plot line graph
        fig_cache_hits_misses = px.line(cache_stats, x=cache_stats.index, y=cache_stats.columns, 
                                        title="Cache Hits vs Misses Over Time", labels={'x': 'Hour of Day'})
        st.plotly_chart(fig_cache_hits_misses, use_container_width=True)

    # Display Data Spill Distribution
    with col6:
        spill_distribution = df_selection.groupby('mbytes_spilled').size()
        fig_spill_dist = px.bar(spill_distribution, title="Data Spill Distribution")
        st.plotly_chart(fig_spill_dist, use_container_width=True, key="fig_spill_dist")


    
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

# Kafka settings
KAFKA_BROKER = 'localhost:9092'  # Kafka broker address
TOPIC_RAW_DATA = 'parquet-stream'  # Kafka topic name
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

def live_view_graphs():
    """Consume real-time data from Kafka, store it in DuckDB, and visualize it"""
    st.title('Kafka Data Consumer & DuckDB Storage')

    # Create Kafka Consumer
    consumer_raw_data = create_consumer(TOPIC_RAW_DATA, 'raw_data')
    consumer_raw_data.subscribe([TOPIC_RAW_DATA])

    # Streamlit placeholders
    raw_data_placeholder = st.empty()
    graph_placeholder = st.empty()

    # Initialize DuckDB
    initialize_duckdb()

    try:
        st.success("Kafka connection successful! Inserting data into DuckDB...")
        while True:
            # Poll message from Kafka
            raw_msg = consumer_raw_data.poll(timeout=1.0)

            if raw_msg is not None and not raw_msg.error():
                message_value = json.loads(raw_msg.value().decode('utf-8'))

                # Display data in Streamlit
                raw_data_placeholder.write(f"Received from {TOPIC_RAW_DATA}:")
                raw_data_placeholder.dataframe(pd.DataFrame([message_value]))

                # Insert data into DuckDB
                insert_into_duckdb(message_value)

            elif raw_msg and raw_msg.error():
                if raw_msg.error().code() == KafkaError._PARTITION_EOF:
                    st.warning(f"End of partition reached for {TOPIC_RAW_DATA}")
                else:
                    st.error(f"Error in consuming from {TOPIC_RAW_DATA}: {raw_msg.error()}")

            # Fetch and update the graph
            with graph_placeholder:
                plot_query_type_pie_chart()

            time.sleep(3)  # Fetch new data every 3 seconds

    except KeyboardInterrupt:
        st.warning("Streamlit app interrupted. Stopping consumer...")
    except Exception as e:
        st.error(f"An error occurred: {str(e)}")
    finally:
        consumer_raw_data.close()

# Show content based on the selected view mode
if view_mode == "Historical View":
    display_metrics()  # Show the basic metrics
    historical_view_graphs()  # Show the historical view graphs
    stress_level_indicator()  # Add the stress level indicator here
    
elif view_mode == "Live View":
    display_metrics()  # Show the basic metrics
    live_view_graphs()  # Show the live view graphs
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
