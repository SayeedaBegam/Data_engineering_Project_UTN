import pandas as pd
import plotly.graph_objects as go
import streamlit as st
import time
from confluent_kafka import Consumer, KafkaError
import json
import threading
from datetime import datetime

# PAGE SETUP
st.set_page_config(page_title="Redset Dashboard", page_icon=":bar_chart:", layout="wide")

# Adding a smooth pastel gradient background with CSS
st.markdown(
    """
    <style>
    body {
        background: linear-gradient(45deg, #FDCB82, #A6D0E4, #B497BD, #E4D1B9);
        color: #333333;
        font-family: 'Arial', sans-serif;
    }
    header {
        background: rgba(255, 255, 255, 0.9);
    }
    .stApp {
        background: rgba(255, 255, 255, 0.8);
        border-radius: 15px;
        padding: 20px;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

st.title("Redset Dashboard")
st.markdown("*Prototype v1.1*: Live Dashboard")

# Initialize a global dataframe
df = pd.DataFrame(columns=["arrival_timestamp", "was_aborted", "was_cached", "execution_duration_ms", "compile_duration_ms", "query_type", "num_permanent_tables_accessed", "num_external_tables_accessed", "num_system_tables_accessed"])

# Kafka Settings
KAFKA_BROKER = 'localhost:9092'  # Kafka broker address
TOPIC_RAW_DATA = 'parquet-stream'  # Kafka topic name
TOPIC_COMPILE_DURATION = 'compile_duration_sort'  # Kafka topic name for sorted durations
TOPIC_QUERY_COUNTER = 'query_counter'  # Kafka topic name for query counters

def create_consumer(topic, group_id):
    """Create a Confluent Kafka Consumer."""
    return Consumer({
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': group_id,
        'auto.offset.reset': 'earliest',  # Start reading from the beginning
        'enable.auto.commit': False       # Automatically commit offsets
    })

# Function to consume Kafka data for all topics
def consume_kafka_data(consumer_raw_data, consumer_compile_duration, consumer_query_counter):
    """Fetch messages from the Kafka topics."""
    consumer_raw_data.subscribe([TOPIC_RAW_DATA])
    consumer_compile_duration.subscribe([TOPIC_COMPILE_DURATION])
    consumer_query_counter.subscribe([TOPIC_QUERY_COUNTER])

    while True:
        raw_msg = consumer_raw_data.poll(timeout=1.0)
        compile_msg = consumer_compile_duration.poll(timeout=1.0)
        query_msg = consumer_query_counter.poll(timeout=1.0)

        if raw_msg is not None and not raw_msg.error():
            message_value = json.loads(raw_msg.value().decode('utf-8'))
            yield ('raw_data', message_value)

        elif compile_msg is not None and not compile_msg.error():
            message_value = json.loads(compile_msg.value().decode('utf-8'))
            yield ('compile_duration', message_value)

        elif query_msg is not None and not query_msg.error():
            message_value = json.loads(query_msg.value().decode('utf-8'))
            yield ('query_counter', message_value)

# Function to update the dashboard with new data
def update_dashboard(new_data):
    global df
    df = pd.concat([df, new_data], ignore_index=True)
    
    # Filter out the data based on the latest timestamps
    latest_timestamp = df['arrival_timestamp'].max()
    filtered_df = df[df['arrival_timestamp'] == latest_timestamp]

    # Get batch size and arrival timestamp
    batch_size = len(new_data)
    arrival_time = latest_timestamp

    # Display batch size and timestamp
    st.write(f"New Batch Received - Timestamp: {arrival_time}, Batch Size: {batch_size}")

    # Update all the plots and metrics with the new data
    update_metrics(filtered_df)

# Function to update query execution summary metrics and visualizations
def update_metrics(filtered_df):
    # Calculate query metrics based on the filtered data
    total_queries = len(filtered_df)
    completed_queries = len(filtered_df[filtered_df["was_aborted"] == 0])
    aborted_queries = len(filtered_df[filtered_df["was_aborted"] == 1])
    cached_queries = len(filtered_df[filtered_df["was_cached"] == 1])

    # Create a table and graph in parallel
    col1, col2 = st.columns(2)

    with col1:
        query_summary = pd.DataFrame(
            {
                "Metric": ["Total Queries", "Completed Queries", "Aborted Queries", "Cached Queries"],
                "Count": [total_queries, completed_queries, aborted_queries, cached_queries],
            }
        )
        st.table(query_summary)

    with col2:
        # Create a Bar Chart for Query Execution Metrics
        query_metrics = ["Total Queries", "Completed Queries", "Aborted Queries", "Cached Queries"]
        query_values = [total_queries, completed_queries, aborted_queries, cached_queries]

        fig_bar = go.Figure()
        fig_bar.add_trace(go.Bar(x=query_metrics, y=query_values, name="Query Metrics"))
        fig_bar.update_layout(
            title="Query Execution Metrics",
            xaxis_title="Query Type",
            yaxis_title="Count",
            template="plotly_dark"
        )
        st.plotly_chart(fig_bar, use_container_width=True)

    # Display more graphs with the updated data (like average response time, etc.)
    update_additional_graphs(filtered_df)

# Function to update other analyses
def update_additional_graphs(filtered_df):
    # Example: Average Response Time for Cached vs Non-Cached Queries
    cached_avg = filtered_df[filtered_df['was_cached'] == 1]['execution_duration_ms'].mean()
    non_cached_avg = filtered_df[filtered_df['was_cached'] == 0]['execution_duration_ms'].mean()

    fig_avg_response = go.Figure()
    fig_avg_response.add_trace(go.Bar(x=['Cached', 'Non-Cached'], y=[cached_avg, non_cached_avg], name="Avg Execution Time (ms)"))
    fig_avg_response.update_layout(title="Average Response Time (Cached vs Non-Cached)", yaxis_title="Execution Duration (ms)")

    st.plotly_chart(fig_avg_response, use_container_width=True)

# Threaded Kafka Consumer
def start_kafka_stream():
    # Create Kafka consumers for each topic
    consumer_raw_data = create_consumer(TOPIC_RAW_DATA, 'raw_data_group')
    consumer_compile_duration = create_consumer(TOPIC_COMPILE_DURATION, 'compile_group')
    consumer_query_counter = create_consumer(TOPIC_QUERY_COUNTER, 'query_group')

    try:
        # Consume Kafka data in a separate thread
        for topic, message in consume_kafka_data(consumer_raw_data, consumer_compile_duration, consumer_query_counter):
            new_data = pd.DataFrame([message])
            update_dashboard(new_data)

    except Exception as e:
        st.error(f"Error in consuming Kafka data: {str(e)}")

    finally:
        # Close the consumers
        consumer_raw_data.close()
        consumer_compile_duration.close()
        consumer_query_counter.close()

# Display the play button for live data processing
if st.button("Start Streaming Data"):
    st.info("Starting live data stream...", icon="⚡")
    
    # Start the Kafka consumer in a separate thread for live data fetching
    kafka_thread = threading.Thread(target=start_kafka_stream)
    kafka_thread.daemon = True  # Daemonize the thread so it exits when the main program exits
    kafka_thread.start()

    # Display placeholder while data is being processed
    with st.empty():
        while True:
            st.write("Streaming live data...")
            time.sleep(1)  # Sleep to prevent continuous looping and flooding

# Display a message if no button is clicked
else:
    st.info("Press 'Start Streaming Data' to begin processing live data.", icon="ℹ️")
