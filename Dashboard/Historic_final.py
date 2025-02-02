import pandas as pd
import streamlit as st
import plotly.graph_objects as go
import duckdb
import os
import time
from numerize import numerize
from confluent_kafka import Consumer
import json

# Set up the page layout
st.set_page_config(page_title="Redset Dashboard", page_icon="🌍", layout="wide")
st.header("Redset Dashboard")

# DuckDB Database File Path
DUCKDB_FILE = "cleaned_data.duckdb"
KAFKA_BROKER = 'localhost:9092'

# Connect to DuckDB
con = duckdb.connect(DUCKDB_FILE)

# Load data from DuckDB
df = con.execute("SELECT * FROM flattened_table_ids").fetchdf()

# Sidebar configuration
st.sidebar.header("Menu")

# 1. View Mode Toggle
view_mode = st.sidebar.radio("Select View", ("Instance View", "Aggregate View"))

# 2. File Size Display
file_size = os.path.getsize(DUCKDB_FILE)
st.sidebar.write(f"File Size: {numerize.numerize(file_size)}")

# 3. Instance ID Input
instance_id = st.sidebar.number_input(
    "Enter Instance ID", min_value=1, max_value=200, value=1, step=1
)

st.write(f"Selected Instance ID: {instance_id}")

# Styling (Optional)
st.markdown("""
    <style>
    body { animation: fadeIn 1.5s ease-in; }
    @keyframes fadeIn { 0% { opacity: 0; } 100% { opacity: 1; } }
    .section { animation: fadeUp 0.5s ease-in-out; }
    @keyframes fadeUp { 0% { transform: translateY(20px); opacity: 0; } 100% { transform: translateY(0); opacity: 1; } }
    .hover-box:hover { transform: scale(1.05); transition: all 0.3s ease; }
    </style>
""", unsafe_allow_html=True)


### FUNCTION TO DISPLAY REAL-TIME STRESS INDEX ###
def real_time_graph_in_historical_view():
    """
    Fetches real-time data from Kafka, processes stress index, and updates visualization.
    """
    consumer = Consumer({
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': 'analytics',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
        'enable.partition.eof': False,
    })

    consumer.subscribe(['flattened'])

    long_avg = 0.0
    short_avg = 0.0
    graph_placeholder = st.empty()  # Dynamic graph placeholder

    while True:
        msg = consumer.poll(timeout=1.0)
        if msg and msg.value():
            try:
                data = json.loads(msg.value().decode('utf-8'))
                execution_duration = float(data.get("execution_duration_ms", 0))
                bytes_spilled = float(data.get("mbytes_spilled", 0))

                # Update averages
                long_avg = (0.0002 * execution_duration) + (1 - 0.0002) * long_avg
                short_avg = (0.02 * execution_duration) + (1 - 0.02) * short_avg

                # Build graph
                fig = go.Figure()
                fig.add_trace(go.Scatter(x=[1], y=[short_avg], mode='lines', name='Short-term Avg', line=dict(color='blue')))
                fig.add_trace(go.Scatter(x=[1], y=[long_avg], mode='lines', name='Long-term Avg', line=dict(color='red')))
                fig.add_trace(go.Scatter(x=[1, 1], y=[0, bytes_spilled], fill='tozeroy', fillcolor='rgba(0,255,0,0.4)', name='Bytes Spilled'))

                # Update layout
                fig.update_layout(title="Stress Index", xaxis_title="Time", yaxis_title="Average Value", template="plotly_dark")

                graph_placeholder.plotly_chart(fig, use_container_width=True)

            except Exception as e:
                st.error(f"Error processing Kafka message: {e}")

        time.sleep(5)  # Refresh every 5 seconds


### FUNCTION TO DISPLAY HISTORICAL INGESTION TABLE ###
def build_historical_ingestion_table():
    """
    Fetches and displays a table with ingestion analytics.
    """
    df = con.execute("""
        WITH analytical_tables AS (
            SELECT instance_id, table_id,
                CAST(COALESCE(select_count / (transform_count + select_count), 0) AS DECIMAL(20, 2)) AS percentage_select_queries
            FROM tables_workload_count
            WHERE percentage_select_queries > 0.80
        )
        SELECT 
            instance_id, 
            read_table_id,
            CAST(AVG(time_since_last_ingest_ms) / 1000.0 AS DECIMAL(20, 0)) AS average_time_since_last_ingest_s, 
            CAST(AVG(time_to_next_ingest_ms) / 1000.0 AS DECIMAL(20, 0)) AS average_time_to_next_ingest_s
        FROM output_table
        WHERE read_table_id IN (SELECT table_id FROM analytical_tables)  
            AND query_type = 'select'
        GROUP BY instance_id, read_table_id;
    """).df()

    # Clean data
    df.fillna(0, inplace=True)
    df['read_table_id'] = df['read_table_id'].astype(str)

    # Render table
    fig = go.Figure(data=[go.Table(
        columnwidth=[5, 10, 10, 10],
        header=dict(
            values=["Instance ID", "Read Table ID", "Avg Time Since Ingest (s)", "Avg Time to Next Ingest (s)"],
            fill_color="royalblue",
            font=dict(color="white", size=14),
            align="center"
        ),
        cells=dict(
            values=[df["instance_id"], df["read_table_id"], df["average_time_since_last_ingest_s"], df["average_time_to_next_ingest_s"]],
            fill_color="black",
            font=dict(color="white", size=12),
            align="center"
        )
    )])

    fig.update_layout(title="Historical Ingestion Metrics", template="plotly_dark", width=750, height=300)
    return fig


### SHOW VISUALIZATIONS ###
if view_mode == "Instance View":
    display_metrics()
    real_time_graph_in_historical_view()
    st.plotly_chart(build_historical_ingestion_table(), use_container_width=True)


### FOOTER ###
st.markdown("""
    <footer style="text-align:center; font-size:12px; color:grey; padding-top:20px; border-top: 1px solid #e0e0e0; margin-top:20px;">
        <p>Pipeline Pioneers &copy; 2025 | UTN</p>
    </footer>
""", unsafe_allow_html=True)
