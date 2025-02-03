import pandas as pd
import streamlit as st
import plotly.graph_objects as go
import duckdb
import os
import time
from numerize import numerize
from confluent_kafka import Consumer
import json
from datetime import datetime
import threading
import matplotlib
import plotly.express as px
# Set up the page layout
st.set_page_config(page_title="Redset Dashboard", page_icon="🌍", layout="wide")
st.header("Redset Dashboard")

# DuckDB Database File Path and Kafka Broker
DUCKDB_FILE = "cleaned_data.duckdb"
KAFKA_BROKER = 'localhost:9092'

# Connect to DuckDB
con = duckdb.connect(DUCKDB_FILE)

# (Optional) Load some general data from DuckDB
df_general = con.execute("SELECT * FROM flattened_table_ids").fetchdf()

# Sidebar configuration
st.sidebar.header("Menu")

# 1. View Mode Toggle
view_mode = st.sidebar.radio("Select View", ("Instance View", "Aggregate View"))

# 2. File Size Display
file_size = os.path.getsize(DUCKDB_FILE)
st.sidebar.write(f"File Size: {numerize.numerize(file_size)}")

# 3. Instance ID Input – used to filter the table data
instance_id = st.sidebar.number_input(
    "Enter Instance ID", min_value=1, max_value=200, value=1, step=1
)
st.write(f"Selected Instance ID: {instance_id}")

# Optional styling for animations, etc.
st.markdown("""
    <style>
    body { animation: fadeIn 1.5s ease-in; }
    @keyframes fadeIn { 0% { opacity: 0; } 100% { opacity: 1; } }
    .section { animation: fadeUp 0.5s ease-in-out; }
    @keyframes fadeUp { 0% { transform: translateY(20px); opacity: 0; } 100% { transform: translateY(0); opacity: 1; } }
    .hover-box:hover { transform: scale(1.05); transition: all 0.3s ease; }
    </style>
""", unsafe_allow_html=True)



###############################################################################
# FUNCTION: Build select queries histogram
###############################################################################
@st.cache_data(ttl=10)  # Cache with TTL of 10 seconds
def build_select_queries_histogram(selected_instance_id):
         query = """
            WITH analytical_tables AS (
                SELECT instance_id, table_id,
                       CAST(COALESCE(select_count / NULLIF(transform_count + select_count, 0), 0) AS DECIMAL(20, 2)) AS percentage_select_queries          
                FROM tables_workload_count
                WHERE instance_id = {selected_instance_id}
                WHERE (select_count / NULLIF(transform_count + select_count, 0)) > 0.80
            ), realtive_to_next_times AS (
                SELECT DISTINCT instance_id, query_id, read_table_id, 
                       EPOCH_MS(arrival_timestamp - last_write_table_insert) / 
                       EPOCH_MS(next_write_table_insert - last_write_table_insert) AS relative_to_next
                FROM output_table
                WHERE read_table_id IN (SELECT table_id FROM analytical_tables)
                AND query_type = 'select'
            ), hist_bins AS (
                SELECT instance_id, query_id, read_table_id,
                       NTILE(10) OVER (ORDER BY relative_to_next) AS bin
                FROM realtive_to_next_times
            )
            SELECT instance_id, read_table_id, bin, COUNT(*) AS count
            FROM hist_bins
            GROUP BY instance_id, read_table_id, bin
            ORDER BY instance_id, read_table_id, bin;
        """
        
        result = duckdb.sql(query).df()
        return result
    
    # Create a placeholder for the chart
    chart_placeholder = st.empty()
    
    # Live updating loop
    while True:
        result = build_select_queries_histogram(selected_instance_id)
    
        if result.empty:
            st.warning("No data available. Waiting for new data...")
        else:
            # Create interactive plotly histogram
            fig = px.bar(
                result, 
                x="bin", 
                y="count", 
                color="read_table_id", 
                barmode="group",
                labels={"bin": "Time Interval", "count": "Query Count"},
                title="Distribution of Select Queries After Ingestion",
            )
            fig.update_layout(xaxis_title="Relative Time Interval", yaxis_title="Query Count")
            
            # Update chart
            chart_placeholder.plotly_chart(fig, use_container_width=True)
    
        # Wait before refreshing (adjust the interval as needed)
        time.sleep(5)  # Update every 5 seconds


###############################################################################
# FUNCTION: Build Historical Ingestion Table (Filtered by Instance ID)
###############################################################################
def build_historical_ingestion_table(selected_instance_id):
    """
    Fetches and displays a table with ingestion analytics for the selected instance.
    The SQL query filters on the given instance ID.
    """
    query = f"""
        WITH analytical_tables AS (
            SELECT instance_id, table_id,
                CAST(COALESCE(select_count / NULLIF(transform_count + select_count, 0), 0) AS DECIMAL(20, 2)) AS percentage_select_queries
            FROM tables_workload_count
        )
        SELECT 
            instance_id, 
            read_table_id,
            CAST(AVG(time_since_last_ingest_ms) / 1000.0 AS DECIMAL(20, 0)) AS average_time_since_last_ingest_s, 
            CAST(AVG(time_to_next_ingest_ms) / 1000.0 AS DECIMAL(20, 0)) AS average_time_to_next_ingest_s
        FROM output_table
        WHERE read_table_id IN (
            SELECT table_id FROM analytical_tables WHERE percentage_select_queries > 0.80
        )
            AND query_type = 'select'
            AND instance_id = {selected_instance_id}
        GROUP BY instance_id, read_table_id;
    """
    df_table = con.execute(query).df()
    df_table.fillna(0, inplace=True)
    df_table['read_table_id'] = df_table['read_table_id'].astype(str)

    # Create a Plotly Table visualization
    fig = go.Figure(data=[go.Table(
        columnwidth=[5, 10, 10, 10],
        header=dict(
            values=["Instance ID", "Read Table ID", "Avg Time Since Ingest (s)", "Avg Time to Next Ingest (s)"],
            fill_color="royalblue",
            font=dict(color="white", size=14),
            align="center"
        ),
        cells=dict(
            values=[
                df_table["instance_id"], 
                df_table["read_table_id"], 
                df_table["average_time_since_last_ingest_s"], 
                df_table["average_time_to_next_ingest_s"]
            ],
            fill_color="black",
            font=dict(color="white", size=12),
            align="center"
        )
    )])
    fig.update_layout(title="Historical Ingestion Metrics", template="plotly_dark", width=750, height=300)
    return fig


def real_time_graph_in_historical_view():
    """
    Fetches real-time data from Kafka, updates the same Plotly figure in a simulation style,
    and continuously updates the displayed graph.
    """
    consumer = Consumer({
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': 'analytics',
        'auto.offset.reset': 'latest',
        'enable.auto.commit': False,
    })
    consumer.subscribe(['flattened'])

    # Initialize lists to hold time series data
    timestamps = []
    short_avgs = []
    long_avgs = []
    bytes_spilled_vals = []

    # Initialize the averages (if no previous values exist, start with 0)
    long_avg = 0.0
    short_avg = 0.0

    # Create a Streamlit placeholder and build the initial figure
    graph_placeholder = st.empty()

    # Create the initial figure with empty traces
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=timestamps, y=short_avgs,
                             mode='lines',
                             name='Short-term Avg',
                             line=dict(color='blue')))
    fig.add_trace(go.Scatter(x=timestamps, y=long_avgs,
                             mode='lines',
                             name='Long-term Avg',
                             line=dict(color='red')))
    fig.add_trace(go.Scatter(x=timestamps, y=bytes_spilled_vals,
                             mode='lines',
                             name='Bytes Spilled',
                             line=dict(color='green')))
    fig.update_layout(title="Real-Time Stress Index",
                      xaxis_title="Time",
                      yaxis_title="Average Value",
                      template="plotly_dark")
    
    # Render the initial figure
    graph_placeholder.plotly_chart(fig, use_container_width=True)

    # Begin polling Kafka and updating the same figure
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg and msg.value():
            try:
                data = json.loads(msg.value().decode('utf-8'))
                execution_duration = float(data.get("execution_duration_ms", 0))
                bytes_spilled = float(data.get("mbytes_spilled", 0))

                # Update moving averages:
                # (If lists are empty, defaults are 0)
                long_avg = (0.0002 * execution_duration) + (1 - 0.0002) * (long_avgs[-1] if long_avgs else 0)
                short_avg = (0.02 * execution_duration) + (1 - 0.02) * (short_avgs[-1] if short_avgs else 0)

                # Append current time and metric values
                current_time = datetime.now().strftime("%H:%M:%S")
                timestamps.append(current_time)
                short_avgs.append(short_avg)
                long_avgs.append(long_avg)
                bytes_spilled_vals.append(bytes_spilled)

                # Keep only the most recent 50 data points for clarity
                if len(timestamps) > 50:
                    timestamps = timestamps[-50:]
                    short_avgs = short_avgs[-50:]
                    long_avgs = long_avgs[-50:]
                    bytes_spilled_vals = bytes_spilled_vals[-50:]

                # Update the existing figure's data (do not recreate the figure)
                fig.data[0].x = timestamps
                fig.data[0].y = short_avgs
                fig.data[1].x = timestamps
                fig.data[1].y = long_avgs
                fig.data[2].x = timestamps
                fig.data[2].y = bytes_spilled_vals

                # Update the placeholder with the updated figure
                graph_placeholder.plotly_chart(fig, use_container_width=True)

            except Exception as e:
                st.error(f"Error processing Kafka message: {e}")

        time.sleep(5)  # Wait 5 seconds before polling again

###############################################################################
# SHOW VISUALIZATIONS BASED ON VIEW MODE
###############################################################################
if view_mode == "Instance View":
    # Display the historical ingestion table filtered by the selected instance ID
    table_fig = build_historical_ingestion_table(instance_id)
    st.plotly_chart(table_fig, use_container_width=True)
    build_select_queries_histogram()
    # Start the real-time stress index graph in a separate thread to avoid blocking the main thread
    stress_thread = threading.Thread(target=real_time_graph_in_historical_view, daemon=True)
    stress_thread.start()

# (Optionally, add code to handle "Aggregate View" here)


###############################################################################
# FOOTER
###############################################################################
st.markdown("""
    <footer style="text-align:center; font-size:12px; color:grey; padding-top:20px; border-top: 1px solid #e0e0e0; margin-top:20px;">
        <p>Pipeline Pioneers &copy; 2025 | UTN</p>
    </footer>
""", unsafe_allow_html=True)




# Get unique read_table_ids
read_table_ids = result["read_table_id"].unique()

# Create subplots
fig, axes = plt.subplots(len(read_table_ids), 1, figsize=(8, len(read_table_ids) * 4), sharex=True)

# If there's only one read_table_id, axes is not a list
if len(read_table_ids) == 1:
    axes = [axes]

# Loop through each read_table_id and plot
for i, read_table_id in enumerate(read_table_ids):
    query = f"SELECT bin, count FROM result WHERE read_table_id = {read_table_id} ORDER BY bin"
    res = duckdb.sql(query).df()

    axes[i].bar(res["bin"], res["count"], width=0.8, color='skyblue', edgecolor='black')
    axes[i].set_title(f"Read Table ID: {read_table_id}")
    axes[i].set_xlabel("Count")
    axes[i].grid(axis='y', linestyle='--', alpha=0.7)

# Set common x-label
plt.xlabel("Bin")
plt.xticks(sorted(res["bin"].unique()))
plt.tight_layout()
plt.show()
