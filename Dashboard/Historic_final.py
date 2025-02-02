import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import os
import duckdb
from datetime import datetime
from numerize import numerize
import time
import pandas as pd
from confluent_kafka import Producer, Consumer
from confluent_kafka.admin import AdminClient, NewTopic, ConfigResource
import json
import pyarrow.parquet as pq
import time
from collections import deque
from ksql import KSQLAPI
import requests
import duckdb
import os

# Set up the page layout
st.set_page_config(page_title="Redset Dashboard", page_icon="🌍", layout="wide")
st.header("Redset Dashboard")

# DuckDB Database File Path
DUCKDB_FILE = "cleaned_data.duckdb"
KAFKA_BROKER = 'localhost:9092'  # Kafka broker address

# Connect to the DuckDB database
con = duckdb.connect(DUCKDB_FILE)

# Load data from DuckDB
df = con.execute("SELECT * FROM flattened_table_ids").fetchdf()

# Strip any extra spaces in column names
df.columns = df.columns.str.strip()

# Sidebar configuration
st.sidebar.header("Menu")

# 1. Historical View / Live View Toggle
view_mode = st.sidebar.radio("Select View", ("Instance View", "Aggregate View"))

# 2. File Size Display
file_size = os.path.getsize(DUCKDB_FILE)
st.sidebar.write(f"File Size: {numerize.numerize(file_size)}")

# 3. Instance ID Input
instance_id = st.sidebar.number_input(
    "Enter Instance ID",  # Label for the input
    min_value=1,          # Minimum value allowed
    max_value=200,        # Maximum value allowed
    value=1,              # Default value
    step=1                # Step size for increment/decrement
)

# Display selected instance ID in the main app (optional)
st.write(f"Selected Instance ID: {instance_id}")


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


def calculate_stress(consumer, long_avg, short_avg):
    """
    Reads the latest message from Kafka, updates the running averages, and exits.

    Args:
        consumer: Kafka consumer instance.
        long_avg: Initial long-term running average.
        short_avg: Initial short-term running average.

    Returns:
        Tuple (short_avg, long_avg) after processing the latest message.
    """
    long_alpha = 0.0002  # Long-term averaging factor
    short_alpha = 0.02   # Short-term averaging factor

    # Poll once to get the latest message
    msg = consumer.poll(timeout=1.0)

    if msg is None or msg.value() is None:
        print("No new messages in Kafka. Exiting...")
        #consumer.close()
        return short_avg, long_avg  # Return unchanged averages if no message was found

    if msg.error():
        print(f"Kafka Error: {msg.error()}")
        #consumer.close()
        return short_avg, long_avg  # Return unchanged averages on error

    try:
        # Decode and parse JSON message
        message_value = msg.value().decode('utf-8')
        message_dict = json.loads(message_value)

        # Convert execution duration to float
        execution_duration = float(message_dict["execution_duration_ms"])
        mb_spilled = message_dict["mbytes_spilled"]

        # Compute running averages
        long_avg = (long_alpha * execution_duration) + (1 - long_alpha) * long_avg
        short_avg = (short_alpha * execution_duration) + (1 - short_alpha) * short_avg

        print(f"Updated Averages → Short: {short_avg}, Long: {long_avg}")

        # Commit offset (optional if auto-commit is disabled)
        consumer.commit()

    except json.JSONDecodeError as e:
        print(f"JSON Decode Error: {e}")
    except ValueError as e:
        print(f"ValueError converting execution_duration_ms: {e}")

    # Close consumer and exit
    #consumer.close()
    return short_avg, long_avg


def check_duckdb_table(table_name, conn):
    """
    Verifies if a table exists and has data in DuckDB.
    
    :param table_name: Name of the table to check.
    :param conn: DuckDB connection object.
    :return: Tuple (exists, row_count)
    """
    try:
        # Check if the table exists
        table_exists = conn.execute(
            f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{table_name}'"
        ).fetchone()[0] > 0

        if not table_exists:
            print(f"Table '{table_name}' does NOT exist in DuckDB.")
            return False, 0

        # Check if the table has data
        row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        
        if row_count > 0:
            print(f"Table '{table_name}' exists and contains {row_count} rows.")
            
            # Optionally show the first few rows
            df_preview = conn.execute(f"SELECT * FROM {table_name}").df()
            print("Table Preview:")
            print(df_preview)
            
        else:
            print(f"Table '{table_name}' exists but is EMPTY.")

        return True, row_count

    except Exception as e:
        print(f"Error checking table '{table_name}': {e}")
        return False, 0


def parquet_to_table(consumer, table, conn, columns,topic):
    """
    Reads messages from a Kafka consumer, extracts data from JSON, writes to Parquet,
    and loads into a DuckDB table.
    
    Args:
        consumer: Kafka consumer instance.
        table: DuckDB table name.
        conn: DuckDB connection.
    """
    data_list = []
    parquet_file = f"kafka_data_{table}.parquet"
    
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            break  # Stop polling when there are no more messages
        
        if msg.error():
            print(f"Kafka Error: {msg.error()}")
            continue  # Skip errors and keep polling

        # Deserialize JSON Kafka message
        message_value = msg.value().decode('utf-8')

        try:
            # Convert JSON string to dictionary
            records = json.loads(message_value)

            if isinstance(records, dict):
                records = [records]  # Ensure list format

            data_list.extend(records)
        
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON: {e}")

    if not data_list:  # If no data was received, exit function early
        print("No data received from Kafka.")
        return

    df = pd.DataFrame(data_list)
    df = df[columns]

    if "arrival_timestamp" in df.columns:
        df["arrival_timestamp"] = pd.to_datetime(df["arrival_timestamp"], errors='coerce')  # Handle parsing errors
    if topic == 'flattened':
        df['read_table_ids'] = df['read_table_ids'].astype(str).str.split(",")
        df = df.explode('read_table_ids', ignore_index=True)

        # Handle None/NaN values before conversion
        df['read_table_ids'] = pd.to_numeric(df['read_table_ids'], errors='coerce')

        # Convert to nullable integer type (allows NaN values)
        df['read_table_ids'] = df['read_table_ids'].astype(pd.Int64Dtype())

    # Save as Parquet
    df.to_parquet(parquet_file, index=False)
    time.sleep(4)
    # Get absolute path for DuckDB compatibility
    parquet_path = os.path.abspath(parquet_file)

    # Load into DuckDB
    conn.execute(f"COPY {table} FROM '{parquet_path}' (FORMAT PARQUET)")
    consumer.commit()  # Commit offset to ensure that only new data is written

# Function to display metrics with subtle colors
def display_metrics():
    # Use df directly instead of df_selection
    total_query = len(df)
    successful_query = len(df[df['was_aborted'] == False])
    aborted_query = len(df[df['was_aborted'] == True])
    cached_query = len(df[df['was_cached'] == True])
    total_scan_mbytes = df['mbytes_scanned'].sum()  # Total MBs scanned
    total_spilled_mbytes = df['mbytes_spilled'].sum()  # Total MBs spilled
    total_joins = df['num_joins'].sum()  # Total joins across all queries
    total_aggregations = df['num_aggregations'].sum()  # Total aggregations across all queries

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

# Ensure 'arrival_timestamp' is converted to datetime
df['arrival_timestamp'] = pd.to_datetime(df['arrival_timestamp'], errors='coerce')

############# STRESS INDEX QUERY ##########################################
def real_time_graph_in_historical_view():
    consumer = Consumer({
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': 'analytics',
        'auto.offset.reset': 'earliest',  # Read from the beginning if no offset found
        'enable.auto.commit': False,       # Enable automatic commit
        'enable.partition.eof': False,    # Avoid EOF issues
        })
    # Initialize Kafka consumer (replace with actual consumer setup)

    #consumer = Consumer(conf)
    consumer.subscribe('flattened')  

    long_avg = 0.0  # Initial value for long-term average
    short_avg = 0.0  # Initial value for short-term average

    # Container for the graph
    graph_placeholder = st.empty()

    try:
        # Update the plot periodically (using a loop)
        for i in range(1000):  # You can adjust the range to control how many times the graph updates
            # Call your function to get updated averages and bytes spilled
            short_avg, long_avg, bytes_spilled = calculate_stress(consumer, long_avg, short_avg)

            # Visualize the stress index with the updated values
            fig = visualize_stress_index(short_avg, long_avg, bytes_spilled)

            # Display the graph in the placeholder
            graph_placeholder.plotly_chart(fig, use_container_width=True)

            # Sleep for a certain interval before updating the graph again
            time.sleep(1)  # Updates every 1 second, adjust the interval as needed

    finally:
        # Close the Kafka consumer and clear the placeholder when done
        consumer.close()
        graph_placeholder.empty()

def visualize_stress_index(short_avg, long_avg, bytes_spilled):
    # Create a figure using Plotly's graph objects
    fig = go.Figure()

    # Add a line for the short-term average (blue)
    fig.add_trace(go.Scatter(
        x=[1], y=[short_avg],
        mode='lines+markers',
        name='Short-term Avg',
        line=dict(color='blue', width=2)
    ))

    # Add a line for the long-term average (red)
    fig.add_trace(go.Scatter(
        x=[1], y=[long_avg],
        mode='lines+markers',
        name='Long-term Avg',
        line=dict(color='red', width=2)
    ))

    # Add shaded area for bytes spilled (green)
    fig.add_trace(go.Scatter(
        x=[1, 1], y=[0, bytes_spilled],
        fill='tozeroy',  # Fills the area under the line
        fillcolor='rgba(0,255,0,0.4)',  # Shaded green color with transparency
        line=dict(color='green', width=2),  # Border of the area (optional)
        name='Bytes Spilled',
        showlegend=False  # We don't need a legend for this trace
    ))

    # Create a secondary y-axis for bytes spilled (green area) to avoid overlap
    fig.update_layout(
        title="Stress Index Visualization",
        xaxis_title="Time",
        yaxis_title="Average Value",
        yaxis2=dict(
            title="Bytes Spilled",
            overlaying="y",  # Overlay the secondary axis with the primary y-axis
            side="right",  # Place the secondary y-axis on the right
        ),
        showlegend=True,
        template='plotly_dark',
        xaxis=dict(tickvals=[1], ticktext=["Time"]),
        margin=dict(t=30, b=30, l=30, r=50),  # Adjust margins for better spacing
    )

    return fig


##################OTHER ANALYTICAL QUERIES############################


def historical_view_graphs():
    
    average_times_ingestion_analytics = """
        WITH analytical_tables AS (
        SELECT  -- get the tables that are identified as tables for analytical workflow
            instance_id,
            table_id,
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
        WHERE 1 
            AND read_table_id IN ( 
                SELECT table_id
                FROM analytical_tables) -- only consider analytial tables that were read
            AND query_type = 'select'
        GROUP BY instance_id, read_table_id
        --HAVING average_time_since_last_ingest_s > average_time_to_next_ingest_s --potential data freshness issues
    """

    # Streamlit UI
    st.title("Average Times Ingestion Analytics")

    # Create a placeholder for the table
    table_placeholder = st.empty()  # Placeholder for dynamic updates

    # Function to fetch data and display the table
    def update_table():
        # Execute the SQL query
        result_df = con.execute(average_times_ingestion_analytics).fetchdf()

        # Clean the data
        result_df['read_table_id'] = result_df['read_table_id'].astype(str)  # Ensure IDs are strings
        result_df.fillna(0, inplace=True)  # Replace NaN values with 0 for clarity

        # Display the table in a smaller width container
        st.markdown(
            """
            <style>
            .dataframe-container {
                width: 600px;  /* Set a smaller width for the table */
                margin: 0 auto;  /* Center the table */
                border: 1px solid #ddd;  /* Add a border for better visuals */
                border-radius: 8px;  /* Rounded corners */
                box-shadow: 0px 4px 6px rgba(0, 0, 0, 0.1);  /* Add shadow effect */
            }
            </style>
            """, unsafe_allow_html=True
        )

        # Render the table
        table_placeholder.dataframe(result_df, use_container_width=False)  # Do not use full width

    # Real-time updates or manual refresh
    if st.button("Start Real-Time Updates"):
        while True:
            update_table()  # Fetch and display fresh data as a table
            time.sleep(5)  # Update every 5 seconds
    else:
        st.write("Click the button to start real-time updates.")


    # ---- INSERT SQL ----
    
    
   
    

    # ---- INSERT SQL ---
    
    


    
    # ---- Now, generate visualizations using the data fetched via SQL queries ----
    
    

    # Display charts in parallel (side by side)
 



# Show content based on the selected view mode
if view_mode == "Instance View":
    display_metrics()  
    real_time_graph_in_historical_view()
    historical_view_graphs()  
    

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
