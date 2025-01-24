import pandas as pd
import plotly.graph_objects as go
import streamlit as st
import time
from kafka import KafkaConsumer
import json
import threading
import duckdb
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
st.markdown("*Prototype v1.1*: Query Metrics with Stress Indicator and Visualization")

# DuckDB setup: Create or connect to an in-memory database
conn = duckdb.connect('historical_data.duckdb')  # Change the filename for persistent storage

# Create a table if it doesn't exist
conn.execute("""
CREATE TABLE IF NOT EXISTS kafka_data (
    query_id STRING,
    arrival_timestamp TIMESTAMP,
    compile_duration_ms INTEGER,
    execution_duration_ms INTEGER,
    queue_duration_ms INTEGER,
    was_aborted BOOLEAN,
    was_cached BOOLEAN,
    query_type STRING,
    num_permanent_tables_accessed INTEGER,
    num_external_tables_accessed INTEGER,
    num_system_tables_accessed INTEGER
);
""")

#######################################
# KAFKA CONSUMER (Replacing File Upload)
#######################################

# Kafka Consumer Setup
KAFKA_SERVER = 'localhost:9092'  # Kafka server address
KAFKA_TOPIC = 'your_topic_name'  # Replace with your Kafka topic
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_SERVER,
    group_id='my-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Shared data storage for Streamlit
message_data = []

def consume_kafka_data():
    """ Function to consume data from Kafka and update message_data """
    global message_data
    for message in consumer:
        # Assuming the Kafka message is a dictionary and is JSON serializable
        message_data.append(message.value)
        
        # Store the incoming data into DuckDB
        df = pd.DataFrame(message_data)
        conn.executemany("""
            INSERT INTO kafka_data (query_id, arrival_timestamp, compile_duration_ms, execution_duration_ms, 
                                    queue_duration_ms, was_aborted, was_cached, query_type,
                                    num_permanent_tables_accessed, num_external_tables_accessed, num_system_tables_accessed)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """, df.values.tolist())
        
        # Update Streamlit UI when a new batch of data is consumed
        if len(message_data) > 0:
            batch_size = len(message_data)
            timestamp = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
            st.sidebar.write(f"New Data Batch Arrived: {batch_size} records at {timestamp}")
            
            # Create DataFrame from the batch of messages
            st.subheader("Batch Data Preview")
            st.dataframe(df)

# Run the Kafka consumer in a separate thread
consumer_thread = threading.Thread(target=consume_kafka_data, daemon=True)
consumer_thread.start()

#######################################
# HISTORICAL DATA QUERY (from DuckDB)
#######################################

st.subheader("Search Historical Data by Date Range")

# Select a date range
start_date = st.date_input("Start Date", min_value=pd.to_datetime("2022-01-01"))
end_date = st.date_input("End Date", max_value=pd.to_datetime("today"))

# Convert to string for SQL query compatibility
start_date_str = start_date.strftime('%Y-%m-%d')
end_date_str = end_date.strftime('%Y-%m-%d')

# Query DuckDB for historical data within the selected date range
query = f"""
SELECT * FROM kafka_data
WHERE arrival_timestamp BETWEEN '{start_date_str}' AND '{end_date_str}'
"""
df_historical = conn.execute(query).fetchdf()

# Show the historical data (if any)
if not df_historical.empty:
    st.subheader(f"Historical Data from {start_date_str} to {end_date_str}")
    st.dataframe(df_historical)

else:
    st.warning("No data found for the selected date range.")

#######################################
# REAL-TIME KAFKA DATA FILTERING BY DATE
#######################################

# Date range selection for real-time Kafka data
st.subheader("Filter Real-Time Kafka Data by Date Range")

# Convert message arrival timestamp to datetime for filtering
filtered_kafka_data = [
    msg for msg in message_data if start_date <= pd.to_datetime(msg['arrival_timestamp']).date() <= end_date
]
filtered_df = pd.DataFrame(filtered_kafka_data)

# Display filtered real-time Kafka data
if not filtered_df.empty:
    st.subheader(f"Real-Time Data from {start_date_str} to {end_date_str}")
    st.dataframe(filtered_df)
else:
    st.warning("No real-time data found for the selected date range.")

#######################################
# DATA UPLOAD - Sidebar
#######################################
'''
with st.sidebar:
    st.header("Configuration")
    uploaded_file = st.file_uploader("Choose a CSV file", type=["csv"])

# Check if a file is uploaded
if uploaded_file is not None:
    # Get the file size and timestamp when uploaded
    file_size = uploaded_file.size  # File size in bytes
    upload_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())  # Timestamp

    # Show file details in the sidebar
    st.sidebar.write(f"File Size: {file_size / 1024:.2f} KB")  # Convert bytes to KB
    st.sidebar.write(f"Uploaded at: {upload_time}")

    # Load the CSV file
    @st.cache_data
    def load_data(file):
        try:
            df = pd.read_csv(file)
        except Exception as e:
            st.error(f"Error loading CSV file: {e}")
            st.stop()  # Stop execution if the file can't be loaded
        return df

    df = load_data(uploaded_file)

    # Show a preview of the data
    st.subheader("Data Preview")
    st.dataframe(df)

else:
    st.info("Please upload a CSV file to proceed.", icon="ℹ️")
'''
# If df is successfully loaded, the rest of the analysis is performed
if uploaded_file is not None:


    #######################################
    # CALENDAR DATE SELECTION
    #######################################
    st.subheader("Select Date Range")

    # Convert arrival_timestamp to datetime
    df['arrival_timestamp'] = pd.to_datetime(df['arrival_timestamp'])

    # Date range selection: user selects start and end date
    start_date, end_date = st.date_input(
        "Select a date range", 
        value=(df['arrival_timestamp'].min(), df['arrival_timestamp'].max()), 
        min_value=df['arrival_timestamp'].min(),
        max_value=df['arrival_timestamp'].max()
    )

    # Filter data based on selected date range
    filtered_df = df[(df['arrival_timestamp'] >= pd.to_datetime(start_date)) & 
                     (df['arrival_timestamp'] <= pd.to_datetime(end_date))]

    # Check if the filtered data is empty
    if filtered_df.empty:
        st.error("No data available for the selected date range. Please try another date.")
    else:
        st.write(f"Showing data from {start_date} to {end_date}.")

        # Display the filtered data preview
        st.subheader("Filtered Data Preview")
        st.dataframe(filtered_df)

        #######################################
        # QUERY EXECUTION SUMMARY - Main Dashboard
        #######################################
        st.subheader("Query Execution Summary")

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

        #######################################
        # QUERY WITH MAX COMPILE DURATION
        #######################################
        st.subheader("Query with Maximum Compile Duration")

        # Get the query with the highest compile duration
        max_compile_query = filtered_df.loc[filtered_df['compile_duration_ms'].idxmax()]

        st.write(f"Query with highest compile duration: {max_compile_query['query_id']}")

        # Create a line chart for the query with maximum compile duration
        fig_max_compile = go.Figure()
        fig_max_compile.add_trace(go.Scatter(
            x=filtered_df['arrival_timestamp'], 
            y=filtered_df['compile_duration_ms'], 
            mode='lines',
            name="Compile Duration",
            line=dict(color='royalblue', width=2)
        ))

        fig_max_compile.update_layout(
            title="Query with Maximum Compile Duration",
            xaxis_title="Timestamp",
            yaxis_title="Compile Duration (ms)",
            template="plotly_dark"
        )
        st.plotly_chart(fig_max_compile, use_container_width=True)

        #######################################
        # OTHER ANALYSES - Parallel Layout
        #######################################
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("Exponentially Moving Average of Query Execution Time (Compact View)")
            ema_alpha = 0.1  # Smoothing factor for EMA
            filtered_df['ema_execution_duration_ms'] = filtered_df['execution_duration_ms'].ewm(alpha=ema_alpha).mean()

            fig_ema = go.Figure()
            fig_ema.add_trace(go.Scatter(x=filtered_df['arrival_timestamp'], y=filtered_df['ema_execution_duration_ms'], mode='lines', name='EMA Execution Time'))
            fig_ema.update_layout(title="EMA Execution Time", xaxis_title="Timestamp", yaxis_title="Execution Duration (ms)", showlegend=False)
            st.plotly_chart(fig_ema, use_container_width=True)

        with col2:
            st.subheader("Query Duration vs Execution Duration")
            fig_duration = go.Figure()

            fig_duration.add_trace(go.Scatter(x=filtered_df['arrival_timestamp'], y=filtered_df['queue_duration_ms'], mode='lines', name='Queue Duration'))
            fig_duration.add_trace(go.Scatter(x=filtered_df['arrival_timestamp'], y=filtered_df['compile_duration_ms'], mode='lines', name='Compile Duration'))
            fig_duration.add_trace(go.Scatter(x=filtered_df['arrival_timestamp'], y=filtered_df['execution_duration_ms'], mode='lines', name='Execution Duration'))

            fig_duration.update_layout(title="Query Duration vs Execution Duration", xaxis_title="Timestamp", yaxis_title="Duration (ms)")
            st.plotly_chart(fig_duration, use_container_width=True)

         #######################################
        # AVERAGE RESPONSE TIME FOR CACHED VS NON-CACHED QUERIES
        #######################################

        st.subheader("Average Response Time for Cached vs Non-Cached Queries")
        cached_avg = filtered_df[filtered_df['was_cached'] == 1]['execution_duration_ms'].mean()
        non_cached_avg = filtered_df[filtered_df['was_cached'] == 0]['execution_duration_ms'].mean()

        fig_avg_response = go.Figure()
        fig_avg_response.add_trace(go.Bar(x=['Cached', 'Non-Cached'], y=[cached_avg, non_cached_avg], name="Avg Execution Time (ms)"))
        fig_avg_response.update_layout(title="Average Response Time (Cached vs Non-Cached)", yaxis_title="Execution Duration (ms)")

        #######################################
        # QUERY TYPE DISTRIBUTION (PIE CHART)
        #######################################

        st.subheader("Query Type Distribution")
        
        # Count the number of queries by query_type
        query_type_counts = filtered_df['query_type'].value_counts()

        # Create a pie chart for the query type distribution
        fig_pie = go.Figure(data=[go.Pie(labels=query_type_counts.index, values=query_type_counts.values)])
        fig_pie.update_layout(
            title="Query Type Distribution",
            template="plotly_dark"
        )

        #######################################
        # Display both plots in parallel
        #######################################

        col1, col2 = st.columns(2)

        with col1:
            st.plotly_chart(fig_avg_response, use_container_width=True)

        with col2:
            st.plotly_chart(fig_pie, use_container_width=True)

        #######################################
        # STRESS LEVEL and SUCCESS RATE
        #######################################
        col3, col4 = st.columns(2)

        with col3:
            st.subheader("Stress Level Indicator (Speedometer)")
            total_computational_time = filtered_df['execution_duration_ms'].sum()
            max_computational_time = filtered_df['execution_duration_ms'].max()
            stress_level = (total_computational_time / (max_computational_time * len(filtered_df))) * 100

            fig_gauge = go.Figure(go.Indicator(
                mode="gauge+number",
                value=stress_level,
                title={"text": "Stress Level (%)"},
                gauge={"axis": {"range": [0, 100]}, "bar": {"color": "red"}}
            ))
            st.plotly_chart(fig_gauge, use_container_width=True)

        with col4:
            st.subheader("Success Rate of Cached Queries (Gauge)")
            cached_success_rate = len(filtered_df[(filtered_df['was_cached'] == 1) & (filtered_df['was_aborted'] == 0)]) / len(filtered_df[filtered_df['was_cached'] == 1]) * 100

            fig_success_rate = go.Figure(go.Indicator(
                mode="gauge+number",
                value=cached_success_rate,
                title={"text": f"Success Rate ({cached_success_rate:.2f}%)"},
                gauge={"axis": {"range": [0, 100]}, "bar": {"color": "green"}}
            ))
            st.plotly_chart(fig_success_rate, use_container_width=True)

        #######################################
        # FINAL LAYOUT WITH SCATTER PLOT FOR READS/WRITES
        #######################################

        st.subheader("Reads and Writes by Hour")
        filtered_df['hour'] = filtered_df['arrival_timestamp'].dt.hour

        # Group by hour and aggregate reads/writes
        hourly_data = filtered_df.groupby('hour').agg({
            'num_permanent_tables_accessed': 'sum',
            'num_external_tables_accessed': 'sum',
            'num_system_tables_accessed': 'sum'
        }).reset_index()

        fig_scatter = go.Figure()
        fig_scatter.add_trace(go.Scatter(x=hourly_data['hour'], y=hourly_data['num_permanent_tables_accessed'], mode='markers', name='Permanent Tables Reads'))
        fig_scatter.add_trace(go.Scatter(x=hourly_data['hour'], y=hourly_data['num_external_tables_accessed'], mode='markers', name='External Tables Reads'))
        fig_scatter.add_trace(go.Scatter(x=hourly_data['hour'], y=hourly_data['num_system_tables_accessed'], mode='markers', name='System Tables Reads'))

        fig_scatter.update_layout(title="Reads and Writes for Cluster Queries by Hour", xaxis_title="Hour", yaxis_title="Count")
        st.plotly_chart(fig_scatter, use_container_width=True)

        #######################################
        # CONCLUSION
        #######################################

        st.write("### Dashboard Completed")
        st.markdown(
            """
            This dashboard provides insights into query performance, including:
            - Execution summary metrics
            - A stress indicator for monitoring
            - Stacked bar charts for query distribution by type
            - Reads and writes for cluster queries over time
            - Query success rates, average response times, and more.
            """
        )
