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


# Initialize the Streamlit layout and options
st.set_page_config(page_title="Redset Dashboard", page_icon="pipeline.png", layout="wide")
st.header("Redset Dashboard")

# Sidebar configuration
st.sidebar.header("Menu")
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

    metrics_placeholder = st.empty()

    try:
        while True:
            # load new data from Kafka
            parquet_to_table(consumer_query_counter, 'LIVE_QUERY_METRICS', con, QUERY_COLUMNS, TOPIC_QUERY_METRICS)
            parquet_to_table(consumer_leaderboard, 'LIVE_LEADERBOARD', con, LEADERBOARD_COLUMNS, TOPIC_LEADERBOARD)
            parquet_to_table(consumer_compile, 'LIVE_COMPILE_METRICS', con, COMPILE_COLUMNS, TOPIC_COMPILE_METRICS)
            # use session state to prevent flashin dashboard
            st.session_state.fig1 = build_leaderboard_compiletime(con)
            st.session_state.fig2 = build_leaderboard_user_queries(con)
            #st.session_state.fig3 = build_live_query_counts(con)
            st.session_state.fig4 = build_live_query_distribution(con)
            st.session_state.fig5 = build_live_compile_metrics(con)
            #st.session_state.fig6 = build_live_spilled_scanned(con)

            #Display metrices dynamically 
            display_metrics(con, metrics_placeholder)

            # Render figures in containers
            st.session_state.fig1.update_layout(width=400)
            st.session_state.fig4.update_layout(width=400)
            st.session_state.fig2.update_layout(width=400)
            st.session_state.fig5.update_layout(width=400)
            uniq_id = str(int(time.time()))
                # First Row: Compile Time Leaderboard (Left) & Query Distribution (Right)
            with st.container():
                col1, col2 = st.columns(2)  # Two columns
                
                with col1:  # Left Column
                    st.plotly_chart(st.session_state.fig1, config={"responsive": True}, use_container_width=True)
                
                with col2:  # Right Column
                    st.plotly_chart(st.session_state.fig4, config={"responsive": True}, use_container_width=True)
            
            # Second Row: User Queries (Left) & Compile Metrics (Right)
            with st.container():
                col3, col4 = st.columns(2)
                
                with col3:  # Left Column
                    st.plotly_chart(st.session_state.fig2, config={"responsive": True}, use_container_width=True)
                
                with col4:  # Right Column
                    st.plotly_chart(st.session_state.fig5, config={"responsive": True}, use_container_width=True)

# Create placeholders for graphs and metrics *once* outside the loop:
fig1_placeholder = st.empty()  # For leaderboard compile time
fig2_placeholder = st.empty()  # For leaderboard user queries
fig4_placeholder = st.empty()  # For live query distribution
fig5_placeholder = st.empty()  # For live compile metrics
metrics_placeholder = st.empty()  # For dynamic metrics

# (Optionally, if you want to arrange the graphs in a fixed layout, create a container:)
graph_container = st.container()  

try:
    while True:
        # Load new data from Kafka and update DuckDB tables
        parquet_to_table(consumer_query_counter, 'LIVE_QUERY_METRICS', con, QUERY_COLUMNS, TOPIC_QUERY_METRICS)
        parquet_to_table(consumer_leaderboard, 'LIVE_LEADERBOARD', con, LEADERBOARD_COLUMNS, TOPIC_LEADERBOARD)
        parquet_to_table(consumer_compile, 'LIVE_COMPILE_METRICS', con, COMPILE_COLUMNS, TOPIC_COMPILE_METRICS)
        
        # Update session state figures (these functions should return updated figures)
        st.session_state.fig1 = build_leaderboard_compiletime(con)
        st.session_state.fig2 = build_leaderboard_user_queries(con)
        st.session_state.fig4 = build_live_query_distribution(con)
        st.session_state.fig5 = build_live_compile_metrics(con)

        # Update metrics display (this function uses the provided placeholder)
        display_metrics(con, metrics_placeholder)

        # Update layout (if needed)
        st.session_state.fig1.update_layout(width=400)
        st.session_state.fig4.update_layout(width=400)
        st.session_state.fig2.update_layout(width=400)
        st.session_state.fig5.update_layout(width=400)

        # Now update the pre-created graph placeholders.
        # Use a fixed layout so that the graphs update in place instead of stacking.
        with graph_container.container():
            col1, col2 = st.columns(2)
            with col1:
                fig1_placeholder.plotly_chart(
                    st.session_state.fig1, 
                    config={"responsive": True}, 
                    use_container_width=True
                )
            with col2:
                fig4_placeholder.plotly_chart(
                    st.session_state.fig4, 
                    config={"responsive": True}, 
                    use_container_width=True
                )
            # Second row of graphs:
            col3, col4 = st.columns(2)
            with col3:
                fig2_placeholder.plotly_chart(
                    st.session_state.fig2, 
                    config={"responsive": True}, 
                    use_container_width=True
                )
            with col4:
                fig5_placeholder.plotly_chart(
                    st.session_state.fig5, 
                    config={"responsive": True}, 
                    use_container_width=True
                )
        
        # Wait before the next update
        time.sleep(5)

except KeyboardInterrupt:
    print("\nStopping consumer...")

finally:
    consumer_raw_data.close()
    consumer_leaderboard.close()
    consumer_query_counter.close()
    consumer_compile.close()
    # consumer_stress.close()  # if used

    except KeyboardInterrupt:
        print("\nStopping consumer...")
    finally:
        consumer_raw_data.close()
        consumer_leaderboard.close()
        consumer_query_counter.close()
        consumer_compile.close()
        #consumer_stress.close()


########### DuckDB to Dashboard ################
# Function to fetch and update metrics in a fixed UI position
def display_metrics(con, metrics_placeholder):
    # Query DuckDB tables to get the latest metrics
    total_query = con.execute("SELECT COUNT(*) FROM LIVE_QUERY_METRICS").fetchone()[0]
    successful_query = con.execute("SELECT COUNT(*) FROM LIVE_QUERY_METRICS WHERE was_aborted = FALSE").fetchone()[0]
    aborted_query = con.execute("SELECT COUNT(*) FROM LIVE_QUERY_METRICS WHERE was_aborted = TRUE").fetchone()[0]
    cached_query = con.execute("SELECT COUNT(*) FROM LIVE_QUERY_METRICS WHERE was_cached = TRUE").fetchone()[0]
    total_scan_mbytes = con.execute("SELECT SUM(mbytes_scanned) FROM LIVE_COMPILE_METRICS").fetchone()[0] or 0  # Handle NULL values
    total_spilled_mbytes = con.execute("SELECT SUM(mbytes_spilled) FROM LIVE_COMPILE_METRICS").fetchone()[0] or 0
    total_joins = con.execute("SELECT SUM(num_joins) FROM LIVE_COMPILE_METRICS").fetchone()[0] or 0
    total_aggregations = con.execute("SELECT SUM(num_aggregations) FROM LIVE_COMPILE_METRICS").fetchone()[0] or 0

    # Update the metrics display inside the placeholder
    with metrics_placeholder.container():
        st.markdown("### Query Metrics")

        # Define layout for metrics display
        col1, col2, col3, col4 = st.columns(4)
        light_red = "#FFEBEE"
        dark_red = "#D32F2F"

        # Total Queries
        with col1:
            st.markdown(f"""
                <div style="padding: 20px; background-color: {light_red}; border-left: 10px solid {dark_red}; border-radius: 10px; text-align: center; margin-bottom: 20px;">
                    <h5>📊 Total Queries</h5>
                    <h3>{total_query}</h3>
                </div>
            """, unsafe_allow_html=True)

        # Successful Queries
        with col2:
            st.markdown(f"""
                <div style="padding: 20px; background-color: {light_red}; border-left: 10px solid {dark_red}; border-radius: 10px; text-align: center; margin-bottom: 20px;">
                    <h5>✅ Successful Queries</h5>
                    <h3>{successful_query}</h3>
                </div>
            """, unsafe_allow_html=True)

        # Aborted Queries
        with col3:
            st.markdown(f"""
                <div style="padding: 20px; background-color: {light_red}; border-left: 10px solid {dark_red}; border-radius: 10px; text-align: center; margin-bottom: 20px;">
                    <h5>❌ Aborted Queries</h5>
                    <h3>{aborted_query}</h3>
                </div>
            """, unsafe_allow_html=True)

        # Cached Queries
        with col4:
            st.markdown(f"""
                <div style="padding: 20px; background-color: {light_red}; border-left: 10px solid {dark_red}; border-radius: 10px; text-align: center; margin-bottom: 20px;">
                    <h5>💾 Cached Queries</h5>
                    <h3>{cached_query}</h3>
                </div>
            """, unsafe_allow_html=True)

        # Additional metrics section
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


# output = table
def build_leaderboard_compiletime(con):
    '''
    PREREQUISITES: parquet_to_table(consumer,'LIVE_LEADERBOARD', 
    con,LEADERBOARD_COLUMNS,TOPIC_LEADERBOARD) has already been called
    
    returns dataframe containing top 10 compile times and their instance_id
    '''
    df1 = con.execute(f"""
    SELECT 
    instance_id, 
    compile_duration_ms as compile_duration
    FROM LIVE_LEADERBOARD
    ORDER BY compile_duration_ms DESC
    LIMIT 10;
    """).df()

    df1["compile_duration"] = df1["compile_duration"].fillna(0).astype(int)
    df1['formatted_compile_time'] = df1['compile_duration'].apply(
        lambda x: f"{x // 60000}:{(x % 60000) // 1000:02d}"
    )

    # Add rank column for leaderboard display
    df1.insert(0, "Rank", range(1, len(df1) + 1))

    fig = go.Figure(data=[go.Table(
        columnwidth=[2, 5, 5],
        header=dict(values=["Rank", "Instance ID", "Compile Duration (mm:ss)"],
                    fill_color="royalblue",
                    font=dict(color="white", size=14),
                    align="center"),
        cells=dict(values=[
            df1["Rank"].tolist(),
            df1["instance_id"].tolist(),
            df1["formatted_compile_time"].tolist()
        ],
                   fill_color="black",
                   font=dict(color="white", size=12),
                   align="center"))
    ])

    fig.update_layout(
        title="Leaderboard: Top 10 Longest Compile Times",
        template="plotly_dark",
        autosize=False,
        width=750,
        height=450
    )
    return fig
#output = bar chart 
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
        template='plotly_dark',
        width=750,  # pick a smaller width
        height=300
    )
    #fig.show()
    return fig
#output = piechart

#output = dounut chart
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
        template='plotly_dark',
        width = 750
    )

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

    # Visualization: Pie Chart using Plotly
    fig = go.Figure()

    fig.add_trace(go.Pie(
        labels=df['query_type'],  # Use query_type as labels
        values=df['occurrence_count'],  # Use occurrence_count as values
        hoverinfo='label+percent',  # Display label and percentage on hover
        textinfo='label+percent',  # Display label and percentage on the chart itself
        marker=dict(colors=px.colors.qualitative.Plotly),  # Dynamic colors
    ))

    fig.update_layout(
        title='Query Type Distribution',
        template='plotly_dark'
    )

    return fig

#output = stacked bar chart
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
        #barmode='stack',
        xaxis_title='Metric Type',
        yaxis_title='Count',
        template='plotly_dark',
        width=750,  # pick a smaller width

    )
    #fig.show()
    return fig

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

    print(df)
    print(df)
    # Save as Parquet
    df.to_parquet(parquet_file, index=False)
    time.sleep(4)
    # Get absolute path for DuckDB compatibility
    parquet_path = os.path.abspath(parquet_file)

    # Load into DuckDB
    conn.execute(f"COPY {table} FROM '{parquet_path}' (FORMAT PARQUET)")
    consumer.commit()  # Commit offset to ensure that only new data is written


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
