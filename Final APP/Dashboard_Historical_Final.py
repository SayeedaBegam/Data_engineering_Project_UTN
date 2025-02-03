
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
import duckdb
import time
import threading
import pandas as pd
import streamlit as st
from datetime import datetime, timedelta
from confluent_kafka import Consumer
import os
import json
import plotly.express as px
import matplotlib.pyplot as plt

# Set up the page layout
st.set_page_config(page_title="Redset Dashboard", page_icon="🌍", layout="wide")
st.header("Redset Dashboard")

# DuckDB Database File Path and Kafka Broker
DUCKDB_FILE = 'cleaned_data.duckdb'
con = duckdb.connect(DUCKDB_FILE)
TOPIC_FLAT_TABLES = 'flattened'
FLAT_COLUMNS = ['instance_id','query_id','write_table_ids','read_table_ids','arrival_timestamp','query_type']

#start = '2024-29-02 23:59:00' # first timestamp in the dataset
#end = '2024-03-01 01:00:00'
instance_id = 85
KAFKA_BROKER = 'localhost:9092'


# Connect to DuckDB
con = duckdb.connect(DUCKDB_FILE)

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
# FUNCTION: Build Historical Ingestion Table (Filtered by Instance ID)
###############################################################################


# Define the table creation SQL queries
#inserted for Kafka
create_flattened_table_ids_table = """
    CREATE OR REPLACE TABLE flattened_table_ids (
        instance_id int32,
        query_id int64,
        write_table_id int64,
        read_table_id int64,
        arrival_timestamp timestamp,
        query_type varchar)
"""
create_ingestion_intervals_per_table = """
    CREATE OR REPLACE TABLE ingestion_intervals_per_table (
        instance_id int32,
        query_id int64,
        write_table_id int64,
        current_timestamp timestamp,
        next_timestamp timestamp)
"""
create_output_table = """
    CREATE OR REPLACE TABLE output_table(
        instance_id int32,
        query_id int64,
        query_type varchar,
        write_table_id int64,
        read_table_id int64,
        arrival_timestamp timestamp,
        last_write_table_insert timestamp,
        next_write_table_insert timestamp,
        time_since_last_ingest_ms int64,
        time_to_next_ingest_ms int64
    )
"""

# view to count analytical queries vs. transform queries per table_id
create_view_tables_workload_count = """
    CREATE OR REPLACE VIEW tables_workload_count AS 
    WITH select_count_table AS (        
        SELECT --count select queries by read_table_ids
            instance_id,
            read_table_id AS table_read_by_select,
            COUNT(CASE WHEN query_type = 'select' THEN 1 END) AS select_count
        FROM output_table
        WHERE 1
            AND query_type = 'select'
            --AND instance_id = {instance_id}
        GROUP BY ALL
    ), transform_count_table AS (
        SELECT --count transformation queries by write_table_id
            instance_id,
            write_table_id AS table_transformed,
            COUNT(CASE WHEN query_type IN ('update', 'delete') THEN 1 END) AS transform_count
        FROM output_table
        WHERE 1
            AND query_type IN ('update', 'delete')
            --AND instance_id = {instance_id}
        GROUP BY ALL
    )
    SELECT 
        COALESCE(s.instance_id, t.instance_id) AS instance_id,
        COALESCE(t.table_transformed, s.table_read_by_select) AS table_id,
        t.transform_count,
        s.select_count,
    FROM select_count_table s
    FULL OUTER JOIN transform_count_table t
    ON t.table_transformed = s.table_read_by_select
    --WHERE instance_id = {instance_id}
"""


# Function to create tables if they don't exist
def create_tables():
    con.execute(create_flattened_table_ids_table)
    con.execute(create_ingestion_intervals_per_table)
    con.execute(create_output_table)
    con.execute(create_view_tables_workload_count)

create_tables()
consumer = Consumer({
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': 'analytics',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
        'enable.partition.eof': False,
    })

consumer.subscribe([TOPIC_FLAT_TABLES])

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

create_consumer(TOPIC_FLAT_TABLES,'liveanalytics')

def update_tables_periodically():
    start = datetime.strptime('2024-02-29 23:59:00', '%Y-%m-%d %H:%M:%S')
    end = datetime.strptime('2024-03-01 00:00:00', '%Y-%m-%d %H:%M:%S')


    while True:
        parquet_to_table(consumer,'flattened_table_ids',con,FLAT_COLUMNS,TOPIC_FLAT_TABLES)
        consumer.commit()
        check_duckdb_table('flattened_table_ids',con)
        #check_duckdb_table('output_table',con)
        check_duckdb_table('tables_workload_count',con)
        check_duckdb_table('output_table',con)

        query_current_max_timestamp = f"""
        SELECT MAX(arrival_timestamp)
        FROM flattened_table_ids
        """
        result = con.execute(query_current_max_timestamp).fetchone()
        con.execute("SELECT * FROM tables_workload_count") #update the view
        current_max_timestamp = result[0]  # Extract the value from the tuple
        print(current_max_timestamp)
        if current_max_timestamp  is None:
            continue
        if current_max_timestamp > datetime.strptime('2024-05-31 00:00:00', '%Y-%m-%d %H:%M:%S'):
            break
        
        print(start, 'then:', end)
        #print(con.execute(f"""SELECT {start}""").fetchone()[0])
        # if latest timestamp of inserted data has exceeded one hour after the
        # previous start time = end
        if (current_max_timestamp > end):
            print('hi again')
            # Define the query for inserting new data into the ingestion intervals table
            insert_into_ingestion_intervals_per_table = f"""
                INSERT INTO ingestion_intervals_per_table (
                    instance_id, 
                    query_id, 
                    write_table_id, 
                    current_timestamp, 
                    next_timestamp
                )   
                -- Insert logic based on the query and arrival timestamps
                SELECT DISTINCT
                    t1.instance_id,
                    t1.query_id,
                    t1.write_table_id,
                    t1.arrival_timestamp AS current_timestamp,
                    t2.arrival_timestamp AS next_timestamp
                FROM flattened_table_ids t1
                LEFT JOIN flattened_table_ids t2
                    ON t1.write_table_id = t2.write_table_id
                    AND t1.instance_id = t2.instance_id
                    AND t2.arrival_timestamp > t1.arrival_timestamp
                WHERE 1
                    AND t1.query_type IN ('insert', 'copy')
                    AND t1.arrival_timestamp BETWEEN '{start}' AND '{end}'
                    --AND t1.instance_id = {instance_id}
                    --AND t2.instance_id = {instance_id}
            """

            # Define the query for updating the ingestion intervals table

            # Insert data into the output table
            insert_into_output_table = f"""
            INSERT INTO output_table(
                    instance_id,
                    query_id,
                    query_type,
                    write_table_id,
                    read_table_id,
                    arrival_timestamp,
                    last_write_table_insert,
                    next_write_table_insert,
                    time_since_last_ingest_ms,
                    time_to_next_ingest_ms
                    )
            WITH output AS (
            SELECT 
                q.instance_id,
                q.query_id,
                q.query_type,
                q.write_table_id,
                q.read_table_id,
                q.arrival_timestamp,
                i.current_timestamp AS last_write_table_insert,
                i.next_timestamp AS next_write_table_insert
            FROM flattened_table_ids q
            LEFT JOIN ingestion_intervals_per_table i
                ON q.write_table_id = i.write_table_id
                AND q.query_id = i.query_id
                AND q.instance_id = i.instance_id
            WHERE 1
                --AND q.instance_id = {instance_id}
                --AND i.instance_id = {instance_id}
                AND q.arrival_timestamp BETWEEN '{start}' AND '{end}'
            ) 
            SELECT DISTINCT
                o.instance_id,
                o.query_id,
                o.query_type,
                o.write_table_id,
                o.read_table_id,
                o.arrival_timestamp,
                i.last_write_table_insert,
                i.next_write_table_insert,
                EPOCH_MS(o.arrival_timestamp - i.last_write_table_insert) AS time_since_last_ingest_ms,
                EPOCH_MS(i.next_write_table_insert - o.arrival_timestamp) AS time_to_next_ingest_ms   
            FROM output AS o
            JOIN output AS i
                ON i.query_type IN ('insert','copy') -- will make cartesian product with table i filtered by query_type
                AND o.arrival_timestamp BETWEEN 
                    i.last_write_table_insert --if no ingest query has run before, the query will be filtered out
                    AND COALESCE(i.next_write_table_insert, STRPTIME('31.12.2999', '%d.%m.%Y')::TIMESTAMP) --to handle NULL values
                AND o.instance_id = i.instance_id
                AND (
                    (o.query_type = 'select' AND o.read_table_id = i.write_table_id) -- a select query should match the read with the write table; if a write query hasn't run before, the corresponding select query will be filtered out
                    OR (o.query_type != 'select' AND o.write_table_id = i.write_table_id)  -- all other queries should math the write tables
                )
                AND i.arrival_timestamp BETWEEN '{start}' AND '{end}' 
            WHERE 1
                AND o.query_type NOT IN ('insert', 'copy') -- ingestion queries would show as duplicates therefore filter and add in the union all
                --AND o.instance_id = {instance_id}
                --AND i.instance_id = {instance_id}
                AND o.arrival_timestamp BETWEEN '{start}' AND '{end}'
            -- add back the ingestion queries
            UNION ALL  
            SELECT  
                instance_id,
                query_id,
                query_type,
                write_table_id,
                read_table_id,
                arrival_timestamp,
                last_write_table_insert,
                next_write_table_insert,
                EPOCH_MS(arrival_timestamp - last_write_table_insert) AS time_since_last_ingest_ms,
                EPOCH_MS(next_write_table_insert - arrival_timestamp) AS time_to_next_ingest_ms   
            FROM output
            WHERE 1
                AND query_type IN ('insert', 'copy')
                --AND instance_id = {instance_id}
                AND arrival_timestamp BETWEEN '{start}' AND '{end}'   
            """

            # Execute data insertions and updates
            con.execute(insert_into_ingestion_intervals_per_table)
            print('checkpoint1')
            #con.execute(update_ingestion_intervals_per_table)
            #print('checkpoint2')
            con.execute(insert_into_output_table)
            print('checkpoint3')
            consumer.commit()

            # # Wait for a specific interval before running the updates again
            # time.sleep()  # Update every 10 seconds (or your preferred interval)
            
            # # Start the table update in a separate thread to run in the background
            # update_thread = threading.Thread(target=update_tables_periodically, daemon=True)
            # update_thread.start()

            # st.write("Tables are being updated every 10 seconds with new data as it arrives.")
            time.sleep(5)
            build_historical_ingestion_table()
            build_select_queries_histogram()
            start = end + timedelta(hours=6)
            end = end + timedelta(minutes=10) + timedelta(hours=6)

historical_ingestion_table_placeholder = st.empty()

def build_historical_ingestion_table():
    """
    Fetches and displays a table with ingestion analytics for the selected instance.
    The SQL query filters on the given instance ID.
    """

    uniq_id = str(int(time.time()))

    query = f"""
        WITH analytical_tables AS (
            SELECT instance_id, table_id,
                CAST(select_count / NULLIF(transform_count + select_count, 0) AS DECIMAL(20, 2)) AS percentage_select_queries
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
        GROUP BY instance_id, read_table_id
        ORDER BY read_table_id
        LIMIT 12 
    """
    print('checkpoint5')
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
    historical_ingestion_table_placeholder.plotly_chart(fig, use_container_width=True,key=uniq_id)

consumer_stress = Consumer({
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': 'analytics',
    'auto.offset.reset': 'latest',
    'enable.auto.commit': False,
})
consumer_stress.subscribe(['stressindex'])

###############################################################################
@st.cache_data(ttl=10)  # Cache with TTL of 10 seconds
def build_select_queries_histogram():
    query = """
    WITH analytical_tables AS (
        SELECT instance_id, table_id,
                CAST(COALESCE(select_count / NULLIF(transform_count + select_count, 0), 0) AS DECIMAL(20, 2)) AS percentage_select_queries          
        FROM tables_workload_count
        WHERE (select_count / NULLIF(transform_count + select_count, 0)) > 0.80
    ), realtive_to_next_times AS (
        SELECT DISTINCT 
                instance_id, 
                query_id, 
                read_table_id,
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
    # Convert query results to a DataFrame
    result = con.execute(query).df()

    # If no data returned, bail out early
    if result.empty:
        st.write("No data available.")
        return

    # Get the unique read_table_ids
    read_table_ids = result["read_table_id"].unique()
    if len(read_table_ids) == 0:
        st.write("No read_table_id in the data.")
        return

    # Create a figure with one subplot per unique read_table_id
    fig, axes = plt.subplots(
        nrows=len(read_table_ids),
        ncols=1,
        figsize=(8, len(read_table_ids) * 4),
        sharex=True
    )

    # If there's only one read_table_id, axes won't be an array. Make it a list for consistency.
    if len(read_table_ids) == 1:
        axes = [axes]

    # Loop over read_table_ids and plot each in its own subplot
    for i, read_table_id in enumerate(read_table_ids):
        # Query again for just that read_table_id 
        # (or you could filter the existing "result" DataFrame directly)
        sub_query = f"""
        SELECT bin, count 
        FROM result
        WHERE read_table_id = {read_table_id}
        ORDER BY bin
        """
        subset = duckdb.sql(sub_query).df()

        axes[i].bar(
            subset["bin"],
            subset["count"],
            color="skyblue",
            edgecolor="black"
        )
        axes[i].set_title(f"Read Table ID: {read_table_id}")
        axes[i].set_ylabel("Count")
        axes[i].grid(axis='y', linestyle='--', alpha=0.7)

    # Only set x-axis label on the bottom subplot to avoid overlap
    axes[-1].set_xlabel("Relative time between ingestion queries")

    # Tidy up layout
    plt.tight_layout()

    # Display in Streamlit
    st.pyplot(plt.gcf())



def real_time_graph_in_historical_view():
    """
    Fetches real-time data from Kafka, updates the same Plotly figure in a simulation style,
    and continuously updates the displayed graph.
    """

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
        msg = consumer_stress.poll(timeout=.01)
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

        time.sleep(2)  # Wait 5 seconds before polling again

def parquet_to_table(consumer, table, conn, columns,topic):
    """
    Reads messages from a Kafka consumer, extracts data from JSON, writes to Parquet,
    and loads into a DuckDB table.
    
    Args:
        consumer: Kafka consumer instance.
        table: DuckDB table name.
        conn: DuckDB connection.
    """
    #print('hello')
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
    #print(df)
    # Save as Parquet
    df.to_parquet(parquet_file, index=False)
    #time.sleep(4)
    # Get absolute path for DuckDB compatibility
    parquet_path = os.path.abspath(parquet_file)

    # Load into DuckDB
    conn.execute(f"COPY {table} FROM '{parquet_path}' (FORMAT PARQUET)")
    consumer.commit()  # Commit offset to ensure that only new data is written

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
            print("Table Preview:", table_name)
            print(df_preview)
            
        else:
            print(f"Table '{table_name}' exists but is EMPTY.")

        return True, row_count

    except Exception as e:
        print(f"Error checking table '{table_name}': {e}")
        return False, 0


###############################################################################
# SHOW VISUALIZATIONS BASED ON VIEW MODE
###############################################################################
if view_mode == "Instance View":
    # Display the historical ingestion table filtered by the selected instance ID
    #table_fig = build_historical_ingestion_table(instance_id)
    #st.plotly_chart(table_fig, use_container_width=True)
    #build_select_queries_histogram()
    update_tables_periodically()
    # Start the real-time stress index graph in a separate thread to avoid blocking the main thread
    #stress_thread = threading.Thread(target=real_time_graph_in_historical_view, daemon=True)
    #stress_thread.start()    

# (Optionally, add code to handle "Aggregate View" here)


###############################################################################
# FOOTER
###############################################################################
st.markdown("""
    <footer style="text-align:center; font-size:12px; color:grey; padding-top:20px; border-top: 1px solid #e0e0e0; margin-top:20px;">
        <p>Pipeline Pioneers &copy; 2025 | UTN</p>
    </footer>
""", unsafe_allow_html=True)
