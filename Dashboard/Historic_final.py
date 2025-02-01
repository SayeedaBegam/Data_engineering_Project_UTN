import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import os
import duckdb
from datetime import datetime
from numerize import numerize
import time
# Set up the page layout
st.set_page_config(page_title="Redset Dashboard", page_icon="🌍", layout="wide")
st.header("Redset Dashboard")

# DuckDB Database File Path
DUCKDB_FILE = "cleaned_data.duckdb"

# Connect to the DuckDB database
con = duckdb.connect(DUCKDB_FILE)

# Load data from DuckDB
df = con.execute("SELECT * FROM cleaned_data").fetchdf()

# Strip any extra spaces in column names
df.columns = df.columns.str.strip()

# Sidebar configuration
st.sidebar.header("Menu")

# 1. Historical View / Live View Toggle
view_mode = st.sidebar.radio("Select View", ("Historical View", "Live View"))

# 2. File Size Display
file_size = os.path.getsize(DUCKDB_FILE)
st.sidebar.write(f"File Size: {numerize.numerize(file_size)}")

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
    # Initialize Kafka consumer (replace with actual consumer setup)
    conf = {
        'bootstrap.servers': 'localhost:9092',  # Kafka broker address
        'group.id': 'stress-index-consumer',
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(conf)
    consumer.subscribe(['TOPIC_FLAT_TABLES'])  

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



##################OTHER ANALYTICAL QUERIES############################
def historical_view_graphs():

# SQL Query for Analytical vs Transform Count
    analytical_vs_transform_count = """
        WITH select_count_table AS (        
            SELECT --count select queries by read_table_ids
                instance_id,
                read_table_id AS table_read_by_select,
                COUNT(CASE WHEN query_type = 'select' THEN 1 END) AS select_count
        FROM output_table
            WHERE query_type = 'select'
            AND read_table_id != 999999 -- only clean data, 999999 stands for Null
            AND instance_id = 0
            GROUP BY ALL
        ), transform_count_table AS (
            SELECT --count transformation queries by write_table_id
                instance_id,
                write_table_id AS table_transformed,
                COUNT(CASE WHEN query_type IN ('update', 'delete') THEN 1 END) AS transform_count
            FROM output_table
            WHERE query_type IN ('update', 'delete')
            AND write_table_id != 999999 -- only clean data, 999999 stands for Null
            AND instance_id = 0
            GROUP BY ALL        
        )
        SELECT 
            COALESCE(s.instance_id, t.instance_id) AS instance_id,
            COALESCE(t.table_transformed, s.table_read_by_select) AS table_id,
            t.transform_count,
            s.select_count
        FROM select_count_table s
        FULL OUTER JOIN transform_count_table t
        ON t.table_transformed = s.table_read_by_select
"""

# Streamlit UI
st.title("Real-Time Analytical vs Transform Count")

# Create an empty placeholder for the graph
graph_placeholder = st.empty()

# Function to fetch data and update the graph
def update_graph():
    # Execute the SQL query
    result_df = con.execute(analytical_vs_transform_count).fetchdf()

    # Clean the data
    result_df['table_id'] = result_df['table_id'].astype(str)
    result_df.fillna(0, inplace=True)

    # Create a bar chart comparing select_count and transform_count per table
    fig = px.bar(result_df, 
                 x='table_id', 
                 y=['select_count', 'transform_count'], 
                 title="Select vs Transform Counts by Table",
                 labels={'table_id': 'Table ID', 'value': 'Count'},
                 barmode='group')  # Group bars for select_count and transform_count

    # Update the placeholder with the new graph
    graph_placeholder.plotly_chart(fig, use_container_width=True)

# Simulate periodic updates (e.g., every 5 seconds)
if st.button("Start Real-Time Updates"):
    while True:
        update_graph()  # Update the graph with fresh data
        time.sleep(5)  # Sleep for 5 seconds (adjust based on how frequently you want to update)
else:
    st.write("Click the button to start real-time updates.")


    # ---- INSERT SQL ----
    
    
   
    

    # ---- INSERT SQL ---
    
    


    
    # ---- Now, generate visualizations using the data fetched via SQL queries ----
    
    

    # Display charts in parallel (side by side)
 



# Show content based on the selected view mode
if view_mode == "Historical View":
    display_metrics()  # Show the basic metrics
    historical_view_graphs()  # Show the historical view graphs
    real_time_graph_in_historical_view() #function calls the stress index

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
