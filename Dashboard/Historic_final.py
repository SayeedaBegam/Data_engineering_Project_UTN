import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import os
import duckdb
from datetime import datetime
from numerize import numerize

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

# Now, you can safely use .dt accessor on 'arrival_timestamp'
def historical_view_graphs():
    # ---- INSERT YOUR SQL QUERY HERE ----
    # Example: Query to get the distribution of query types
    sql_query_query_type_dist = """
        SELECT query_type, COUNT(*) AS count
        FROM cleaned_data
        GROUP BY query_type
    """
    
    # Execute the SQL query to get the distribution of query types
    query_type_dist = con.execute(sql_query_query_type_dist).fetchdf()
    
    # ---- INSERT SQL QUERY FOR EXECUTION TIME CATEGORIES ----
    # Example: Query to get execution time categories (Short, Medium, Long)
    sql_query_exec_time_category = """
        SELECT 
            CASE 
                WHEN execution_duration_ms <= 100 THEN 'Short'
                WHEN execution_duration_ms <= 1000 THEN 'Medium'
                ELSE 'Long'
            END AS execution_time_category, 
            COUNT(*) AS count
        FROM cleaned_data
        GROUP BY execution_time_category
    """
    
    # Execute the SQL query for execution time category
    execution_time_dist = con.execute(sql_query_exec_time_category).fetchdf()

    # ---- INSERT SQL QUERY FOR HOURLY QUERY COUNT ----
    # Example: Query to get hourly query count
    sql_query_hourly_query_count = """
        SELECT EXTRACT(HOUR FROM arrival_timestamp) AS hour, COUNT(*) AS query_count
        FROM cleaned_data
        GROUP BY hour
        ORDER BY hour
    """
    
    # Execute the SQL query for hourly query count
    query_count_per_hour = con.execute(sql_query_hourly_query_count).fetchdf()


    # Creates a table to store the times between ingestion per write_table_id
    # this table will be filled as an intermediate step in the analytics workflow
    create_ingestion_intervals_per_table = """
        CREATE OR REPLACE TABLE ingestion_intervals_per_table (
                   instance_id int32,
                   query_id int64,
                   write_table_id int64
                   current_timestamp timestamp,
                   next_timestamp timestamp)
    """
    # Create ingestion intervals table
    con.execute(create_ingestion_intervals_per_table)

    # Creates a table to store the flattened write and read table ids
    # Here the Kafka topic will write into.
    create_flattened_table_ids_table = """
        CREATE OR REPLACE TABLE flattened_table_ids(
            instance_id int32,
            query_id int64,
            write_table_id int64,
            read_table_id int64,
            arrival_timestamp timestamp,
            query_type varchar)
      """
     # Create flattend read and write table ids table
    con.execute(create_flattened_table_ids_table)
    
    # ---- Now, generate visualizations using the data fetched via SQL queries ----
    
    # Query Type Distribution (Pie Chart)
    fig_query_type = px.pie(query_type_dist, names='query_type', values='count', title="Query Type Distribution")
    
    # Execution Time Category Distribution (Donut Chart)
    fig_exec_time_donut = px.pie(execution_time_dist, names='execution_time_category', values='count', title="Query Execution Time Distribution", hole=0.3)

    # Hourly Query Count (Line Chart)
    fig_query_count_hourly = px.line(query_count_per_hour, x='hour', y='query_count', title="Query Count per Hour", labels={'hour': 'Hour of Day', 'query_count': 'Query Count'})

    # Display charts in parallel (side by side)
    col1, col2 = st.columns(2)
    with col1:
        st.plotly_chart(fig_query_type, use_container_width=True)
    with col2:
        st.plotly_chart(fig_exec_time_donut, use_container_width=True)

    # Display the second row of charts (2 parallel)
    col3, col4 = st.columns(2)
    with col3:
        st.plotly_chart(fig_query_count_hourly, use_container_width=True)
    with col4:
        st.plotly_chart(fig_query_count_hourly, use_container_width=True)


# Show content based on the selected view mode
if view_mode == "Historical View":
    display_metrics()  # Show the basic metrics
    historical_view_graphs()  # Show the historical view graphs
    
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
