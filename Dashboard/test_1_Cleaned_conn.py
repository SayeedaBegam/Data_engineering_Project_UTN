import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import os
from datetime import datetime
from numerize import numerize
import duckdb
import json
import time
import random

# Set up the page layout
st.set_page_config(page_title="Redset Dashboard", page_icon="🌍", layout="wide")
st.header("Redset Dashboard")

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

# Function to query data from DuckDB
def fetch_duckdb_data():
    """Fetch cleaned data from DuckDB."""
    conn = duckdb.connect(database='live_cleaned_data.duckdb001')  # Replace with your actual DuckDB database file path
    query = """
    SELECT * FROM live_query_metrics WHERE arrival_timestamp >= CURRENT_DATE
    """  # Modify query as needed to select relevant data
    df_duckdb = conn.execute(query).fetchdf()
    conn.close()
    return df_duckdb

# Function to display metrics
def display_metrics(df_selection, metric_placeholders):
    """Dynamically update the query metrics in real time with styled boxes."""
    total_query = len(df_selection)
    successful_query = len(df_selection[df_selection['was_aborted'] == False])
    aborted_query = len(df_selection[df_selection['was_aborted'] == True])
    cached_query = len(df_selection[df_selection['was_cached'] == True])
    total_scan_mbytes = df_selection['mbytes_scanned'].sum()
    total_spilled_mbytes = df_selection['mbytes_spilled'].sum()
    total_joins = df_selection['num_joins'].sum()
    total_aggregations = df_selection['num_aggregations'].sum()

    metric_placeholders["total_query"].markdown(f"""
        <div style="padding: 20px; background-color: #FFEBEE; border-left: 10px solid #D32F2F; border-radius: 10px; text-align: center; margin-bottom: 20px;">
            <h5>📊 Total Queries</h5>
            <h3>{total_query}</h3>
        </div>
    """, unsafe_allow_html=True)

    metric_placeholders["successful_query"].markdown(f"""
        <div style="padding: 20px; background-color: #E8F5E9; border-left: 10px solid #388E3C; border-radius: 10px; text-align: center; margin-bottom: 20px;">
            <h5>✅ Successful Queries</h5>
            <h3>{successful_query}</h3>
        </div>
    """, unsafe_allow_html=True)

    metric_placeholders["aborted_query"].markdown(f"""
        <div style="padding: 20px; background-color: #FFEBEE; border-left: 10px solid #D32F2F; border-radius: 10px; text-align: center; margin-bottom: 20px;">
            <h5>❌ Aborted Queries</h5>
            <h3>{aborted_query}</h3>
        </div>
    """, unsafe_allow_html=True)

    metric_placeholders["cached_query"].markdown(f"""
        <div style="padding: 20px; background-color: #E3F2FD; border-left: 10px solid #1976D2; border-radius: 10px; text-align: center; margin-bottom: 20px;">
            <h5>💾 Cached Queries</h5>
            <h3>{cached_query}</h3>
        </div>
    """, unsafe_allow_html=True)

    metric_placeholders["scan_mbytes"].markdown(f"""
        <div style="padding: 20px; background-color: #FFF3E0; border-left: 10px solid #F57C00; border-radius: 10px; text-align: center; margin-bottom: 20px;">
            <h5>📊 MBs Scanned</h5>
            <h3>{total_scan_mbytes} MB</h3>
        </div>
    """, unsafe_allow_html=True)

    metric_placeholders["spilled_mbytes"].markdown(f"""
        <div style="padding: 20px; background-color: #FBE9E7; border-left: 10px solid #E53935; border-radius: 10px; text-align: center; margin-bottom: 20px;">
            <h5>💡 MBs Spilled</h5>
            <h3>{total_spilled_mbytes} MB</h3>
        </div>
    """, unsafe_allow_html=True)

    metric_placeholders["total_joins"].markdown(f"""
        <div style="padding: 20px; background-color: #E8F5E9; border-left: 10px solid #2E7D32; border-radius: 10px; text-align: center; margin-bottom: 20px;">
            <h5>🔗 Total Joins</h5>
            <h3>{total_joins}</h3>
        </div>
    """, unsafe_allow_html=True)

    metric_placeholders["total_aggregations"].markdown(f"""
        <div style="padding: 20px; background-color: #F3E5F5; border-left: 10px solid #8E24AA; border-radius: 10px; text-align: center; margin-bottom: 20px;">
            <h5>🔢 Total Aggregations</h5>
            <h3>{total_aggregations}</h3>
        </div>
    """, unsafe_allow_html=True)

# For Live View, fetch data from DuckDB
if view_mode == "Live View":
    df_selection = fetch_duckdb_data()

# For Historical View, fetch data from DuckDB and apply the necessary filtering
if view_mode == "Historical View":
    df_selection = fetch_duckdb_data()
    df_selection = df_selection[(df_selection['arrival_timestamp'] >= start_date) & (df_selection['arrival_timestamp'] <= end_date)]

# Placeholder function for live view
def historical_live_view_graphs():
    pass  # Use the relevant function to display historical and live view graphs

# Run the dashboard code with these updates
# Show content based on the selected view mode
if view_mode == "Historical View":
    display_metrics(df_selection, metric_placeholders)
    historical_live_view_graphs()
elif view_mode == "Live View":
    display_metrics(df_selection, metric_placeholders)
    historical_live_view_graphs()

# Footer
st.markdown("""
    <footer style="text-align:center; font-size:12px; color:grey; padding-top:20px; border-top: 1px solid #e0e0e0; margin-top:20px;">
        <p>Pipeline Pioneers &copy; 2025 | UTN</p>
    </footer>
""", unsafe_allow_html=True)

# Hide Streamlit elements
hide_st_style = """
<style>
#MainMenu {visibility:hidden;}
footer
header {visibility:hidden;}
</style>
"""
st.markdown(hide_st_style, unsafe_allow_html=True)
