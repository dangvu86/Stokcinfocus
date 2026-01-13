import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime

import config
import drive_utils
from database import DatabaseManager

# Page Config
st.set_page_config(
    page_title="Stock In Focus",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize DB Manager
db_manager = DatabaseManager()

# --- HELPER FUNCTIONS ---
def load_data(force_refresh=False):
    # Auto-download if DB is missing (First run on Cloud) or forced
    if not config.DB_PATH.exists() or force_refresh:
        with st.spinner("Initializing Data..." if not force_refresh else "Downloading latest data..."):
            success, msg = drive_utils.check_and_update_db()
            if success:
                if force_refresh: st.success(msg)
                st.cache_data.clear()
            else:
                st.error(f"Sync Failed: {msg}")
                return pd.DataFrame()
    
    return db_manager.get_all_stocks()

# --- SIDEBAR ---
with st.sidebar:
    st.header("Stock In Focus")
    if st.button("🔄 Refresh Data"):
        df = load_data(force_refresh=True)
    else:
        df = load_data()

    st.divider()
    
    # Filters
    st.subheader("Filters")
    
    # Year Filter (Derived from data)
    years = sorted(df['Pick_Date'].dt.year.unique().tolist(), reverse=True) if not df.empty else []
    selected_years = st.multiselect("Pick Year", years, default=years)
    
    # Status Filter
    status_options = ["Active", "Closed", "All"]
    selected_status = st.radio("Status", status_options, index=2) # Default All
    
    # Validation Filter
    min_conviction = st.slider("Min Short-Term Conviction", 1, 5, 3)

# --- MAIN PAGE ---
st.title("📊 Portfolio Monitor")

if df.empty:
    st.warning("No data available. Please check database connection.")
    st.stop()

# --- KEY METRICS (Active Only) ---
active_df = df[df['IsClosed'] == 0].copy()
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Total Active Picks", len(active_df))
with col2:
    if not active_df.empty:
        avg_ret = active_df['Stock_Ret'].mean()
        st.metric("Avg Return (Active)", f"{avg_ret:.2%}", delta_color="normal")
with col3:
    if not active_df.empty:
        avg_alpha = active_df['Alpha'].mean()
        st.metric("Avg Alpha (Active)", f"{avg_alpha:.2%}")
with col4:
    last_update = df['Action_Date'].max()
    st.date_input("Last Data Update", value=last_update, disabled=True)

st.divider()

# --- YEARLY SUMMARY ---
st.subheader("🗓️ Yearly Performance Summary")
summary_df = db_manager.get_yearly_summary(df)

# Formatting for display
st.dataframe(
    summary_df,
    hide_index=True,
    column_config={
        "Year": "Year",
        "Avg Return": st.column_config.NumberColumn(format="%.2f%%"),
        "Avg Alpha": st.column_config.NumberColumn(format="%.2f%%"),
    },
    use_container_width=True
)

# --- DETAIL TABLE ---
st.subheader("📋 Detail List")

# Apply Filters
filtered_df = df.copy()

# Filter Year
if selected_years:
    filtered_df = filtered_df[filtered_df['Pick_Date'].dt.year.isin(selected_years)]

# Filter Status
if selected_status == "Active":
    filtered_df = filtered_df[filtered_df['IsClosed'] == 0]
elif selected_status == "Closed":
    filtered_df = filtered_df[filtered_df['IsClosed'] == 1]

# Filter Conviction
filtered_df = filtered_df[filtered_df['ShortTermConviction'] >= min_conviction]

# Ensure Action_Date is formatted as string for display
# This prevents Styler/ColumnConfig mismatch issues
filtered_df['Action_Date_Str'] = filtered_df['Action_Date'].apply(lambda x: x.strftime('%d-%m-%Y') if pd.notnull(x) else "")

# Map IsClosed to String
filtered_df['Status'] = filtered_df['IsClosed'].map({0: 'Active', 1: 'Closed'})

# Styling function for Rating and Returns
def style_table(styler):
    def color_val(val):
        if pd.isna(val): return ''
        color = 'green' if val > 0 else '#d32f2f' if val < 0 else 'black'
        return f'color: {color}'
    
    def color_rating(val):
        if val == "Outperform": return 'color: green; font-weight: bold'
        if val == "Underperform": return 'color: #d32f2f; font-weight: bold'
        return 'color: gray'

    def color_status(val):
        return 'color: green; font-weight: bold' if val == 'Active' else 'color: gray'

    return (styler
        .format({
            'Pick_Date': '{:%Y-%m-%d}',
            'Price_At_Call': '{:,.0f}',
            'LastPrice': '{:,.0f}',
            'Stock_Ret': '{:.2%}',
            'VNI_Ret': '{:.2%}',
            'Alpha': '{:.2%}'
        })
        .map(color_val, subset=['Stock_Ret', 'Alpha'])
        .map(color_rating, subset=['Rating'])
        .map(color_status, subset=['Status'])
    )

# Select Columns for Display (Use Action_Date_Str)
display_cols = [
    'Ticker', 'Pick_Date', 'Price_At_Call', 
    'Action_Date_Str', 'LastPrice', 
    'Stock_Ret', 'VNI_Ret', 'Alpha', 'Rating', 
    'Status'
]

# Display Table
st.dataframe(
    style_table(filtered_df[display_cols].style),
    use_container_width=True,
    height=600,
    column_config={
        "Action_Date_Str": "Close/Action Date",
        "Stock_Ret": st.column_config.NumberColumn("Stock Return"), # Remove format, let Styler handle it
        "VNI_Ret": st.column_config.NumberColumn("Index Return"),   # Remove format
        "Pick_Date": st.column_config.DateColumn("Pick Date", format="DD-MM-YYYY"),
    }
)

# --- FOOTER ---
if st.checkbox("Show Raw Data"):
    st.write(df)
