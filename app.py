import streamlit as st
import pyodbc
import pandas as pd
import os
import json
import io
from datetime import datetime, date
from dotenv import load_dotenv
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, JsCode

# Load environment variables
load_dotenv()

# Page config
st.set_page_config(
    page_title="Database Explorer",
    page_icon="📊",
    layout="wide"
)

# Initialize session state for saved queries
if 'saved_queries' not in st.session_state:
    st.session_state.saved_queries = {}

if 'query_result' not in st.session_state:
    st.session_state.query_result = None

def get_db_connection(database_name: str):
    """Create database connection"""
    server = os.getenv("DB_SERVER")
    username = os.getenv("DB_USERNAME")
    password = os.getenv("DB_PASSWORD")

    if not all([server, database_name, username, password]):
        return None, f"Missing environment variables for {database_name}"

    conn_str = f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={server};DATABASE={database_name};UID={username};PWD={password}'

    try:
        connection = pyodbc.connect(conn_str)
        return connection, None
    except pyodbc.Error as ex:
        return None, str(ex)

def get_all_tables(conn):
    """Get list of all tables in the database"""
    query = """
        SELECT TABLE_SCHEMA, TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE = 'BASE TABLE'
        ORDER BY TABLE_SCHEMA, TABLE_NAME
    """
    df = pd.read_sql(query, conn)
    df['FULL_NAME'] = df['TABLE_SCHEMA'] + '.' + df['TABLE_NAME']
    return df

def get_primary_keys(conn, schema, table_name):
    """Get list of primary key columns for a table"""
    query = """
        SELECT 
            c.COLUMN_NAME
        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
        JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE c 
            ON tc.CONSTRAINT_NAME = c.CONSTRAINT_NAME
            AND tc.TABLE_SCHEMA = c.TABLE_SCHEMA
            AND tc.TABLE_NAME = c.TABLE_NAME
        WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
            AND tc.TABLE_SCHEMA = ?
            AND tc.TABLE_NAME = ?
    """
    df = pd.read_sql(query, conn, params=[schema, table_name])
    return df['COLUMN_NAME'].tolist()

def get_foreign_keys(conn, schema, table_name):
    """Get foreign key information for a table"""
    query = """
        SELECT 
            kcu.COLUMN_NAME,
            ccu.TABLE_SCHEMA AS REFERENCED_SCHEMA,
            ccu.TABLE_NAME AS REFERENCED_TABLE,
            ccu.COLUMN_NAME AS REFERENCED_COLUMN
        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
        JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
            ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
            AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
            AND tc.TABLE_NAME = kcu.TABLE_NAME
        JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
            ON tc.CONSTRAINT_NAME = rc.CONSTRAINT_NAME
        JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE ccu
            ON rc.UNIQUE_CONSTRAINT_NAME = ccu.CONSTRAINT_NAME
        WHERE tc.CONSTRAINT_TYPE = 'FOREIGN KEY'
            AND tc.TABLE_SCHEMA = ?
            AND tc.TABLE_NAME = ?
    """
    df = pd.read_sql(query, conn, params=[schema, table_name])
    return df

def get_table_columns(conn, schema, table_name):
    """Get columns information for a specific table including key types"""
    query = """
        SELECT
            COLUMN_NAME,
            DATA_TYPE,
            CHARACTER_MAXIMUM_LENGTH,
            IS_NULLABLE,
            COLUMN_DEFAULT
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        ORDER BY ORDINAL_POSITION
    """
    df = pd.read_sql(query, conn, params=[schema, table_name])
    
    # Get primary keys and foreign keys
    pk_columns = get_primary_keys(conn, schema, table_name)
    fk_df = get_foreign_keys(conn, schema, table_name)
    fk_columns = fk_df['COLUMN_NAME'].tolist() if not fk_df.empty else []
    
    # Add KEY_TYPE column
    def get_key_type(col_name):
        key_types = []
        if col_name in pk_columns:
            key_types.append('PK')
        if col_name in fk_columns:
            key_types.append('FK')
        return ', '.join(key_types) if key_types else '-'
    
    df['KEY_TYPE'] = df['COLUMN_NAME'].apply(get_key_type)
    
    # Add FK reference info
    def get_fk_reference(col_name):
        if fk_df.empty:
            return '-'
        ref = fk_df[fk_df['COLUMN_NAME'] == col_name]
        if not ref.empty:
            return f"{ref.iloc[0]['REFERENCED_SCHEMA']}.{ref.iloc[0]['REFERENCED_TABLE']}.{ref.iloc[0]['REFERENCED_COLUMN']}"
        return '-'
    
    df['FK_REFERENCE'] = df['COLUMN_NAME'].apply(get_fk_reference)
    
    return df

def get_date_columns(df_columns):
    """Get list of date/datetime columns from table schema"""
    date_types = ['date', 'datetime', 'datetime2', 'smalldatetime', 'datetimeoffset']
    date_cols = df_columns[df_columns['DATA_TYPE'].isin(date_types)]['COLUMN_NAME'].tolist()
    return date_cols

def get_date_range(conn, schema, table_name, date_column):
    """Get min and max dates from a date column"""
    try:
        query = f"SELECT MIN([{date_column}]) as min_date, MAX([{date_column}]) as max_date FROM [{schema}].[{table_name}] WHERE [{date_column}] IS NOT NULL"
        result = pd.read_sql(query, conn)
        return result['min_date'][0], result['max_date'][0], None
    except Exception as e:
        return None, None, str(e)

def get_table_preview(conn, schema, table_name, limit=100, date_column=None, start_date=None, end_date=None):
    """Get preview of table data with optional date filtering"""
    try:
        query = f"SELECT TOP {limit} * FROM [{schema}].[{table_name}]"

        # Add date filter if specified
        if date_column and start_date and end_date:
            query += f" WHERE [{date_column}] BETWEEN ? AND ?"
            df = pd.read_sql(query, conn, params=[start_date, end_date])
        else:
            df = pd.read_sql(query, conn)

        return df, None
    except Exception as e:
        return None, str(e)

def execute_custom_query(conn, query):
    """Execute custom SQL query"""
    try:
        df = pd.read_sql(query, conn)
        return df, None
    except Exception as e:
        return None, str(e)

def export_to_excel(df):
    """Export DataFrame to Excel in memory"""
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name='Data', index=False)
    return output.getvalue()

def export_to_csv(df):
    """Export DataFrame to CSV"""
    return df.to_csv(index=False).encode('utf-8')

def get_income_statement_isa22_pivot(conn):
    """Query ISA22 from 4 Income Statement tables and create pivot table"""
    table_sources = {
        'S_SPS_IncomeStatement_Bank': 'Bank',
        'S_SPS_IncomeStatement_Company': 'Company',
        'S_SPS_IncomeStatement_Insurance': 'Insurance',
        'S_SPS_IncomeStatement_Security': 'Securities'
    }
    
    all_data = []
    
    for table, source in table_sources.items():
        query = f"""
            SELECT 
                TICKER,
                ENDDATE,
                YEARREPORT,
                LENGTHREPORT,
                ISA22
            FROM [dbo].[{table}]
            WHERE ISA22 IS NOT NULL AND TICKER IS NOT NULL AND TICKER != '' AND LENGTHSERIES = 3
            ORDER BY TICKER, ENDDATE DESC
        """
        try:
            df = pd.read_sql(query, conn)
            df['SOURCE'] = source
            all_data.append(df)
        except Exception as e:
            continue
    
    if not all_data:
        return None, "No data found"
    
    # Combine all data
    df_combined = pd.concat(all_data, ignore_index=True)
    
    # Convert ENDDATE to string for column names
    df_combined['ENDDATE'] = pd.to_datetime(df_combined['ENDDATE']).dt.strftime('%Y-%m-%d')
    
    # Convert ISA22 to billions (ty)
    df_combined['ISA22'] = df_combined['ISA22'] / 1e9
    
    # Pivot the data
    df_pivot = df_combined.pivot_table(
        index=['TICKER', 'SOURCE'],
        columns='ENDDATE',
        values='ISA22',
        aggfunc='first'
    ).reset_index()
    
    # Sort by ticker
    df_pivot = df_pivot.sort_values('TICKER')
    
    return df_pivot, None

# Query templates
QUERY_TEMPLATES = {
    "Simple SELECT": "SELECT TOP 100 * FROM [schema].[table_name]",
    "JOIN Two Tables": """SELECT TOP 100
    t1.*,
    t2.*
FROM [schema].[table1] t1
INNER JOIN [schema].[table2] t2 ON t1.id = t2.id""",
    "Date Range Filter": """SELECT *
FROM [schema].[table_name]
WHERE [date_column] BETWEEN '2024-01-01' AND '2024-12-31'""",
    "Aggregation": """SELECT
    [group_column],
    COUNT(*) as count,
    AVG([value_column]) as avg_value
FROM [schema].[table_name]
GROUP BY [group_column]""",
    "Recommendation + Price JOIN": """SELECT TOP 1000
    r.Ticker,
    r.Date,
    r.Recommendation,
    p.TRADE_DATE,
    p.PX_LAST as Price
FROM [dbo].[ForecastVersionChange] r
LEFT JOIN [dbo].[S_BBG_DATA_DWH_ADJUSTED] p
    ON r.Ticker = p.PRIMARYSECID
    AND CAST(r.Date AS DATE) = CAST(p.TRADE_DATE AS DATE)
WHERE r.ItemId IN (SELECT Id FROM Item WHERE KeyCode = 'Recommendation')
ORDER BY r.Date DESC"""
}

# Main UI
st.title("📊 Database Explorer Pro")
st.markdown("---")

# Create main navigation
main_tab1, main_tab2, main_tab3, main_tab4, main_tab5 = st.tabs([
    "📋 Table Explorer",
    "💻 SQL Query Editor",
    "🔗 Table Join Wizard",
    "🔍 Advanced Filters",
    "📈 Income Statement ISA22"
])

# Database selection (common for all tabs)
db_options = {
    "DiamondDB (Recommendations)": os.getenv("DB_NAME_REC"),
    "DiamondDWH (Price Data)": os.getenv("DB_NAME_PRICE")
}

# ========== TAB 1: TABLE EXPLORER ==========
with main_tab1:
    selected_db_label = st.selectbox("Select Database", list(db_options.keys()), key="tab1_db")
    selected_db = db_options[selected_db_label]

    if selected_db:
        with st.spinner(f"Connecting to {selected_db}..."):
            conn, error = get_db_connection(selected_db)

        if error:
            st.error(f"Connection error: {error}")
        else:
            st.success(f"✅ Connected to {selected_db}")

            try:
                df_tables = get_all_tables(conn)
                st.subheader(f"📋 Tables in {selected_db}")
                st.metric("Total Tables", len(df_tables))

                with st.expander("View All Tables", expanded=True):
                    st.dataframe(
                        df_tables[['TABLE_SCHEMA', 'TABLE_NAME']],
                        use_container_width=True,
                        height=300
                    )

                st.markdown("---")
                st.subheader("🔍 Explore Table Details")

                table_list = df_tables['FULL_NAME'].tolist()
                selected_table_full = st.selectbox(
                    "Select a table to view details",
                    ["-- Select a table --"] + table_list,
                    key="table_selector"
                )

                if selected_table_full and selected_table_full != "-- Select a table --":
                    schema, table_name = selected_table_full.split('.', 1)  # maxsplit=1 to handle table names with dots

                    tab1, tab2, tab3 = st.tabs(["📝 Columns", "👁️ Data Preview", "📊 Statistics"])

                    with tab1:
                        st.markdown(f"### Columns in `{selected_table_full}`")
                        df_columns = get_table_columns(conn, schema, table_name)
                        df_columns['CHARACTER_MAXIMUM_LENGTH'] = df_columns['CHARACTER_MAXIMUM_LENGTH'].fillna('-')
                        df_columns['COLUMN_DEFAULT'] = df_columns['COLUMN_DEFAULT'].fillna('-')

                        st.dataframe(
                            df_columns,
                            use_container_width=True,
                            column_config={
                                "COLUMN_NAME": "Column Name",
                                "DATA_TYPE": "Data Type",
                                "CHARACTER_MAXIMUM_LENGTH": "Max Length",
                                "IS_NULLABLE": "Nullable",
                                "COLUMN_DEFAULT": "Default Value",
                                "KEY_TYPE": st.column_config.TextColumn(
                                    "Key Type",
                                    help="PK = Primary Key, FK = Foreign Key"
                                ),
                                "FK_REFERENCE": st.column_config.TextColumn(
                                    "FK Reference",
                                    help="Referenced table.column for Foreign Keys"
                                )
                            }
                        )
                        st.metric("Total Columns", len(df_columns))

                    with tab2:
                        st.markdown(f"### Data Preview")
                        date_columns = get_date_columns(df_columns)

                        col1, col2 = st.columns([1, 2])
                        with col1:
                            limit = st.number_input("Number of rows", min_value=10, max_value=10000, value=100, step=10)

                        date_filter_enabled = False
                        selected_date_col = None
                        start_date = None
                        end_date = None

                        if date_columns:
                            st.markdown("#### 📅 Date Range Filter (Optional)")
                            col3, col4, col5 = st.columns(3)

                            with col3:
                                selected_date_col = st.selectbox(
                                    "Select date column",
                                    ["-- No filter --"] + date_columns,
                                    key="date_col_selector"
                                )

                            if selected_date_col and selected_date_col != "-- No filter --":
                                with st.spinner(f"Loading date range for {selected_date_col}..."):
                                    min_date, max_date, date_error = get_date_range(conn, schema, table_name, selected_date_col)

                                if date_error:
                                    st.warning(f"Could not load date range: {date_error}")
                                elif min_date and max_date:
                                    if hasattr(min_date, 'date'):
                                        min_date_display = min_date.date()
                                    else:
                                        min_date_display = min_date

                                    if hasattr(max_date, 'date'):
                                        max_date_display = max_date.date()
                                    else:
                                        max_date_display = max_date

                                    with col4:
                                        start_date = st.date_input(
                                            "Start date",
                                            value=min_date_display,
                                            min_value=min_date_display,
                                            max_value=max_date_display,
                                            key="start_date"
                                        )

                                    with col5:
                                        end_date = st.date_input(
                                            "End date",
                                            value=max_date_display,
                                            min_value=min_date_display,
                                            max_value=max_date_display,
                                            key="end_date"
                                        )

                                    date_filter_enabled = True
                                    st.info(f"Available date range: {min_date_display} to {max_date_display}")

                        st.markdown("---")

                        if st.button("Load Preview", key="load_preview_btn"):
                            with st.spinner("Loading data..."):
                                if date_filter_enabled:
                                    df_preview, preview_error = get_table_preview(
                                        conn, schema, table_name, limit,
                                        selected_date_col, start_date, end_date
                                    )
                                else:
                                    df_preview, preview_error = get_table_preview(conn, schema, table_name, limit)

                            if preview_error:
                                st.error(f"Error loading preview: {preview_error}")
                            else:
                                st.dataframe(df_preview, use_container_width=True, height=400)
                                info_text = f"Showing {len(df_preview)} rows"
                                if date_filter_enabled:
                                    info_text += f" (filtered by {selected_date_col}: {start_date} to {end_date})"
                                st.info(info_text)

                                # Export options
                                st.markdown("#### 📥 Export Data")
                                col_exp1, col_exp2 = st.columns(2)
                                with col_exp1:
                                    excel_data = export_to_excel(df_preview)
                                    st.download_button(
                                        label="📊 Download as Excel",
                                        data=excel_data,
                                        file_name=f"{selected_table_full}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                                    )
                                with col_exp2:
                                    csv_data = export_to_csv(df_preview)
                                    st.download_button(
                                        label="📄 Download as CSV",
                                        data=csv_data,
                                        file_name=f"{selected_table_full}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                                        mime="text/csv"
                                    )

                    with tab3:
                        st.markdown(f"### Table Statistics")
                        if st.button("Load Statistics"):
                            with st.spinner("Calculating statistics..."):
                                try:
                                    count_query = f"SELECT COUNT(*) as row_count FROM [{schema}].[{table_name}]"
                                    row_count = pd.read_sql(count_query, conn)['row_count'][0]

                                    col1, col2, col3 = st.columns(3)
                                    with col1:
                                        st.metric("Total Rows", f"{row_count:,}")
                                    with col2:
                                        st.metric("Total Columns", len(df_columns))
                                    with col3:
                                        nullable_cols = len(df_columns[df_columns['IS_NULLABLE'] == 'YES'])
                                        st.metric("Nullable Columns", nullable_cols)

                                except Exception as e:
                                    st.error(f"Error calculating statistics: {e}")

            except Exception as e:
                st.error(f"Error loading tables: {e}")
            finally:
                conn.close()

# ========== TAB 2: SQL QUERY EDITOR ==========
with main_tab2:
    st.markdown("### 💻 Custom SQL Query Editor")
    st.info("Write custom SQL queries to JOIN multiple tables, filter data, and perform complex operations.")

    selected_db_label_sql = st.selectbox("Select Database", list(db_options.keys()), key="tab2_db")
    selected_db_sql = db_options[selected_db_label_sql]

    # Query templates
    col_temp1, col_temp2 = st.columns([2, 1])
    with col_temp1:
        template_choice = st.selectbox("📑 Load Query Template", ["-- Custom Query --"] + list(QUERY_TEMPLATES.keys()))

    with col_temp2:
        st.markdown("<br>", unsafe_allow_html=True)
        if st.button("📋 Load Template"):
            if template_choice != "-- Custom Query --":
                st.session_state['query_text'] = QUERY_TEMPLATES[template_choice]
                st.rerun()

    # SQL Query input
    if 'query_text' not in st.session_state:
        st.session_state['query_text'] = "SELECT TOP 100 * FROM [dbo].[table_name]"

    query = st.text_area(
        "SQL Query",
        value=st.session_state['query_text'],
        height=200,
        help="Write your SQL query here. Use TOP to limit results."
    )

    col_btn1, col_btn2, col_btn3 = st.columns([1, 1, 4])
    with col_btn1:
        execute_btn = st.button("▶️ Execute Query", type="primary")
    with col_btn2:
        save_query_btn = st.button("💾 Save Query")

    # Save query functionality
    if save_query_btn:
        query_name = st.text_input("Query Name", key="save_query_name")
        if query_name:
            st.session_state.saved_queries[query_name] = query
            st.success(f"✅ Query saved as '{query_name}'")

    # Load saved queries
    if st.session_state.saved_queries:
        st.markdown("#### 📚 Saved Queries")
        saved_query_name = st.selectbox("Load Saved Query", ["-- Select --"] + list(st.session_state.saved_queries.keys()))
        if saved_query_name != "-- Select --":
            if st.button("📥 Load Saved Query"):
                st.session_state['query_text'] = st.session_state.saved_queries[saved_query_name]
                st.rerun()

    # Execute query
    if execute_btn and query.strip():
        with st.spinner(f"Connecting to {selected_db_sql}..."):
            conn_sql, error_sql = get_db_connection(selected_db_sql)

        if error_sql:
            st.error(f"Connection error: {error_sql}")
        else:
            with st.spinner("Executing query..."):
                df_result, query_error = execute_custom_query(conn_sql, query)

            if query_error:
                st.error(f"❌ Query Error: {query_error}")
            else:
                st.success(f"✅ Query executed successfully! {len(df_result)} rows returned.")
                st.dataframe(df_result, use_container_width=True, height=400)

                st.session_state.query_result = df_result

                # Export options
                st.markdown("#### 📥 Export Results")
                col_exp1, col_exp2 = st.columns(2)
                with col_exp1:
                    excel_data = export_to_excel(df_result)
                    st.download_button(
                        label="📊 Download as Excel",
                        data=excel_data,
                        file_name=f"query_result_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key="download_excel_query"
                    )
                with col_exp2:
                    csv_data = export_to_csv(df_result)
                    st.download_button(
                        label="📄 Download as CSV",
                        data=csv_data,
                        file_name=f"query_result_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv",
                        key="download_csv_query"
                    )

            conn_sql.close()

# ========== TAB 3: TABLE JOIN WIZARD ==========
with main_tab3:
    st.markdown("### 🔗 Visual Table Join Wizard")
    st.info("Select multiple tables and join them visually without writing SQL.")

    selected_db_label_join = st.selectbox("Select Database", list(db_options.keys()), key="tab3_db")
    selected_db_join = db_options[selected_db_label_join]

    if selected_db_join:
        with st.spinner(f"Connecting to {selected_db_join}..."):
            conn_join, error_join = get_db_connection(selected_db_join)

        if error_join:
            st.error(f"Connection error: {error_join}")
        else:
            df_tables_join = get_all_tables(conn_join)
            table_list_join = df_tables_join['FULL_NAME'].tolist()

            col_j1, col_j2 = st.columns(2)

            with col_j1:
                st.markdown("#### Table 1 (Main)")
                table1 = st.selectbox("Select first table", table_list_join, key="join_table1")
                if table1:
                    schema1, name1 = table1.split('.', 1)  # maxsplit=1 to handle table names with dots
                    df_cols1 = get_table_columns(conn_join, schema1, name1)
                    cols1 = df_cols1['COLUMN_NAME'].tolist()
                    selected_cols1 = st.multiselect("Select columns from Table 1", cols1, default=cols1[:5] if len(cols1) >= 5 else cols1, key="cols1")
                    join_key1 = st.selectbox("Join Key (Table 1)", cols1, key="join_key1")

            with col_j2:
                st.markdown("#### Table 2 (Join)")
                table2 = st.selectbox("Select second table", table_list_join, key="join_table2")
                if table2:
                    schema2, name2 = table2.split('.', 1)  # maxsplit=1 to handle table names with dots
                    df_cols2 = get_table_columns(conn_join, schema2, name2)
                    cols2 = df_cols2['COLUMN_NAME'].tolist()
                    selected_cols2 = st.multiselect("Select columns from Table 2", cols2, default=cols2[:5] if len(cols2) >= 5 else cols2, key="cols2")
                    join_key2 = st.selectbox("Join Key (Table 2)", cols2, key="join_key2")

            join_type = st.selectbox("Join Type", ["INNER JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL OUTER JOIN"])
            limit_join = st.number_input("Limit rows", min_value=10, max_value=10000, value=100, key="limit_join")

            if st.button("🔗 Execute Join", type="primary"):
                if table1 and table2 and selected_cols1 and selected_cols2:
                    # Build column list - use name1/name2 which already handled the split correctly
                    cols1_str = ", ".join([f"t1.[{c}] as [{name1}_{c}]" for c in selected_cols1])
                    cols2_str = ", ".join([f"t2.[{c}] as [{name2}_{c}]" for c in selected_cols2])

                    join_query = f"""
                    SELECT TOP {limit_join}
                        {cols1_str},
                        {cols2_str}
                    FROM [{schema1}].[{name1}] t1
                    {join_type} [{schema2}].[{name2}] t2
                        ON t1.[{join_key1}] = t2.[{join_key2}]
                    """

                    st.code(join_query, language="sql")

                    with st.spinner("Executing join..."):
                        df_join_result, join_error = execute_custom_query(conn_join, join_query)

                    if join_error:
                        st.error(f"❌ Join Error: {join_error}")
                    else:
                        st.success(f"✅ Join executed successfully! {len(df_join_result)} rows returned.")
                        st.dataframe(df_join_result, use_container_width=True, height=400)

                        # Export options
                        st.markdown("#### 📥 Export Results")
                        col_exp1, col_exp2 = st.columns(2)
                        with col_exp1:
                            excel_data = export_to_excel(df_join_result)
                            st.download_button(
                                label="📊 Download as Excel",
                                data=excel_data,
                                file_name=f"join_result_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                key="download_excel_join"
                            )
                        with col_exp2:
                            csv_data = export_to_csv(df_join_result)
                            st.download_button(
                                label="📄 Download as CSV",
                                data=csv_data,
                                file_name=f"join_result_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                                mime="text/csv",
                                key="download_csv_join"
                            )
                else:
                    st.warning("⚠️ Please select both tables and at least one column from each.")

            conn_join.close()

# ========== TAB 4: ADVANCED FILTERS ==========
with main_tab4:
    st.markdown("### 🔍 Advanced Multi-Column Filters")
    st.info("Apply complex filters with multiple conditions across different columns.")

    selected_db_label_filter = st.selectbox("Select Database", list(db_options.keys()), key="tab4_db")
    selected_db_filter = db_options[selected_db_label_filter]

    if selected_db_filter:
        with st.spinner(f"Connecting to {selected_db_filter}..."):
            conn_filter, error_filter = get_db_connection(selected_db_filter)

        if error_filter:
            st.error(f"Connection error: {error_filter}")
        else:
            df_tables_filter = get_all_tables(conn_filter)
            table_list_filter = df_tables_filter['FULL_NAME'].tolist()

            selected_table_filter = st.selectbox("Select Table", table_list_filter, key="filter_table")

            if selected_table_filter:
                schema_f, name_f = selected_table_filter.split('.', 1)  # maxsplit=1 to handle table names with dots
                df_cols_filter = get_table_columns(conn_filter, schema_f, name_f)

                st.markdown("#### Build Filter Conditions")

                num_filters = st.number_input("Number of filter conditions", min_value=1, max_value=5, value=1)

                where_clauses = []

                for i in range(num_filters):
                    st.markdown(f"**Condition {i+1}**")
                    col_f1, col_f2, col_f3, col_f4 = st.columns([2, 1, 2, 1])

                    with col_f1:
                        filter_column = st.selectbox(f"Column", df_cols_filter['COLUMN_NAME'].tolist(), key=f"filter_col_{i}")

                    with col_f2:
                        operator = st.selectbox(f"Operator", ["=", "!=", ">", "<", ">=", "<=", "LIKE", "IN"], key=f"filter_op_{i}")

                    with col_f3:
                        filter_value = st.text_input(f"Value", key=f"filter_val_{i}", help="For IN operator, use comma-separated values. For dates, use format: YYYY-MM-DD")

                    with col_f4:
                        if i < num_filters - 1:
                            logic = st.selectbox("Logic", ["AND", "OR"], key=f"filter_logic_{i}")
                        else:
                            logic = ""

                    if filter_value:
                        # Get column data type
                        col_type = df_cols_filter[df_cols_filter['COLUMN_NAME'] == filter_column]['DATA_TYPE'].values[0]
                        is_date_type = col_type in ['date', 'datetime', 'datetime2', 'smalldatetime', 'datetimeoffset']
                        is_numeric_type = col_type in ['int', 'bigint', 'smallint', 'tinyint', 'decimal', 'numeric', 'float', 'real', 'money', 'smallmoney']

                        if operator == "LIKE":
                            where_clauses.append(f"[{filter_column}] LIKE '%{filter_value}%'")
                        elif operator == "IN":
                            if is_numeric_type:
                                values = ", ".join([v.strip() for v in filter_value.split(',')])
                            elif is_date_type:
                                values = ", ".join([f"CAST('{v.strip()}' AS DATE)" for v in filter_value.split(',')])
                            else:
                                values = ", ".join([f"'{v.strip()}'" for v in filter_value.split(',')])
                            where_clauses.append(f"[{filter_column}] IN ({values})")
                        elif is_date_type:
                            # For date types, cast the value to DATE
                            where_clauses.append(f"CAST([{filter_column}] AS DATE) {operator} CAST('{filter_value}' AS DATE)")
                        elif is_numeric_type:
                            # For numeric types, don't use quotes
                            where_clauses.append(f"[{filter_column}] {operator} {filter_value}")
                        elif operator in ["=", "!="]:
                            # For text types with = or !=
                            where_clauses.append(f"[{filter_column}] {operator} '{filter_value}'")
                        else:
                            # Other cases (shouldn't happen for text)
                            where_clauses.append(f"[{filter_column}] {operator} '{filter_value}'")

                        if logic and i < num_filters - 1:
                            where_clauses.append(logic)

                limit_filter = st.number_input("Limit rows", min_value=10, max_value=10000, value=100, key="limit_filter")

                if st.button("🔍 Apply Filters", type="primary"):
                    if where_clauses:
                        where_clause = " ".join(where_clauses)
                        filter_query = f"""
                        SELECT TOP {limit_filter} *
                        FROM [{schema_f}].[{name_f}]
                        WHERE {where_clause}
                        """
                    else:
                        filter_query = f"SELECT TOP {limit_filter} * FROM [{schema_f}].[{name_f}]"

                    st.code(filter_query, language="sql")

                    with st.spinner("Executing filtered query..."):
                        df_filter_result, filter_error = execute_custom_query(conn_filter, filter_query)

                    if filter_error:
                        st.error(f"❌ Filter Error: {filter_error}")
                    else:
                        st.success(f"✅ Query executed successfully! {len(df_filter_result)} rows returned.")
                        st.dataframe(df_filter_result, use_container_width=True, height=400)

                        # Export options
                        st.markdown("#### 📥 Export Results")
                        col_exp1, col_exp2 = st.columns(2)
                        with col_exp1:
                            excel_data = export_to_excel(df_filter_result)
                            st.download_button(
                                label="📊 Download as Excel",
                                data=excel_data,
                                file_name=f"filtered_result_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                key="download_excel_filter"
                            )
                        with col_exp2:
                            csv_data = export_to_csv(df_filter_result)
                            st.download_button(
                                label="📄 Download as CSV",
                                data=csv_data,
                                file_name=f"filtered_result_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                                mime="text/csv",
                                key="download_csv_filter"
                            )

            conn_filter.close()

# ========== TAB 5: INCOME STATEMENT ISA22 ==========
with main_tab5:
    st.markdown("### 📈 Income Statement - ISA22 Pivot View")
    st.info("View ISA22 field from 4 Income Statement tables (Bank, Company, Insurance, Securities) in pivot format. Values are displayed in billions (tỷ VNĐ).")
    
    if st.button("🔄 Load ISA22 Data", type="primary", key="load_isa22"):
        with st.spinner("Connecting to DiamondDWH and loading data..."):
            conn_isa, error_isa = get_db_connection(os.getenv("DB_NAME_PRICE"))
        
        if error_isa:
            st.error(f"Connection error: {error_isa}")
        else:
            with st.spinner("Querying Income Statement tables and creating pivot..."):
                df_pivot, pivot_error = get_income_statement_isa22_pivot(conn_isa)
            
            conn_isa.close()
            
            if pivot_error:
                st.error(f"Error: {pivot_error}")
            else:
                # Store in session state
                st.session_state['isa22_pivot'] = df_pivot
                st.success(f"✅ Data loaded successfully! {len(df_pivot)} tickers found.")
    
    # Display data if available in session state
    if 'isa22_pivot' in st.session_state and st.session_state['isa22_pivot'] is not None:
        df_display = st.session_state['isa22_pivot'].copy()
        
        # Filters
        st.markdown("#### 🔍 Filters")
        col_f1, col_f2, col_f3 = st.columns([1, 1, 2])
        
        with col_f1:
            source_options = ['All'] + df_display['SOURCE'].unique().tolist()
            selected_source = st.selectbox("Filter by Source", source_options, key="isa22_source_filter")
        
        with col_f2:
            ticker_search = st.text_input("Search Ticker", key="isa22_ticker_search", placeholder="e.g. VCB, ACB")
        
        # Get date columns
        date_columns = [col for col in df_display.columns if col not in ['TICKER', 'SOURCE']]
        
        with col_f3:
            if date_columns:
                st.markdown("**Select Date Range**")
                col_d1, col_d2 = st.columns(2)
                with col_d1:
                    start_date_idx = st.selectbox("From Date", date_columns, index=0, key="isa22_start_date")
                with col_d2:
                    end_date_idx = st.selectbox("To Date", date_columns, index=len(date_columns)-1, key="isa22_end_date")
        
        # Apply filters
        if selected_source != 'All':
            df_display = df_display[df_display['SOURCE'] == selected_source]
        
        if ticker_search:
            df_display = df_display[df_display['TICKER'].str.contains(ticker_search.upper(), na=False)]
        
        # Filter date columns
        if date_columns and start_date_idx and end_date_idx:
            start_idx = date_columns.index(start_date_idx)
            end_idx = date_columns.index(end_date_idx)
            if start_idx <= end_idx:
                selected_date_cols = date_columns[start_idx:end_idx+1]
                df_display = df_display[['TICKER', 'SOURCE'] + selected_date_cols]
        
        # Display metrics
        st.markdown("---")
        col_m1, col_m2, col_m3, col_m4 = st.columns(4)
        with col_m1:
            st.metric("Total Tickers", len(df_display))
        with col_m2:
            st.metric("Bank", len(df_display[df_display['SOURCE'] == 'Bank']))
        with col_m3:
            st.metric("Company", len(df_display[df_display['SOURCE'] == 'Company']))
        with col_m4:
            ins_count = len(df_display[df_display['SOURCE'] == 'Insurance'])
            sec_count = len(df_display[df_display['SOURCE'] == 'Securities'])
            st.metric("Insurance/Securities", f"{ins_count}/{sec_count}")
        
        # Display dataframe
        st.markdown("#### 📊 ISA22 Pivot Table (values in billions VNĐ)")
        
        # Get numeric columns for formatting
        numeric_cols = [col for col in df_display.columns if col not in ['TICKER', 'SOURCE']]
        
        # Configure AgGrid
        gb = GridOptionsBuilder.from_dataframe(df_display)
        
        # Configure default column settings
        gb.configure_default_column(
            sortable=True,
            filterable=True,
            resizable=True
        )
        
        # Configure TICKER and SOURCE columns
        gb.configure_column('TICKER', pinned='left', width=80)
        gb.configure_column('SOURCE', pinned='left', width=100)
        
        # Configure numeric columns with formatting
        for col in numeric_cols:
            gb.configure_column(
                col,
                type=['numericColumn'],
                valueFormatter=JsCode("""function(params) {
                    if (params.value == null || params.value === '') return '';
                    return params.value.toLocaleString('en-US', {maximumFractionDigits: 0});
                }""")
            )
        
        # Build grid options (no pagination, scrolling only)
        gb.configure_grid_options(domLayout='normal')
        grid_options = gb.build()
        
        # Display AgGrid
        AgGrid(
            df_display,
            gridOptions=grid_options,
            height=500,
            allow_unsafe_jscode=True,
            theme='streamlit',
            update_mode=GridUpdateMode.NO_UPDATE
        )
        
        # Export options
        st.markdown("#### 📥 Export Data")
        col_exp1, col_exp2 = st.columns(2)
        with col_exp1:
            excel_data = export_to_excel(df_display)
            st.download_button(
                label="📊 Download as Excel",
                data=excel_data,
                file_name=f"ISA22_pivot_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="download_excel_isa22"
            )
        with col_exp2:
            csv_data = export_to_csv(df_display)
            st.download_button(
                label="📄 Download as CSV",
                data=csv_data,
                file_name=f"ISA22_pivot_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv",
                key="download_csv_isa22"
            )

# Sidebar info
with st.sidebar:
    st.header("ℹ️ About")
    st.markdown("""
    **Database Explorer Pro** allows you to:

    📋 **Table Explorer**
    - View all tables and columns
    - Preview data with filters
    - View table statistics

    💻 **SQL Query Editor**
    - Write custom SQL queries
    - JOIN multiple tables
    - Save and load query templates

    🔗 **Table Join Wizard**
    - Visual interface to join tables
    - Select specific columns
    - Choose join types

    🔍 **Advanced Filters**
    - Multi-column filtering
    - Complex conditions (AND/OR)
    - Multiple operators

    📈 **Income Statement ISA22**
    - View ISA22 from 4 tables
    - Pivot format (ticker rows, date columns)
    - Values in billions (tỷ VNĐ)
    - Filter by Source/Ticker/Date

    📥 **Export**
    - Download results as Excel/CSV
    - Export from any tab
    """)

    st.markdown("---")
    st.markdown("**Environment Status**")
    st.text(f"DB Server: {'✅' if os.getenv('DB_SERVER') else '❌'}")
    st.text(f"Username: {'✅' if os.getenv('DB_USERNAME') else '❌'}")
    st.text(f"Password: {'✅' if os.getenv('DB_PASSWORD') else '❌'}")
    st.text(f"DB_NAME_REC: {'✅' if os.getenv('DB_NAME_REC') else '❌'}")
    st.text(f"DB_NAME_PRICE: {'✅' if os.getenv('DB_NAME_PRICE') else '❌'}")
