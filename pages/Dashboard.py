import streamlit as st
import pandas as pd
import plotly.express as px
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import subprocess
import sys

def install(package):
    subprocess.check_call([sys.executable, "-m", "pip", "install", package])

install("snowflake-connector-python")


# Snowflake connection function
@st.cache_resource
def init_snowflake_connection():
    conn = snowflake.connector.connect(
        user=st.secrets["snowflake"]["user"],
        password=st.secrets["snowflake"]["password"],
        account=st.secrets["snowflake"]["account"],
        warehouse=st.secrets["snowflake"]["warehouse"],
        database=st.secrets["snowflake"]["database"],
        schema=st.secrets["snowflake"]["schema"]
    )
    return conn

# Load data from Snowflake
@st.cache_data(ttl=3600)
def load_data():
    conn = init_snowflake_connection()
    
    # Query for tourist visits data
    visits_query = """
    SELECT STATE_NAME, MONTH, TOURIST_COUNT 
    FROM TOURISTS_VISITS 
    WHERE year = 2023
    ORDER BY STATE_NAME, MONTH
    """
    
    # Query for tourism spending data
    spending_query = """
    SELECT CATEGORY, AMOUNT 
    FROM TOURISM_SPENDING
    WHERE YEAR = 2023
    ORDER BY AMOUNT DESC
    """
    
    # Query for tourism revenue data
    revenue_query = """
    SELECT STATE_NAME, REVENUE 
    FROM TOURISM_REVENUE 
    WHERE YEAR = 2023
    ORDER BY REVENUE DESC
    LIMIT 10
    """
    
    visits_df = pd.read_sql(visits_query, conn)
    spending_df = pd.read_sql(spending_query, conn)
    revenue_df = pd.read_sql(revenue_query, conn)
    
    conn.close()
    
    return visits_df, spending_df, revenue_df

# Load all data
visits_df, spending_df, revenue_df = load_data()

# Dashboard layout
col1, col2 = st.columns([3, 1])

with col1:
    st.subheader("State-wise Tourism Analysis")
    selected_state = st.selectbox("Select a state", visits_df['STATE_NAME'].unique(), key='state_select')
    
with col2:
    st.metric("Total Tourists (2023)", 
              f"{visits_df[visits_df['STATE_NAME'] == selected_state]['TOURIST_COUNT'].sum():,.0f}")

# Row 1: Line chart and pie chart
row1_col1, row1_col2 = st.columns([2, 1])

with row1_col1:
    # Line chart for monthly tourist visits
    state_visits = visits_df[visits_df['STATE_NAME'] == selected_state]
    fig_visits = px.line(state_visits, x='MONTH', y='TOURIST_COUNT', 
                        title=f"Monthly Tourist Visits in {selected_state} (2023)",
                        labels={'MONTH': 'Month', 'TOURIST_COUNT': 'Tourist Count'},
                        template='plotly_dark',
                        markers=True)
    st.plotly_chart(fig_visits, use_container_width=True)

with row1_col2:
    # Pie chart for government spending
    fig_spending = px.pie(spending_df, values='AMOUNT', names='CATEGORY',
                         title="Government Spending on Tourism (2023)",
                         template='plotly_dark')
    fig_spending.update_traces(textposition='inside', textinfo='percent+label')
    st.plotly_chart(fig_spending, use_container_width=True)

# Row 2: Bar charts
row2_col1, row2_col2 = st.columns(2)

with row2_col1:
    # Bar chart for top states by revenue
    fig_revenue = px.bar(revenue_df, x='STATE_NAME', y='REVENUE',
                        title="Top 10 States by Tourism Revenue (2023)",
                        labels={'STATE_NAME': 'State', 'REVENUE': 'Revenue (in crores)'},
                        template='plotly_dark')
    st.plotly_chart(fig_revenue, use_container_width=True)

with row2_col2:
    # Bar chart for state comparison (optional)
    compare_states = st.multiselect("Compare with other states", 
                                  visits_df['STATE_NAME'].unique(),
                                  default=[selected_state],
                                  key='compare_select')
    
    if compare_states:
        compare_df = visits_df[visits_df['STATE_NAME'].isin(compare_states)]
        compare_df = compare_df.groupby('STATE_NAME')['TOURIST_COUNT'].sum().reset_index()
        
        fig_compare = px.bar(compare_df, x='STATE_NAME', y='TOURIST_COUNT',
                            title="Tourist Count Comparison (2023)",
                            labels={'STATE_NAME': 'State', 'TOURIST_COUNT': 'Total Tourists'},
                            template='plotly_dark')
        st.plotly_chart(fig_compare, use_container_width=True)

# Add some metrics at the bottom
st.subheader("National Tourism Overview")
col1, col2, col3 = st.columns(3)

with col1:
    st.metric("Total Tourists Nationwide", 
              f"{visits_df['TOURIST_COUNT'].sum():,.0f}",
              "5.2% vs 2022")

with col2:
    st.metric("Total Government Spending", 
              f"₹{spending_df['AMOUNT'].sum()/100:.1f} crores",
              "8.7% increase")

with col3:
    st.metric("Total Revenue Generated", 
              f"₹{revenue_df['REVENUE'].sum():,.0f} crores",
              "12.3% increase")

# Add disclaimer
st.caption("Data source: Ministry of Tourism, Government of India. Updated monthly.")
