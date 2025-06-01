# Placeholder for future Snowflake integration
import snowflake.connector
import pandas as pd

def get_snowflake_data(query):
    conn = snowflake.connector.connect(
        user='Shubh',
        password='Shubhamagarwal0411',
        account='rdfxfak-jo92756',
        warehouse='COMPUTE_WH',
        database='TOURISM_DB',
        schema='PUBLIC'
    )
    cursor = conn.cursor()
    cursor.execute(query)
    df = cursor.fetch_pandas_all()
    cursor.close()
    conn.close()
    return df
