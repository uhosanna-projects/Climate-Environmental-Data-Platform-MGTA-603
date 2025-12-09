import streamlit as st
import pandas as pd
from config_sql import get_engine

# --- Streamlit Page Setup ---
st.set_page_config(page_title="Climate Dashboard", layout="wide")

st.title("🌧️ Monthly Rainfall Analysis")
st.write("This dashboard shows monthly rainfall totals computed from the SQL database.")

# --- Connect to SQL ---
engine = get_engine()

# --- SQL Query ---
query = """
SELECT
    d.month,
    d.month_name,
    SUM(f.value) AS total_rainfall_all_years,
    COUNT(*)     AS n_records,
    AVG(f.value) AS avg_rain_per_record 
FROM fact_measurement f
JOIN dim_sensor  se ON f.sensor_id  = se.sensor_id
JOIN dim_date    d  ON f.date_key   = d.date_key
WHERE se.sensor_name = 'Total Precipitation'
GROUP BY d.month, d.month_name
ORDER BY d.month;
"""

# --- Load DataFrame ---
df = pd.read_sql_query(query, engine)

# --- Display DataFrame in Streamlit ---
st.subheader("📊 Rainfall Table (Across All Years)")
st.dataframe(df)

# --- Visualization Example ---
import matplotlib.pyplot as plt

fig, ax = plt.subplots(figsize=(10, 5))
ax.bar(df["month_name"], df["total_rainfall_all_years"])
ax.set_title("Total Rainfall per Month (All Years)")
ax.set_xlabel("Month")
ax.set_ylabel("Total Rainfall")

st.pyplot(fig)


