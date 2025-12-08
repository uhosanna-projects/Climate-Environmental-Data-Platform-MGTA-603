import pandas as pd
from sqlalchemy import create_engine
from config_sql import get_engine
import matplotlib.pyplot as plt

engine = get_engine()

# --- 1. Query only 2024 data ---
query = """
SELECT
    d.year,
    d.month,
    d.month_name,
    d.full_date,
    f.value AS temperature
FROM fact_measurement f
JOIN dim_sensor  se ON f.sensor_id  = se.sensor_id
JOIN dim_date    d  ON f.date_key   = d.date_key
WHERE se.sensor_name = 'Mean Temperature'
  AND d.year = 2024
ORDER BY d.month, d.full_date;
"""

df = pd.read_sql_query(query, engine)

# --- 2. Group temperatures by month ---
groups = []
labels = []

# Ensure correct month ordering
month_order = ["January","February","March","April","May","June",
               "July","August","September","October","November","December"]

df["month_name"] = pd.Categorical(df["month_name"], categories=month_order, ordered=True)
df = df.sort_values("month_name")

for month, group in df.groupby("month_name"):
    groups.append(group["temperature"].values)
    labels.append(month)

# --- 3. Plot boxplot per month ---
plt.figure(figsize=(14, 6))

plt.boxplot(groups, labels=labels, showfliers=False)
plt.xlabel("Month (2024)")
plt.ylabel("Mean Temperature (°C)")
plt.title("Temperature Variability per Month in 2024 (Boxplot)")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()
