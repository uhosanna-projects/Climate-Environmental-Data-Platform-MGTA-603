import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import text
from config_sql import get_engine
from redis_query_log import cache_recent_query

from config_sql import get_engine

engine = get_engine()

# 2) Put your SQL in a Python triple-quoted string
query = """
-- Subquery / CTE Requirement
-- Stations whose average temperature is higher than the global average
WITH global_avg AS (
    SELECT AVG(f2.value) AS avg_temp
    FROM fact_measurement f2
    JOIN dim_sensor se2 ON f2.sensor_id = se2.sensor_id
    WHERE se2.sensor_name = 'Mean Temperature'
)
SELECT 
    st.station_id,
    st.station_name,
    AVG(f.value) AS station_avg_temp
FROM fact_measurement f
JOIN dim_station st ON f.station_id = st.station_id
JOIN dim_sensor se  ON f.sensor_id  = se.sensor_id,
global_avg g
WHERE se.sensor_name = 'Mean Temperature'
GROUP BY st.station_id, st.station_name, g.avg_temp
HAVING station_avg_temp > g.avg_temp
ORDER BY station_avg_temp DESC;
"""

# 3) Run the query and load into a DataFrame
high_temp = pd.read_sql_query(query, engine)

# 4) Look at the result
print(high_temp)


#Visualize bar chart

import matplotlib.pyplot as plt

x = high_temp['station_name']
y = high_temp['station_avg_temp']

plt.figure(figsize=(10, 5))        # optional: make it wider
plt.bar(x, y)                      # ← bar chart instead of line chart
plt.xlabel("Station")
plt.ylabel("Average Temperature")
plt.title("Stations Above Global Average Temperature")
plt.xticks(rotation=45, ha='right')  # rotate labels if there are many stations

plt.tight_layout()
plt.show()

cache_recent_query(query)         # <--- Log into Redis

df = pd.read_sql(text(query), engine)
print(df.head())
