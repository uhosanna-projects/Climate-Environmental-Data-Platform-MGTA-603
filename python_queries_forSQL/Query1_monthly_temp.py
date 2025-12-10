import pandas as pd
from sqlalchemy import text
from python_queries_forNosql.redis_query_log import cache_recent_query
from config_sql import get_engine

engine = get_engine()

# 2) Put your SQL in a Python triple-quoted string
query = """
SELECT 
    s.station_id,
    s.station_name,
    d.year,
    d.month,
    AVG(f.value) AS avg_temp
FROM fact_measurement f
JOIN dim_station s ON f.station_id = s.station_id
JOIN dim_date d    ON f.date_key   = d.date_key
JOIN dim_sensor se ON f.sensor_id  = se.sensor_id
WHERE se.sensor_name = 'Mean Temperature'
GROUP BY 
    s.station_id, s.station_name,
    d.year, d.month
ORDER BY 
    s.station_id, d.year, d.month;
"""

# 3) Run the query and load into a DataFrame
df_monthly_temp = pd.read_sql_query(query, engine)

# 4) Look at the result
print(df_monthly_temp.head())

cache_recent_query(query)         # <--- Log into Redis

df = pd.read_sql(text(query), engine)
print(df.head())
