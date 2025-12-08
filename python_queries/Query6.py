import pandas as pd
from sqlalchemy import create_engine

from config_sql import get_engine

engine = get_engine()

# 2) Put your SQL in a Python triple-quoted string
query = """
SELECT 
    st.station_id,
    st.station_name,
    AVG(f.value) AS avg_temp_2000
FROM fact_measurement f
JOIN dim_station st ON f.station_id = st.station_id
JOIN dim_date d     ON f.date_key   = d.date_key
WHERE f.sensor_id = 1      -- 1 = Mean Temperature
  AND d.year = 2000        -- change year if needed
GROUP BY st.station_id, st.station_name
ORDER BY avg_temp_2000 DESC
LIMIT 1;
"""

# 3) Run the query and load into a DataFrame
high_temp = pd.read_sql_query(query, engine)

# 4) Look at the result
print(high_temp)

