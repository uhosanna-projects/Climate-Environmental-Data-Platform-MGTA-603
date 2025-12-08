import pandas as pd
from sqlalchemy import create_engine

from config_sql import get_engine

engine = get_engine()

# 2) SQL in a Python triple-quoted string
query = """
WITH temp_by_day AS (
    SELECT 
        s.province_code AS region,
        d.full_date,
        AVG(f.value) AS daily_max_temp   -- average across stations in same region
    FROM fact_measurement f
    JOIN dim_station s ON f.station_id = s.station_id
    JOIN dim_sensor  se ON f.sensor_id  = se.sensor_id
    JOIN dim_date    d ON f.date_key    = d.date_key
    WHERE se.sensor_name = 'Max Temperature'
    GROUP BY s.province_code, d.full_date
)
SELECT region, full_date, daily_max_temp
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY region ORDER BY daily_max_temp DESC) AS rn
    FROM temp_by_day
) ranked
WHERE rn = 1;
"""

# 3) Run the query and load into a DataFrame
hottest_day = pd.read_sql_query(query, engine)

# 4) Look at the result
print(hottest_day.head())