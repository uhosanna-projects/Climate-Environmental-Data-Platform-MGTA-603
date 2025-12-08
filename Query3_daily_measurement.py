import pandas as pd
from sqlalchemy import create_engine

from db_config import get_engine

engine = get_engine()

# 2) Put your SQL in a Python triple-quoted string
query = """
SELECT 
    d.full_date,
    st.station_name,
    se.sensor_name,
    se.unit,
    f.value
FROM fact_measurement f
JOIN dim_date d     ON f.date_key   = d.date_key
JOIN dim_station st ON f.station_id = st.station_id
JOIN dim_sensor se  ON f.sensor_id  = se.sensor_id
WHERE st.station_id = '1020590'          -- pick any station you want
ORDER BY d.full_date, se.sensor_name;
"""

# 3) Run the query and load into a DataFrame
daily_measuremnet = pd.read_sql_query(query, engine)

# 4) Look at the result
print(daily_measuremnet.head())

