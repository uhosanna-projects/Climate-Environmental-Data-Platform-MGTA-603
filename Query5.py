import pandas as pd
from sqlalchemy import create_engine, text

from config_sql import get_engine

engine = get_engine()

# 2) Put your SQL in a Python triple-quoted string
query_view = """
CREATE OR REPLACE VIEW v_daily_station_measurements AS
SELECT 
    f.station_id,
    st.station_name,
    d.full_date,
    d.year,
    d.month,
    d.season,
    f.sensor_id,
    se.sensor_name,
    se.unit,
    f.value
FROM fact_measurement f
JOIN dim_station st ON f.station_id = st.station_id
JOIN dim_date d     ON f.date_key   = d.date_key
JOIN dim_sensor se  ON f.sensor_id  = se.sensor_id;
"""

with engine.begin() as connection:
    connection.execute(text(query_view))

query = """
SELECT *
FROM v_daily_station_measurements
WHERE sensor_name = 'Mean Temperature'
ORDER BY full_date;
"""

# 3) Run the query and load into a DataFrame
data = pd.read_sql_query(query, engine)

# 4) Look at the result
print(data)