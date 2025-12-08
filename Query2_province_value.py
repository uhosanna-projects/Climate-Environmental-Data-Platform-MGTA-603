import pandas as pd
from sqlalchemy import create_engine

from db_config import get_engine

engine = get_engine()

# 2) Put your SQL in a Python triple-quoted string
query = """
SELECT 
    st.province_code,
    d.season,
    AVG(f.value) AS avg_value
FROM fact_measurement f
JOIN dim_station st ON f.station_id = st.station_id
JOIN dim_date d     ON f.date_key   = d.date_key
JOIN dim_sensor se  ON f.sensor_id  = se.sensor_id
WHERE se.sensor_name = 'Mean Temperature'    -- or 'Total Precipitation', etc.
GROUP BY 
    st.province_code,
    d.season
ORDER BY 
    st.province_code,
    d.season;
"""

# 3) Run the query and load into a DataFrame
province_value = pd.read_sql_query(query, engine)

# 4) Look at the result
print(province_value.head())

import matplotlib.pyplot as plt

x = province_value["season"]
y = province_value["avg_value"]

plt.plot(x, y)
plt.show()
