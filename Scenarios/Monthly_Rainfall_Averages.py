import pandas as pd
from sqlalchemy import create_engine

from config_sql import get_engine

engine = get_engine()

# 2) SQL in a Python triple-quoted string
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

# 3) Run the query and load into a DataFrame
rainfall_average = pd.read_sql_query(query, engine)

# 4) Look at the result
print(rainfall_average)