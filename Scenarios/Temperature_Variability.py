import pandas as pd
from sqlalchemy import create_engine

from config_sql import get_engine

engine = get_engine()

# 2) SQL in a Python triple-quoted string
query = """
SELECT
    d.year,
    STDDEV(f.value) AS annual_temp_variability
FROM fact_measurement f
JOIN dim_sensor  se ON f.sensor_id  = se.sensor_id
JOIN dim_date    d  ON f.date_key   = d.date_key
WHERE se.sensor_name = 'Mean Temperature'
GROUP BY d.year
ORDER BY d.year;


"""

# 3) Run the query and load into a DataFrame
df = pd.read_sql_query(query, engine)

# 4) Look at the result
print(df)


import matplotlib.pyplot as plt

plt.figure(figsize=(10, 5))

plt.plot(df['year'], df['annual_temp_variability'], marker='o')
plt.title("Annual Temperature Variability (Standard Deviation)")
plt.xlabel("Year")
plt.ylabel("Temperature Variability (°C)")
plt.grid(True)

plt.tight_layout()
plt.show()
