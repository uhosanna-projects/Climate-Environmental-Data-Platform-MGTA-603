import pandas as pd
from sqlalchemy import create_engine

from config_sql import get_engine

engine = get_engine()

# 2) SQL in a Python triple-quoted string
query = """
SELECT
    d.year,
    d.month,
    d.month_name,
    SUM(f.value) AS total_rainfall
FROM fact_measurement f
JOIN dim_sensor  se ON f.sensor_id  = se.sensor_id
JOIN dim_date    d  ON f.date_key   = d.date_key
WHERE se.sensor_name = 'Total Rain'
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year, d.month;


"""

# 3) Run the query and load into a DataFrame
df = pd.read_sql_query(query, engine)

# 4) Look at the result
print(df)


heatmap_df = df.pivot(index="month_name", columns="year", values="total_rainfall")

# Optional: sort months correctly
order = ["January","February","March","April","May","June",
         "July","August","September","October","November","December"]

heatmap_df = heatmap_df.reindex(order)

heatmap_df


# visualization
import matplotlib.pyplot as plt
import numpy as np

plt.figure(figsize=(12, 8))

plt.imshow(heatmap_df, aspect='auto')
plt.colorbar(label='Total Rainfall (mm)')

plt.xticks(
    ticks=np.arange(len(heatmap_df.columns)),
    labels=heatmap_df.columns,
    rotation=45
)
plt.yticks(
    ticks=np.arange(len(heatmap_df.index)),
    labels=heatmap_df.index
)

plt.title("Rainfall Heatmap (Months × Years)")
plt.xlabel("Year")
plt.ylabel("Month")

plt.tight_layout()
plt.show()

