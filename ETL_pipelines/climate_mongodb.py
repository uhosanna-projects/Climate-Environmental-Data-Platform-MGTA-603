import pandas as pd
from pymongo import MongoClient

from config_mongo import MONGO_URI, MONGO_DB, MONGO_COLLECTION
from config_sql import get_engine

engine = get_engine()

query = """
SELECT
    f.value,
    se.sensor_id,
    se.sensor_name,
    d.year,
    d.month,
    d.month_name,
    s.station_id,
    s.station_name,
    s.province_code
FROM fact_measurement f
JOIN dim_sensor   se ON f.sensor_id = se.sensor_id
JOIN dim_date     d  ON f.date_key = d.date_key
JOIN dim_station  s  ON f.station_id = s.station_id
ORDER BY d.year, d.month;
"""

df = pd.read_sql_query(query, engine)
print(df.shape)


# compute monthly average

monthly_avg = (
    df.groupby(["sensor_id", "sensor_name", "year", "month", "month_name"])
      .agg(
          avg_value=("value", "mean"),
          min_value=("value", "min"),
          max_value=("value", "max"),
          count=("value", "count")
      )
      .reset_index()
)

print(monthly_avg.head())

mongo_docs = monthly_avg.to_dict(orient="records")

client = MongoClient(MONGO_URI)
db = client[MONGO_DB]
collection = db[MONGO_COLLECTION]

# print(collection.count())
# Insert data
collection.insert_many(mongo_docs)

# print(collection.count())

print("Inserted documents:", len(mongo_docs))