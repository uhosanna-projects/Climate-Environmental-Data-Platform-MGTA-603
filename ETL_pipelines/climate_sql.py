from sqlalchemy import text

from clean import clean, sensors_data, stations_data, date_data, fact_measures
from config_sql import get_engine
from config_sql import load_table
from load import load

df = load('data/*.csv')

df = clean(df)
sensor = sensors_data(df)
station = stations_data(df)
date = date_data(df)
fact = fact_measures(df)



engine = get_engine()
with engine.begin() as conn:
    conn.exec_driver_sql("DROP TABLE IF EXISTS fact_measurement")
    conn.exec_driver_sql("DROP TABLE IF EXISTS dim_date")
    conn.exec_driver_sql("DROP TABLE IF EXISTS dim_sensor")
    conn.exec_driver_sql("DROP TABLE IF EXISTS dim_station")
    # add dim_weather if you have it

# 4) Load dimensions first, then fact
load_table(sensor,  "dim_sensor")
load_table(station, "dim_station")
load_table(date,    "dim_date")
load_table(fact,    "fact_measurement")

with engine.connect() as con:
    con.execute(text("ALTER TABLE dim_sensor ADD PRIMARY KEY (sensor_id)"))
    con.execute(text("ALTER TABLE dim_date ADD PRIMARY KEY (date_key)"))
    con.execute(text("ALTER TABLE dim_station MODIFY station_id VARCHAR(10) NOT NULL"))
    con.execute(text("ALTER TABLE fact_measurement MODIFY station_id VARCHAR(10) NOT NULL"))
    con.execute(text("ALTER TABLE dim_station ADD PRIMARY KEY (station_id)"))
    con.execute(text("ALTER TABLE fact_measurement ADD FOREIGN KEY (sensor_id) REFERENCES dim_sensor(sensor_id)"))
    con.execute(text("ALTER TABLE fact_measurement ADD FOREIGN KEY (date_key) REFERENCES dim_date(date_key)"))
    con.execute(text("ALTER TABLE fact_measurement ADD FOREIGN KEY (station_id) REFERENCES dim_station(station_id)"))
    con.execute(text("ALTER TABLE fact_measurement ADD PRIMARY KEY (sensor_id, date_key, station_id)"))