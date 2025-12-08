-- JOIN Query #3 (no GROUP BY)
-- All daily measurements for one station, with sensor names

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
