-- Station with the highest average daily mean temperature in 2000
SELECT 
    st.station_id,
    st.station_name,
    AVG(f.value) AS avg_temp_2000
FROM fact_measurement f
JOIN dim_station st ON f.station_id = st.station_id
JOIN dim_date d     ON f.date_key   = d.date_key
WHERE f.sensor_id = 1      -- 1 = Mean Temperature
  AND d.year = 2000        -- change year if needed
GROUP BY st.station_id, st.station_name
ORDER BY avg_temp_2000 DESC
LIMIT 1;
