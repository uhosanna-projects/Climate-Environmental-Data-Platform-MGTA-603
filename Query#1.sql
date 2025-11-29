-- JOIN Query #1 + GROUP BY
-- Monthly average temperature per station (for one sensor, e.g. “Mean Temperature”)

SELECT 
    s.station_id,
    s.station_name,
    d.year,
    d.month,
    AVG(f.value) AS avg_temp
FROM fact_measurement f
JOIN dim_station s ON f.station_id = s.station_id
JOIN dim_date d    ON f.date_key   = d.date_key
JOIN dim_sensor se ON f.sensor_id  = se.sensor_id
WHERE se.sensor_name = 'Mean Temperature'   -- change to your exact name
GROUP BY 
    s.station_id, s.station_name,
    d.year, d.month
ORDER BY 
    s.station_id, d.year, d.month;
