-- JOIN Query #2 + GROUP BY
-- Average value per province and season (e.g. seasonal temperature by province)

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
