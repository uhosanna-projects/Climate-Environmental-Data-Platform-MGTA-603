-- View Requirement
-- View for “daily station measurements with all labels

CREATE OR REPLACE VIEW v_daily_station_measurements AS
SELECT 
    f.station_id,
    st.station_name,
    d.full_date,
    d.year,
    d.month,
    d.season,
    f.sensor_id,
    se.sensor_name,
    se.unit,
    f.value
FROM fact_measurement f
JOIN dim_station st ON f.station_id = st.station_id
JOIN dim_date d     ON f.date_key   = d.date_key
JOIN dim_sensor se  ON f.sensor_id  = se.sensor_id;





SELECT *
FROM v_daily_station_measurements
WHERE sensor_name = 'Mean Temperature'
ORDER BY full_date;
