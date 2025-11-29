-- Subquery / CTE Requirement
-- Stations whose average temperature is higher than the global average
-- Version A – Using a subquery

SELECT 
    st.station_id,
    st.station_name,
    AVG(f.value) AS station_avg_temp
FROM fact_measurement f
JOIN dim_station st ON f.station_id = st.station_id
JOIN dim_sensor se  ON f.sensor_id  = se.sensor_id
WHERE se.sensor_name = 'Mean Temperature'
GROUP BY st.station_id, st.station_name
HAVING station_avg_temp >
       (
         SELECT AVG(f2.value)
         FROM fact_measurement f2
         JOIN dim_sensor se2 ON f2.sensor_id = se2.sensor_id
         WHERE se2.sensor_name = 'Mean Temperature'
       )
ORDER BY station_avg_temp DESC;



-- Version B – Same idea with a CTE (if you want to show off MySQL 8)

WITH global_avg AS (
    SELECT AVG(f2.value) AS avg_temp
    FROM fact_measurement f2
    JOIN dim_sensor se2 ON f2.sensor_id = se2.sensor_id
    WHERE se2.sensor_name = 'Mean Temperature'
)
SELECT 
    st.station_id,
    st.station_name,
    AVG(f.value) AS station_avg_temp
FROM fact_measurement f
JOIN dim_station st ON f.station_id = st.station_id
JOIN dim_sensor se  ON f.sensor_id  = se.sensor_id,
global_avg g
WHERE se.sensor_name = 'Mean Temperature'
GROUP BY st.station_id, st.station_name, g.avg_temp
HAVING station_avg_temp > g.avg_temp
ORDER BY station_avg_temp DESC;


