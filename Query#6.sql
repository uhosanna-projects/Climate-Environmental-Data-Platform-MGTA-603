-- Stored Procedure Requirement
-- Procedure: get monthly averages for a chosen station + sensor

DELIMITER $$

CREATE PROCEDURE GetMonthlyAverages (
    IN p_station_id VARCHAR(10),
    IN p_sensor_id  INT
)
BEGIN
    SELECT 
        d.year,
        d.month,
        AVG(f.value) AS avg_value
    FROM fact_measurement f
    JOIN dim_date d ON f.date_key = d.date_key
    WHERE f.station_id = p_station_id
      AND f.sensor_id  = p_sensor_id
    GROUP BY d.year, d.month
    ORDER BY d.year, d.month;
END $$

DELIMITER ;





CALL GetMonthlyAverages('1020590', 1);
