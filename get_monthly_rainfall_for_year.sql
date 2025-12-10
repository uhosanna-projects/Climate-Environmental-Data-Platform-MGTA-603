DELIMITER //

CREATE PROCEDURE get_monthly_rainfall_for_year(IN p_year INT)
BEGIN
    SELECT
        d.month,
        d.month_name,
        SUM(f.value) AS total_rainfall
    FROM fact_measurement f
    JOIN dim_sensor  se ON f.sensor_id = se.sensor_id
    JOIN dim_date    d  ON f.date_key   = d.date_key
    WHERE se.sensor_name = 'Total Rain'
      AND d.year = p_year
    GROUP BY d.month, d.month_name
    ORDER BY d.month;
END //

DELIMITER ;

