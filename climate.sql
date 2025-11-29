-- 1) DROP OLD TABLES (fact first because of foreign keys)
DROP TABLE IF EXISTS fact_measurement;
DROP TABLE IF EXISTS dim_weather_type;
DROP TABLE IF EXISTS dim_sensor;
DROP TABLE IF EXISTS dim_station;
DROP TABLE IF EXISTS dim_date;

-- 2) CREATE DIMENSION TABLES

-- dim_date.csv
-- Columns: 20000101, 2000-01-01, 2000, 1, January, Winter, True
CREATE TABLE dim_date (
    date_key     INT         NOT NULL,      -- e.g. 20000101
    full_date    DATE        NOT NULL,      -- e.g. '2000-01-01'
    year         INT         NOT NULL,      -- e.g. 2000
    month        TINYINT     NOT NULL,      -- e.g. 1..12
    month_name   VARCHAR(20) NOT NULL,      -- e.g. 'January'
    season       VARCHAR(20) NOT NULL,      -- e.g. 'Winter'
    PRIMARY KEY (date_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- dim_sensor.csv
-- Columns: id, name, unit  (1, "Mean Temperature", "C"), etc.
CREATE TABLE dim_sensor (
    sensor_id    INT          NOT NULL,
    sensor_name  VARCHAR(100) NOT NULL,
    unit         VARCHAR(20)  NOT NULL,
    PRIMARY KEY (sensor_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- dim_station.csv
-- Columns: station_id, station_name, province_code, lat, lon
-- Note: station_id can contain letters (e.g. "116C8P0"), so use VARCHAR
CREATE TABLE dim_station (
    station_id    VARCHAR(10)  NOT NULL,      -- e.g. "1020590", "116C8P0"
    station_name  VARCHAR(100) NOT NULL,
    province_code CHAR(2)      NOT NULL,      -- e.g. "BC"
    latitude      DECIMAL(9,6)     NULL,
    longitude     DECIMAL(9,6)     NULL,
    PRIMARY KEY (station_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- dim_weather_type.csv
-- Columns: id, name, description
-- CREATE TABLE dim_weather_type (
--     weather_type_id    INT           NOT NULL,
--     weather_type_name  VARCHAR(50)   NOT NULL,
--     weather_type_desc  VARCHAR(255)       NULL,
--     PRIMARY KEY (weather_type_id)
-- ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 3) CREATE FACT TABLE

-- fact_measurement_full.csv
-- Columns: station_id, date_key, sensor_id, weather_type_id, value
CREATE TABLE fact_measurement (
    station_id       VARCHAR(10)   NOT NULL,
    date_key         INT           NOT NULL,
    sensor_id        INT           NOT NULL,
    -- weather_type_id  INT                NULL,
    value            DECIMAL(10,2)      NULL,

    -- Composite primary key: one record per station/date/sensor
    PRIMARY KEY (station_id, date_key, sensor_id),

    -- Foreign keys
    CONSTRAINT fk_fact_station
        FOREIGN KEY (station_id)      REFERENCES dim_station(station_id),
    CONSTRAINT fk_fact_date
        FOREIGN KEY (date_key)        REFERENCES dim_date(date_key),
    CONSTRAINT fk_fact_sensor
        FOREIGN KEY (sensor_id)       REFERENCES dim_sensor(sensor_id)
    -- CONSTRAINT fk_fact_weather
        -- FOREIGN KEY (weather_type_id) REFERENCES dim_weather_type(weather_type_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;





