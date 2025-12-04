CREATE DATABASE hive;
USE hive;

-- First creating the location table
CREATE EXTERNAL TABLE location (
    location_id INT,
    latitude DOUBLE,
    longitude DOUBLE,
    elevation INT,
    utc_offset_seconds INT,
    timezone STRING,
    timezone_abbreviation INT,
    city_name STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/data/location/'
TBLPROPERTIES ("skip.header.line.count"="1");


-- Now creating the weather table
CREATE EXTERNAL TABLE weather (
    location_id INT,
    `date` STRING,
    weather_code INT,
    temperature_2m_max DOUBLE,
    temperature_2m_min DOUBLE,
    temperature_2m_mean DOUBLE,
    apparent_temperature_max DOUBLE,
    apparent_temperature_min DOUBLE,
    apparent_temperature_mean DOUBLE,
    daylight_duration DOUBLE,
    sunshine_duration DOUBLE,
    precipitation_sum DOUBLE,
    precipitation_hours INT,
    wind_speed_10m_max DOUBLE,
    wind_gusts_10m_max DOUBLE,
    wind_direction_10m_dominant INT,
    shortwave_radiation_sum DOUBLE,
    et0_fao_evapotranspiration DOUBLE,
    sunrise STRING,
    sunset STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/data/weather/'
TBLPROPERTIES ("skip.header.line.count"="1");

-- Query to find the top 10 cities with the highest temperature
SELECT l.city_name, MAX(w.temperature_2m_max) AS max_temp
FROM weather w
JOIN location l ON w.location_id = l.location_id
GROUP BY l.city_name
ORDER BY max_temp DESC
LIMIT 10;

-- First I created a view to extract month and year from the date to make it easier to work with
CREATE OR REPLACE VIEW weather_with_month AS
SELECT
    w.location_id,
    l.city_name,
    w.et0_fao_evapotranspiration AS evaporation,
    CAST(substr(w.`date`, 6, 2) AS INT) AS month,
    CAST(substr(w.`date`, 1, 4) AS INT) AS year
FROM weather w
JOIN location l ON w.location_id = l.location_id;

-- Now I can calculate the average evapotranspiration for Yala and Maha seasons
SELECT
    city_name,
    year,
    if(month IN (9, 10, 11, 12, 1, 2, 3), 'Maha', 'Yala') AS season,
    ROUND(AVG(evaporation),2) AS avg_evapotranspiration
FROM weather_with_month
GROUP BY city_name,year,if(month IN (9, 10, 11, 12, 1, 2, 3), 'Maha', 'Yala')
ORDER BY city_name,year,season;



