SELECT CASE
        WHEN EXTRACT(HOUR FROM pickup_datetime) BETWEEN 6 AND 11 THEN 'Morning'
        WHEN EXTRACT(HOUR FROM pickup_datetime) BETWEEN 12 AND 17 THEN 'Noon'
        WHEN EXTRACT(HOUR FROM pickup_datetime) BETWEEN 18 AND 23 THEN 'Evening'
        ELSE 'Night'END AS daypart,
       
       * FROM `bigquery-public-data.new_york_taxi_trips.tlc_green_trips_2014` 
where 1=1 --AND pickup_datetime > dropoff_datetime
AND pickup_datetime BETWEEN '2014-03-01' AND '2014-03-07'
LIMIT 10;


-- Tablo Create Aşaması
DROP TABLE `teknasyoncase.taxi_trips.TRX_TAXI_TRIPS`;
TRUNCATE TABLE `teknasyoncase.taxi_trips.TRX_TAXI_TRIPS`;
CREATE TABLE `teknasyoncase.taxi_trips.TRX_TAXI_TRIPS`
PARTITION BY DT_TRX_DATE AS
--INSERT INTO `teknasyoncase.taxi_trips.TRX_TAXI_TRIPS`
SELECT DISTINCT CAST(pickup_datetime AS STRING FORMAT 'yyyyMMdd') AS ID_YEARMONTHDAY,
data_file_year AS ID_YEAR,
data_file_month AS ID_YEARMONTH,
EXTRACT(HOUR FROM pickup_datetime) ID_HOUR,
FARM_FINGERPRINT(CONCAT(vendor_id,pickup_datetime,dropoff_datetime,pickup_location_id,dropoff_location_id,total_amount))ID_TRX_NUM,
DATE(pickup_datetime) AS DT_TRX_DATE,
pickup_datetime AS DT_PICKUP_DATETIME,
dropoff_datetime AS DT_DROPOFF_DATETIME,
CASE
        WHEN EXTRACT(HOUR FROM pickup_datetime) BETWEEN 6 AND 11 THEN 'Morning'
        WHEN EXTRACT(HOUR FROM pickup_datetime) BETWEEN 12 AND 17 THEN 'Noon'
        WHEN EXTRACT(HOUR FROM pickup_datetime) BETWEEN 18 AND 23 THEN 'Evening'
        ELSE 'Night'END AS DS_DAY_PART,
pickup_location_id AS ID_PICKUP_LOCATION,
dropoff_location_id AS ID_DROPOFF_LOCATION,
vendor_id AS ID_VENDOR,
store_and_fwd_flag AS FL_STORE_AND_FWD,
rate_code AS ID_RATE,
passenger_count AS MT_PASSENGER_COUNT,
trip_distance AS MT_TRIP_DISTANCE,
payment_type AS ID_PAYMENT_TYPE,
total_amount AS MT_TOTAL_AMOUNT,
fare_amount AS MT_FARE_AMOUNT,
extra AS MT_EXTRA,
mta_tax AS MT_MTA_TAX,
tip_amount AS MT_TIP_AMOUNT,
tolls_amount AS MT_TOLLS_AMOUNT,
ehail_fee AS MT_EHAIL_FEE,
airport_fee AS MT_IRPORT_FEE,
imp_surcharge AS MT_IMP_SURCHARGE,
trip_type AS ID_TRIP_TYPE,
distance_between_service AS DS_DISTANCE_BETWEEN_SERVICE,
time_between_service AS DS_TIME_BETWEEN_SERVICE,
CURRENT_DATETIME() AS DT_INSERT_DTTM,
CURRENT_DATETIME() AS DT_UPDATE_DTTM
FROM `bigquery-public-data.new_york_taxi_trips.tlc_green_trips_2014` 
WHERE 1 = 1 
AND pickup_datetime BETWEEN '2014-03-01' AND '2014-03-08'
AND pickup_datetime < dropoff_datetime
limit 100
;

--SELECT * FROM `teknasyoncase.taxi_trips.TRX_TAXI_TRIPS` WHERE DT_TRX_DATE = "2014-03-01" LIMIT 10;

-- LKP tabloların oluşturulması
CREATE TABLE `teknasyoncase.taxi_trips.LKP_ZONES` AS 
SELECT 
zone_id AS ID_ZONE,
zone_name AS DS_ZONE_NAME,
borough AS DS_BOROUGH,
zone_geom AS DS_ZONE_GEOM
FROM `bigquery-public-data.new_york_taxi_trips.taxi_zone_geom`;

CREATE TABLE `teknasyoncase.taxi_trips.LKP_TAXI_VENDOR` AS 
SELECT 1 AS ID_VENDOR, 'Creative Mobile Technologies' AS DS_VENDOR_NAME
UNION ALL
SELECT 2 AS ID_VENDOR, 'VeriFone Inc.' AS DS_VENDOR_NAME;


CREATE TABLE `teknasyoncase.taxi_trips.LKP_PAYMENT_TYPE` AS 
SELECT 1 AS ID_PAYMENT_TYPE, 'Credit card' AS DS_PAYMENT_TYPE
UNION ALL
SELECT 2 AS ID_PAYMENT_TYPE, 'Cash' AS DS_PAYMENT_TYPE
UNION ALL
SELECT 3 AS ID_PAYMENT_TYPE, 'No charge' AS DS_PAYMENT_TYPE
UNION ALL
SELECT 4 AS ID_PAYMENT_TYPE, 'Dispute' AS DS_PAYMENT_TYPE
UNION ALL
SELECT 5 AS ID_PAYMENT_TYPE, 'Unknown' AS DS_PAYMENT_TYPE
UNION ALL
SELECT 6 AS ID_PAYMENT_TYPE, 'Voided trip' AS DS_PAYMENT_TYPE;

CREATE TABLE `teknasyoncase.taxi_trips.LKP_RATE` AS 
SELECT 1 AS ID_RATE, 'Standard rate' AS DS_RATE_CODE
UNION ALL
SELECT 2 AS ID_RATE, 'JFK' AS DS_RATE_CODE
UNION ALL
SELECT 3 AS ID_RATE, 'Newark' AS DS_RATE_CODE
UNION ALL
SELECT 4 AS ID_RATE, 'Nassau or Westchester' AS DS_RATE_CODE
UNION ALL
SELECT 5 AS ID_RATE, 'Negotiated fare' AS DS_RATE_CODE
UNION ALL
SELECT 6 AS ID_RATE, 'Group ride' AS DS_RATE_CODE;




CREATE TABLE `teknasyoncase.taxi_trips.LKP_TRIP_TYPE` AS 
SELECT 1 AS ID_TRIP_TYPE, 'Street-hail' AS DS_TRIP_TYPE
UNION ALL
SELECT 2 AS ID_TRIP_TYPE, 'Dispatch' AS DS_TRIP_TYPE;



-- Raporlarda kullanılacak özet fact tabloların oluşturulması


DROP TABLE `teknasyoncase.taxi_trips.F_TRX_TAXI_TRIPS_PICKUP_LOCATION_CNT_D`;
-- What are the most popular pickup locations
CREATE TABLE `teknasyoncase.taxi_trips.F_TRX_TAXI_TRIPS_PICKUP_LOCATION_CNT_D`
PARTITION BY DT_TRX_DATE AS
SELECT DT_TRX_DATE,ID_PICKUP_LOCATION,COUNT(ID_PICKUP_LOCATION)MT_CNT_PICKUP_LOCATION, CURRENT_DATETIME('Turkey') AS DT_INSERT_DTTM,CURRENT_DATETIME('Turkey') AS DT_UPDATE_DTTM 
FROM `teknasyoncase.taxi_trips.TRX_TAXI_TRIPS` --WHERE DT_TRX_DATE = "2014-03-02" 
GROUP BY DT_TRX_DATE,ID_PICKUP_LOCATION
ORDER BY 1 DESC,3 DESC
LIMIT 10;


DROP TABLE `teknasyoncase.taxi_trips.F_TRX_TAXI_TRIPS_DROPOFF_LOCATION_CNT_D`;
-- What are the most popular dropoff locations
CREATE TABLE `teknasyoncase.taxi_trips.F_TRX_TAXI_TRIPS_DROPOFF_LOCATION_CNT_D`
PARTITION BY DT_TRX_DATE AS
SELECT DT_TRX_DATE,ID_DROPOFF_LOCATION,COUNT(ID_DROPOFF_LOCATION)MT_CNT_DROPOFF_LOCATION, CURRENT_DATETIME('Turkey') AS DT_INSERT_DTTM,CURRENT_DATETIME('Turkey') AS DT_UPDATE_DTTM  
FROM `teknasyoncase.taxi_trips.TRX_TAXI_TRIPS` --WHERE DT_TRX_DATE = "2014-03-02" 
GROUP BY DT_TRX_DATE,ID_DROPOFF_LOCATION
ORDER BY 1 DESC,3 DESC
LIMIT 10;


DROP TABLE `teknasyoncase.taxi_trips.F_TRX_TAXI_TRIPS_ROUTE_CNT_D`;
-- What are the most popular routes
CREATE TABLE `teknasyoncase.taxi_trips.F_TRX_TAXI_TRIPS_ROUTE_CNT_D`
PARTITION BY DT_TRX_DATE AS
SELECT DT_TRX_DATE,CONCAT(ID_PICKUP_LOCATION,' TO ',ID_DROPOFF_LOCATION) AS DS_ROUTES,COUNT(ID_PICKUP_LOCATION)MT_CNT_ROUTE, CURRENT_DATETIME('Turkey') AS DT_INSERT_DTTM,CURRENT_DATETIME('Turkey') AS DT_UPDATE_DTTM 
FROM `teknasyoncase.taxi_trips.TRX_TAXI_TRIPS` --WHERE DT_TRX_DATE = "2014-03-02" 
GROUP BY DT_TRX_DATE,DS_ROUTES
ORDER BY 1 DESC,3 DESC
LIMIT 10;



-- Çoklama kontrolü ve uniq key oluştulabilmenin incelenmesi
SELECT FARM_FINGERPRINT(CONCAT(vendor_id,pickup_datetime,dropoff_datetime,pickup_location_id,dropoff_location_id,total_amount))ID_TRX_NUM
,COUNT(*)
FROM (SELECT DISTINCT * FROM`bigquery-public-data.new_york_taxi_trips.tlc_green_trips_2014`  )
--WHERE DT_TRX_DATE = "2014-03-02" 
GROUP BY ID_TRX_NUM
HAVING COUNT(ID_TRX_NUM)>1
ORDER BY 2 DESC
LIMIT 100;




/*
SELECT DISTINCT ID_YEARMONTHDAY 
FROM `teknasyoncase.taxi_trips.TRX_TAXI_TRIPS`
order by 1;

DELETE FROM `teknasyoncase.taxi_trips.TRX_TAXI_TRIPS`
WHERE DT_TRX_DATE = '2014-03-08';

SELECT *
FROM `teknasyoncase.taxi_trips.F_TRX_TAXI_TRIPS_PICKUP_LOCATION_CNT_D`
order by 1;

SELECT *
FROM `teknasyoncase.taxi_trips.F_TRX_TAXI_TRIPS_DROPOFF_LOCATION_CNT_D`
order by 1;

SELECT *
FROM `teknasyoncase.taxi_trips.F_TRX_TAXI_TRIPS_ROUTE_CNT_D`
order by 1;


*/


