

DECLARE DS_DATETIME DATETIME DEFAULT NULL;
DECLARE DS_DATE DATE DEFAULT NULL;
SET (DS_DATETIME,DS_DATE) = ((SELECT CURRENT_DATETIME('Turkey')),(SELECT MAX(DT_TRX_DATE)-1 FROM `teknasyoncase.taxi_trips.TRX_TAXI_TRIPS`));

TRUNCATE TABLE `teknasyoncase.taxi_trips.ODS_TRX_TAXI_TRIPS`;

INSERT INTO `teknasyoncase.taxi_trips.ODS_TRX_TAXI_TRIPS`
(vendor_id, 
pickup_datetime, 
dropoff_datetime, 
store_and_fwd_flag, 
rate_code, 
passenger_count, 
trip_distance, 
fare_amount, 
extra, 
mta_tax, 
tip_amount, 
tolls_amount, 
ehail_fee, 
airport_fee, 
total_amount, 
payment_type, 
distance_between_service, 
time_between_service, 
trip_type, 
imp_surcharge, 
pickup_location_id, 
dropoff_location_id, 
data_file_year, 
data_file_month)
SELECT vendor_id, 
pickup_datetime, 
dropoff_datetime, 
store_and_fwd_flag, 
rate_code, 
passenger_count, 
trip_distance, 
fare_amount, 
extra, 
mta_tax, 
tip_amount, 
tolls_amount, 
ehail_fee, 
airport_fee, 
total_amount, 
payment_type, 
distance_between_service, 
time_between_service, 
trip_type, 
imp_surcharge, 
pickup_location_id, 
dropoff_location_id, 
data_file_year, 
data_file_month
FROM `bigquery-public-data.new_york_taxi_trips.tlc_green_trips_2014` 
WHERE 1 = 1 
AND DATE(pickup_datetime) BETWEEN DS_DATE AND '2014-03-08';

-- Değişen kayıtları ods'ten gün kırılımında alır.

DROP TABLE IF EXISTS `teknasyoncase.taxi_trips.TEMP_DIST_DT_TRX_DATE`;

CREATE TABLE `teknasyoncase.taxi_trips.TEMP_DIST_DT_TRX_DATE` AS
SELECT DISTINCT DATE(pickup_datetime)AS DT_TRX_DATE
FROM `teknasyoncase.taxi_trips.ODS_TRX_TAXI_TRIPS`
WHERE 1 = 1;


-- Değişen kayıtları gün kırılımında siler.
DELETE `teknasyoncase.taxi_trips.TRX_TAXI_TRIPS` d
WHERE EXISTS (SELECT 0
FROM `teknasyoncase.taxi_trips.TEMP_DIST_DT_TRX_DATE` t
WHERE d.DT_TRX_DATE = t.DT_TRX_DATE);

DROP TABLE IF EXISTS `teknasyoncase.taxi_trips.TEMP_DIST_DT_TRX_DATE`;

-- Değişen kayıtları gün kırılımında insert eder.
INSERT INTO `teknasyoncase.taxi_trips.TRX_TAXI_TRIPS`
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
DS_DATETIME AS DT_INSERT_DTTM,
DS_DATETIME AS DT_UPDATE_DTTM
FROM `teknasyoncase.taxi_trips.ODS_TRX_TAXI_TRIPS` 
WHERE 1 = 1 
AND pickup_datetime < dropoff_datetime;




