CREATE OR REPLACE TABLE `kenil.uber_data_engineering.tbl_analytics` AS (
SELECT 
f.trip_id,
f.VendorID,
d.tpep_pickup_datetime,
d.tpep_dropoff_datetime,
p.passenger_count,
t.trip_distance,
r.rate_code_name,
pay.payment_type_name,
f.fare_amount,
f.extra,
f.mta_tax,
f.tip_amount,
f.tolls_amount,
f.improvement_surcharge,
f.total_amount

FROM 

`kenil.uber_data_engineering.fact_table` f
JOIN `kenil.uber_data_engineering.datetime_dim` d  ON f.datetime_id=d.datetime_id
JOIN `kenil.uber_data_engineering.passenger_count_dim` p  ON p.passenger_count_id=f.passenger_count_id  
JOIN `kenil.uber_data_engineering.trip_distance_dim` t  ON t.trip_distance_id=f.trip_distance_id  
JOIN `kenil.uber_data_engineering.rate_code_dim` r ON r.rate_code_id=f.rate_code_id
JOIN `kenil.uber_data_engineering.payment_type_dim` pay ON pay.payment_type_id=f.payment_type_id)
;