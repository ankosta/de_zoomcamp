-- Q1--------------
SELECT count(*) from `caramel-park-375820.week_3_fhv.ny_taxi`
-------------------

-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `caramel-park-375820.week_3_fhv.external_ny_taxi`
OPTIONS (
  format = 'CSV',
  uris = ['gs://dtc_data_lake_caramel-park-375820/data/fhv/fhv_tripdata_2019-*.csv.gz']
);

-- Check fhv trip data
SELECT * FROM `caramel-park-375820.week_3_fhv.external_ny_taxi` limit 10;

---Q2-----------------
SELECT count(*)
FROM `caramel-park-375820.week_3_fhv.external_ny_taxi`
WHERE PUlocationID IS NULL AND DOlocationID IS NULL;
----------------------

-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE `caramel-park-375820.week_3_fhv.external_ny_taxi_non_partitoned` AS
SELECT * FROM `caramel-park-375820.week_3_fhv.external_ny_taxi`;


-- Create a partitioned table from external table
CREATE OR REPLACE TABLE `caramel-park-375820.week_3_fhv.external_ny_taxi_partitoned`
PARTITION BY
  DATE(pickup_datetime) AS
SELECT * FROM `caramel-park-375820.week_3_fhv.external_ny_taxi`;

-- Let's look into the partitons
-- SELECT table_name, partition_id, total_rows
-- FROM `week_3_fhv.INFORMATION_SCHEMA.PARTITIONS`
-- WHERE table_name = 'external_ny_taxi_partitoned'
-- ORDER BY total_rows DESC;

-- Creating a partition and cluster table
CREATE OR REPLACE TABLE `caramel-park-375820.week_3_fhv.external_ny_taxi_partitoned_clustered`
PARTITION BY DATE(pickup_datetime)
CLUSTER BY Affiliated_base_number AS
SELECT * FROM `caramel-park-375820.week_3_fhv.external_ny_taxi`;

--Q5--------------------------
-- Impact of partition
-- Scanning 647.87 MB of data
SELECT DISTINCT(Affiliated_base_number)
FROM `caramel-park-375820.week_3_fhv.external_ny_taxi_non_partitoned`
WHERE DATE(pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-31';

-- Scanning ~0 MB of DATA
SELECT DISTINCT(Affiliated_base_number)
FROM `caramel-park-375820.week_3_fhv.external_ny_taxi_partitoned_clustered`
WHERE DATE(pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-31';
--------------------------------
