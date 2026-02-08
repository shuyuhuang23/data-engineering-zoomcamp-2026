-- 1. CREATE EXTERNAL TABLE
CREATE OR REPLACE EXTERNAL TABLE `dtc-de-course-485414.zoomcamp.yellow_tripdata_ext`
OPTIONS (
  format = 'PARQUET',
  uris = [
    'gs://dtc-de-course-485414-kestra-bucket/yellow_tripdata_2024-01.parquet',
    'gs://dtc-de-course-485414-kestra-bucket/yellow_tripdata_2024-02.parquet',
    'gs://dtc-de-course-485414-kestra-bucket/yellow_tripdata_2024-03.parquet',
    'gs://dtc-de-course-485414-kestra-bucket/yellow_tripdata_2024-04.parquet',
    'gs://dtc-de-course-485414-kestra-bucket/yellow_tripdata_2024-05.parquet',
    'gs://dtc-de-course-485414-kestra-bucket/yellow_tripdata_2024-06.parquet'
  ]
);

-- 2. CREATE REGULAR TABLE
CREATE OR REPLACE TABLE `dtc-de-course-485414.zoomcamp.yellow_tripdata_reg` AS
SELECT 
    *, 
    REGEXP_EXTRACT(_FILE_NAME, r'[^/]+$') AS source_parquet_file 
FROM `dtc-de-course-485414.zoomcamp.yellow_tripdata_ext`;

Question 1. 
SELECT COUNT(1) FROM `dtc-de-course-485414.zoomcamp.yellow_tripdata_ext`;

Question 2. 
SELECT COUNT(DISTINCT `PULocationID`) FROM `dtc-de-course-485414.zoomcamp.yellow_tripdata_ext`;
SELECT COUNT(DISTINCT `PULocationID`) FROM `dtc-de-course-485414.zoomcamp.yellow_tripdata_reg`;

Question 4. 
SELECT  COUNT(1) FROM `dtc-de-course-485414.zoomcamp.yellow_tripdata_reg`
WHERE fare_amount = 0;

Question 5. 
-- 3. CREATE TABLE WITH PARTITIONING AND CLUSTERING
CREATE OR REPLACE TABLE `dtc-de-course-485414.zoomcamp.yellow_tripdata_partition`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID
AS
SELECT *
FROM `dtc-de-course-485414.zoomcamp.yellow_tripdata_ext`;

Question 6. 
SELECT COUNT(DISTINCT VendorID) FROM `dtc-de-course-485414.zoomcamp.yellow_tripdata_reg`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';
SELECT COUNT(DISTINCT VendorID) FROM `dtc-de-course-485414.zoomcamp.yellow_tripdata_partition`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';