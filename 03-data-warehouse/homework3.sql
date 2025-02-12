CREATE OR REPLACE EXTERNAL TABLE `dtc-de-448703.trips_data_all.external_yellow_tripdata`
OPTIONS (
  format = 'Parquet',
  uris = ['gs://dtc_data_lake_448703/yellow_tripdata_2024-0*.parquet']
);

select count(*) from `dtc-de-448703.trips_data_all.external_yellow_tripdata`;

CREATE OR REPLACE TABLE dtc-de-448703.trips_data_all.yellow_tripdata_non_partitoned AS
SELECT * FROM dtc-de-448703.trips_data_all.external_yellow_tripdata;

select distinct PULocationID from dtc-de-448703.trips_data_all.external_yellow_tripdata;

SELECT distinct PULocationID FROM dtc-de-448703.trips_data_all.yellow_tripdata_non_partitoned;

SELECT * FROM dtc-de-448703.trips_data_all.yellow_tripdata_non_partitoned where fare_amount=0;

CREATE OR REPLACE TABLE dtc-de-448703.trips_data_all.yellow_tripdata_partitoned
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT * FROM dtc-de-448703.trips_data_all.yellow_tripdata_non_partitoned;

select distinct VendorID from dtc-de-448703.trips_data_all.yellow_tripdata_partitoned where tpep_dropoff_datetime >='2024-03-01' and tpep_dropoff_datetime <='2024-03-15';
select distinct VendorID from dtc-de-448703.trips_data_all.yellow_tripdata_non_partitoned where tpep_dropoff_datetime >='2024-03-01' and tpep_dropoff_datetime <='2024-03-15';