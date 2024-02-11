## Homework Week 3
For this homework we will be using the 2022 Green Taxi Trip Record Parquet Files from the New York
City Taxi Data found here: </br> https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page </br>
If you are using orchestration such as Mage, Airflow or Prefect do not load the data into Big Query using the orchestrator.</br> 
Stop with loading the files into a bucket. </br></br>
<u>NOTE:</u> You will need to use the PARQUET option files when creating an External Table</br>

<b>SETUP:</b></br>
Create an external table using the Green Taxi Trip Records Data for 2022. </br>
Create a table in BQ using the Green Taxi Trip Records for 2022 (do not partition or cluster this table). </br>
</p>

## Question 1:
Question 1: What is count of records for the 2022 Green Taxi Data??
- 65,623,481
- 840,402 ✔️
- 1,936,423
- 253,647

## Question 2:
Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.</br> 
What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

- 0 MB for the External Table and 6.41MB for the Materialized Table ✔️
- 18.82 MB for the External Table and 47.60 MB for the Materialized Table
- 0 MB for the External Table and 0MB for the Materialized Table
- 2.14 MB for the External Table and 0MB for the Materialized Table


## Question 3:
How many records have a fare_amount of 0?
- 12,488
- 128,219 
- 112
- 1,622 ✔️

## Question 4:
What is the best strategy to make an optimized table in Big Query if your query will always order the results by PUlocationID and filter based on lpep_pickup_datetime? (Create a new table with this strategy)
- Cluster on lpep_pickup_datetime Partition by PUlocationID
- Partition by lpep_pickup_datetime  Cluster on PUlocationID ✔️
- Partition by lpep_pickup_datetime and Partition by PUlocationID 
- Cluster on by lpep_pickup_datetime and Cluster on PUlocationID

## Question 5:
Write a query to retrieve the distinct PULocationID between lpep_pickup_datetime
06/01/2022 and 06/30/2022 (inclusive)</br>

Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values? </br>

Choose the answer which most closely matches.</br> 

- 22.82 MB for non-partitioned table and 647.87 MB for the partitioned table
- 12.82 MB for non-partitioned table and 1.12 MB for the partitioned table ✔️
- 5.63 MB for non-partitioned table and 0 MB for the partitioned table
- 10.31 MB for non-partitioned table and 10.31 MB for the partitioned table


## Question 6: 
Where is the data stored in the External Table you created?

- Big Query
- GCP Bucket ✔️
- Big Table
- Container Registry


## Question 7:
It is best practice in Big Query to always cluster your data:
- True
- False ✔️


## (Bonus: Not worth points) Question 8:
No Points: Write a `SELECT count(*)` query FROM the materialized table you created. How many bytes does it estimate will be read? Why?

<b>The query processed for 0 bytes when executed, this is due to partitioning and clusteringm, it helps reduce memory usage and improve performance by optimizing data storage and retrieval. Partitioning reduces the amount of data scanned for each query, while clustering organizes data within partitions to improve query performance.</b>

### SOLUTION

```
# Creates an external table using the Green Taxi Trip Records Data for 2022.
CREATE OR REPLACE EXTERNAL TABLE `mage-zoomcamp-413020.ny_taxi.external_green_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = [
    'gs://mage-zoomcamp-jp-2/green_tripdata_2022-01.parquet',
    'gs://mage-zoomcamp-jp-2/green_tripdata_2022-02.parquet',
    'gs://mage-zoomcamp-jp-2/green_tripdata_2022-03.parquet',
    'gs://mage-zoomcamp-jp-2/green_tripdata_2022-04.parquet',
    'gs://mage-zoomcamp-jp-2/green_tripdata_2022-05.parquet',
    'gs://mage-zoomcamp-jp-2/green_tripdata_2022-06.parquet',
    'gs://mage-zoomcamp-jp-2/green_tripdata_2022-07.parquet',
    'gs://mage-zoomcamp-jp-2/green_tripdata_2022-08.parquet',
    'gs://mage-zoomcamp-jp-2/green_tripdata_2022-09.parquet',
    'gs://mage-zoomcamp-jp-2/green_tripdata_2022-10.parquet',
    'gs://mage-zoomcamp-jp-2/green_tripdata_2022-11.parquet',
    'gs://mage-zoomcamp-jp-2/green_tripdata_2022-12.parquet'
  ]
);

# Creates a table in BQ using the Green Taxi Trip Records for 2022 (do not partition or cluster this table).
CREATE OR REPLACE TABLE mage-zoomcamp-413020.ny_taxi.green_tripdata_non_partitioned AS
SELECT * FROM mage-zoomcamp-413020.ny_taxi.external_green_tripdata;

# Question 1: What is count of records for the 2022 Green Taxi Data?
SELECT COUNT(*) FROM mage-zoomcamp-413020.ny_taxi.external_green_tripdata;

# Question 2: What is the estimated amount of data that will be read when this query is executed on the External Table and the Table? 

-- EXTERNAL TABLE
SELECT COUNT(DISTINCT PULocationID) FROM mage-zoomcamp-413020.ny_taxi.external_green_tripdata;

-- MATERIALIZED TABLE
SELECT COUNT(DISTINCT PULocationID) FROM mage-zoomcamp-413020.ny_taxi.green_tripdata_non_partitioned;

## Question 3: How many records have a fare_amount of 0?
SELECT COUNT(*) FROM mage-zoomcamp-413020.ny_taxi.external_green_tripdata WHERE fare_amount = 0;

## Question 4: What is the best strategy to make an optimized table in Big Query if your query will always order the results by PUlocationID and filter based on lpep_pickup_datetime?

CREATE OR REPLACE TABLE mage-zoomcamp-413020.ny_taxi.green_tripdata_partitoned_clustered
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY PULocationID AS
SELECT * FROM mage-zoomcamp-413020.ny_taxi.external_green_tripdata;

# Question 5: Write a query to retrieve the distinct PULocationID between lpep_pickup_datetime.

-- NON - PARTITIONED TABLE
SELECT DISTINCT(PULocationID) FROM mage-zoomcamp-413020.ny_taxi.green_tripdata_non_partitioned WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';

-- PARTITIONED TABLE (FROM QUESTION 4)
SELECT DISTINCT(PULocationID) FROM mage-zoomcamp-413020.ny_taxi.green_tripdata_partitoned_clustered WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';
```