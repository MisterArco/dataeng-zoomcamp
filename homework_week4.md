## Module 4 Homework 

### Question 1: 

**What happens when we execute dbt build --vars '{'is_test_run':'true'}'**
You'll need to have completed the ["Build the first dbt models"](https://www.youtube.com/watch?v=UVI30Vxzd6c) video. 
- It's the same as running *dbt build* 
- It applies a _limit 100_ to all of our models
- It applies a _limit 100_ only to our staging models ✔️
- Nothing

### Question 2: 

**What is the code that our CI job will run? Where is this code coming from?**  

- The code that has been merged into the main branch
- The code that is behind the creation object on the dbt_cloud_pr_ schema
- The code from any development branch that has been opened based on main
- The code from the development branch we are requesting to merge to main ✔️


### Question 3 (2 points)

**What is the count of records in the model fact_fhv_trips after running all dependencies with the test run variable disabled (:false)?**  
Create a staging model for the fhv data, similar to the ones made for yellow and green data. Add an additional filter for keeping only records with pickup time in year 2019.
Do not add a deduplication step. Run this models without limits (is_test_run: false).

Create a core model similar to fact trips, but selecting from stg_fhv_tripdata and joining with dim_zones.
Similar to what we've done in fact_trips, keep only records with known pickup and dropoff locations entries for pickup and dropoff locations. 
Run the dbt model without limits (is_test_run: false).

- 12998722
- 22998722 ✔️
- 32998722
- 42998722

![](resources/images_week_4/counts.png)

```
# STAGING MODEL
{{
    config(
        materialized='view'
    )
}}

select *
from {{ source('staging', 'fhv_tripdata') }}
where EXTRACT(YEAR FROM pickup_datetime) = 2019

# CORE MODEL
{{
    config(
        materialized='table'
    )
}}

with fhv_tripdata as (
    select * from {{ ref('stg_fhv_tripdata') }}
), 
dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select 
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone  
from fhv_tripdata
inner join dim_zones as pickup_zone
on fhv_tripdata.PUlocationID = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on fhv_tripdata.DOlocationID = dropoff_zone.locationid
```

### Question 4 (2 points)

**What is the service that had the most rides during the month of July 2019 month with the biggest amount of rides after building a tile for the fact_fhv_trips table?**

Create a dashboard with some tiles that you find interesting to explore the data. One tile should show the amount of trips per month, as done in the videos for fact_trips, including the fact_fhv_trips data.

- FHV ✔️
- Green
- Yellow
- FHV and Green

![](resources/images_week_4/chart1.png)

![](resources/images_week_4/chart2.png)



