{{ config(materialized='view') }}

select 

    -- identifiers
    cast(dispatching_base_num as string) as  dispatch_num,
    cast(PUlocationID as integer) as  pickup_locationid,
    cast(DOlocationID as integer) as dropoff_locationid,

    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropOff_datetime as timestamp) as dropoff_datetime,
    
    -- trip info
    cast(SR_Flag as integer) as sr_flag,
    cast(Affiliated_base_number as string) as aff_base_num
    
from {{ source('staging','fhv_tripdata') }}
where dispatching_base_num is not null
-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}