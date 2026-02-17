{{
    config(
        materialized='view'
    )
}}

with source as (
    select * from {{ source('staging', 'fhv_tripdata') }}
),

renamed as (
    select
        dispatching_base_num,
        cast(PUlocationID as integer) as pickup_location_id,
        cast(DOlocationID as integer) as dropoff_location_id,
        cast(pickup_datetime as timestamp) as pickup_datetime,
        cast(dropOff_datetime as timestamp) as dropoff_datetime,
        cast(SR_Flag as integer) as sr_flag,
        Affiliated_base_number as affiliated_base_number
    from source
    where dispatching_base_num is not null
)

select * from renamed
