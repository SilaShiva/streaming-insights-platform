-- Staging: typed, renamed, deduped view over the raw landed events.
-- Dedup keeps the latest occurrence per event_id (at-least-once upstream).

with raw as (
    select * from {{ source('raw', 'raw_events') }}
),

deduped as (
    select *
    from raw
    qualify row_number() over (
        partition by event_id
        order by ts desc
    ) = 1
)

select
    cast(event_id as varchar)        as event_id,
    cast(event_type as varchar)      as event_type,
    cast(ts as timestamp)            as event_ts,
    cast(order_id as varchar)        as order_id,
    cast(city as varchar)            as city,
    cast(restaurant_id as varchar)   as restaurant_id,
    cast(customer_id as varchar)     as customer_id,
    cast(promised_minutes as integer)   as promised_minutes,
    cast(actual_minutes as integer)     as actual_minutes,
    cast(delay_minutes as integer)      as delay_minutes,
    cast(review_text as varchar)     as review_text,
    cast(rating as integer)          as rating,
    cast(prep_minutes as integer)    as prep_minutes,
    cast(riders_online as integer)   as riders_online,
    cast(traffic_index as double)    as traffic_index,
    cast(weather_bad as integer)     as weather_bad
from deduped
