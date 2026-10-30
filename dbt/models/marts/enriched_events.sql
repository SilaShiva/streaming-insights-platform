-- Enriched events exposed as a dbt model so marts can ref() it.
-- Passes through the enrichment consumer's table with typed columns.

select
    cast(event_id as varchar)            as event_id,
    cast(event_type as varchar)          as event_type,
    cast(ts as timestamp)                as event_ts,
    cast(order_id as varchar)            as order_id,
    cast(city as varchar)                as city,
    cast(restaurant_id as varchar)       as restaurant_id,
    cast(promised_minutes as integer)    as promised_minutes,
    cast(actual_minutes as integer)      as actual_minutes,
    cast(delay_minutes as integer)       as delay_minutes,
    cast(rating as integer)              as rating,
    cast(review_text as varchar)         as review_text,
    cast(delay_reason as varchar)        as delay_reason,
    cast(sentiment as varchar)           as sentiment,
    cast(enrich_source as varchar)       as enrich_source
from {{ source('raw', 'enriched_events') }}
