-- Mart: per-city, per-day delay analytics.
-- Grain: (city, event_date). Partitioned by date in BigQuery; simple table in DuckDB.

with delivered as (
    select
        city,
        cast(event_ts as date) as event_date,
        order_id,
        promised_minutes,
        actual_minutes,
        delay_minutes,
        prep_minutes,
        riders_online,
        traffic_index,
        weather_bad
    from {{ ref('stg_events') }}
    where event_type = 'order_delivered'
)

select
    city,
    event_date,
    count(*)                                   as deliveries,
    round(avg(delay_minutes), 2)               as avg_delay_minutes,
    round(max(delay_minutes), 2)               as max_delay_minutes,
    round(
        100.0 * sum(case when delay_minutes > 10 then 1 else 0 end) / count(*),
        2
    )                                          as pct_late_over_10min,
    round(avg(prep_minutes), 2)                as avg_prep_minutes,
    round(avg(riders_online), 1)               as avg_riders_online,
    round(avg(traffic_index), 3)               as avg_traffic_index,
    round(100.0 * sum(weather_bad) / count(*), 2) as pct_bad_weather
from delivered
group by city, event_date
