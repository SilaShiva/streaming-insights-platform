-- Mart: review sentiment + delay-reason mix from enrichment.
-- Grain: (city, event_date, delay_reason).

with reviews as (
    select
        e.city,
        cast(e.event_ts as date) as event_date,
        r.delay_reason,
        r.sentiment,
        r.enrich_source,
        e.rating
    from {{ ref('stg_events') }} e
    inner join {{ ref('enriched_events') }} r
        on e.event_id = r.event_id
    where e.event_type = 'review_posted'
      and e.review_text is not null
)

select
    city,
    event_date,
    delay_reason,
    count(*)                                        as reviews,
    round(avg(rating), 2)                           as avg_rating,
    round(100.0 * sum(case when sentiment = 'negative' then 1 else 0 end) / count(*), 2)
                                                    as pct_negative,
    round(100.0 * sum(case when enrich_source = 'llm' then 1 else 0 end) / count(*), 2)
                                                    as pct_llm_enriched
from reviews
group by city, event_date, delay_reason
