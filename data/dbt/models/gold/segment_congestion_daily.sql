{{ config(materialized='view') }}

with daily_windows as (
    select
        road_segment_id,
        event_time,
        date_trunc('day', event_time) as event_date,
        congestion_score
    from {{ ref('segment_traffic_5min') }}
),
daily_ranked as (
    select
        *,
        row_number() over (
            partition by road_segment_id, event_date
            order by congestion_score desc, event_time asc
        ) as peak_rank
    from daily_windows
)
select
    road_segment_id,
    event_date,
    max(congestion_score) as max_congestion_score,
    avg(congestion_score) as avg_congestion_score,
    sum(case when congestion_score >= 0.70 then 5.0 / 60.0 else 0.0 end) as total_congestion_hours,
    max(case when peak_rank = 1 then extract(hour from event_time)::int end) as peak_hour
from daily_ranked
group by road_segment_id, event_date
