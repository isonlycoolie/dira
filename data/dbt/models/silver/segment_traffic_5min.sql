{{ config(materialized='view') }}

with traffic_events_source as (
    select
        te.road_segment_id,
        te.event_time,
        te.vehicle_count,
        te.avg_speed_kmh,
        te.flow_rate,
        te.weather_factor,
        te.incident_flag,
        coalesce(re.speed_limit_kmh, 50) as free_flow_speed_kmh,
        date_trunc('hour', te.event_time)
            + (floor(extract(minute from te.event_time) / 5)::int * interval '5 minutes') as window_start
    from {{ source('dira', 'traffic_events') }} as te
    left join {{ source('dira', 'road_edges') }} as re
        on re.id = te.road_segment_id
),
window_ranked as (
    select
        *,
        row_number() over (
            partition by road_segment_id, window_start
            order by event_time desc
        ) as event_rank
    from traffic_events_source
),
five_minute_windows as (
    select
        road_segment_id,
        window_start,
        window_start + interval '5 minutes' as window_end,
        sum(coalesce(vehicle_count, 0)) as vehicle_count,
        avg(avg_speed_kmh) as avg_speed_kmh,
        sum(coalesce(flow_rate, 0.0)) as flow_rate,
        max(free_flow_speed_kmh) as free_flow_speed_kmh,
        max(case when event_rank = 1 then weather_factor end) as weather_factor
    from window_ranked
    group by 1, 2
),
window_features as (
    select
        *,
        greatest(max(coalesce(vehicle_count, 0)) over segment_windows, 1) as capacity,
        lag(avg_speed_kmh, 1) over segment_windows as upstream_speed_kmh,
        case
            when avg_speed_kmh < 5
             and lag(avg_speed_kmh, 1) over segment_windows < 5
             and lag(avg_speed_kmh, 2) over segment_windows < 5
            then true
            else false
        end as dwell_flag
    from five_minute_windows
    window segment_windows as (
        partition by road_segment_id
        order by window_start
    )
),
scored_windows as (
    select
        *,
        greatest(
            0.0,
            least(
                1.0,
                (
                    (1.0 - least(coalesce(avg_speed_kmh, 0.0) / nullif(free_flow_speed_kmh, 0.0), 1.0)) * 0.6
                    + least(coalesce(vehicle_count::numeric, 0.0) / nullif(capacity::numeric, 0.0), 1.0) * 0.4
                )
            )
        ) as congestion_score
    from window_features
)
select
    road_segment_id,
    window_start as event_time,
    window_end,
    vehicle_count,
    avg_speed_kmh,
    flow_rate,
    upstream_speed_kmh,
    weather_factor,
    dwell_flag,
    congestion_score,
    case
        when congestion_score < 0.30 then 'free_flow'
        when congestion_score < 0.50 then 'light'
        when congestion_score < 0.70 then 'moderate'
        when congestion_score < 0.90 then 'heavy'
        else 'severe'
    end as congestion_level
from scored_windows
