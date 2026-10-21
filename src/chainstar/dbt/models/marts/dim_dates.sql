-- Every calendar day (UTC) from the earliest to the latest loaded block, across chains.
with bounds as (
    select
        min(block_date) as first_date,
        max(block_date) as last_date
    from {{ ref('stg_blocks') }}
),

days as (
    {{ chainstar.date_series('bounds.first_date', 'bounds.last_date', 'bounds') }}
)

select
    date_day,
    cast(extract(year from date_day) as integer) as year,
    cast(extract(quarter from date_day) as integer) as quarter,
    cast(extract(month from date_day) as integer) as month,
    cast(extract(day from date_day) as integer) as day_of_month,
    cast(extract(isodow from date_day) as integer) as iso_day_of_week,
    cast(extract(week from date_day) as integer) as iso_week,
    extract(isodow from date_day) in (6, 7) as is_weekend
from days
where date_day is not null
