-- Every calendar day (UTC) from the earliest to the latest loaded block, across chains.
-- A few hundred rows a year, so it is rebuilt in full on every run, from the
-- one-row-per-chain summary in int_chain_coverage rather than from every block.
{{ config(materialized='table') }}

with bounds as (
    select
        min(first_date) as first_date,
        max(last_date) as last_date
    from {{ ref('int_chain_coverage') }}
),

days as (
    {{ chainstar.date_series('bounds.first_date', 'bounds.last_date', 'bounds') }}
)

select
    date_day,
    cast({{ chainstar.date_part('year', 'date_day') }} as integer) as year,
    cast({{ chainstar.date_part('quarter', 'date_day') }} as integer) as quarter,
    cast({{ chainstar.date_part('month', 'date_day') }} as integer) as month,
    cast({{ chainstar.date_part('day', 'date_day') }} as integer) as day_of_month,
    cast({{ chainstar.date_part('isodow', 'date_day') }} as integer) as iso_day_of_week,
    cast({{ chainstar.date_part('isoweek', 'date_day') }} as integer) as iso_week,
    cast({{ chainstar.date_part('isodow', 'date_day') }} in (6, 7) as {{ chainstar.type_boolean() }}) as is_weekend
from days
where date_day is not null
