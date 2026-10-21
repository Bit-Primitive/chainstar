{#
  The few places where DuckDB and Postgres SQL differ. Everything else in the
  models is written in SQL both accept.
#}

{# Unix seconds to a UTC timestamp without time zone, independent of session time zone. #}
{% macro unix_to_utc_timestamp(column) -%}
    {{ return(adapter.dispatch('unix_to_utc_timestamp', 'chainstar')(column)) }}
{%- endmacro %}

{% macro default__unix_to_utc_timestamp(column) -%}
    (to_timestamp({{ column }}) at time zone 'UTC')
{%- endmacro %}

{% macro duckdb__unix_to_utc_timestamp(column) -%}
    make_timestamp(cast({{ column }} as bigint) * 1000000)
{%- endmacro %}


{# One row per calendar day between two date expressions, inclusive, as column date_day. #}
{% macro date_series(start_date, end_date, relation) -%}
    {{ return(adapter.dispatch('date_series', 'chainstar')(start_date, end_date, relation)) }}
{%- endmacro %}

{% macro default__date_series(start_date, end_date, relation) -%}
    select cast(series.day as date) as date_day
    from {{ relation }},
        generate_series(
            cast({{ start_date }} as timestamp),
            cast({{ end_date }} as timestamp),
            interval '1 day'
        ) as series(day)
{%- endmacro %}

{% macro duckdb__date_series(start_date, end_date, relation) -%}
    select cast(unnest(generate_series(
        cast({{ start_date }} as timestamp),
        cast({{ end_date }} as timestamp),
        interval 1 day
    )) as date) as date_day
    from {{ relation }}
{%- endmacro %}
