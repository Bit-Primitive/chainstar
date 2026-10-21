{#
  The places where the warehouses' SQL differs. Everything else in the models is
  written in SQL that DuckDB, Postgres, ClickHouse, BigQuery and Snowflake accept;
  type names go through the type_* macros below where they differ.
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

{% macro clickhouse__unix_to_utc_timestamp(column) -%}
    toDateTime64({{ column }}, 6, 'UTC')
{%- endmacro %}

{% macro bigquery__unix_to_utc_timestamp(column) -%}
    datetime(timestamp_seconds({{ column }}))
{%- endmacro %}

{% macro snowflake__unix_to_utc_timestamp(column) -%}
    to_timestamp_ntz({{ column }})
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

{% macro clickhouse__date_series(start_date, end_date, relation) -%}
    select arrayJoin(arrayMap(
        offset -> toDate({{ start_date }}) + offset,
        range(toUInt32(dateDiff('day', toDate({{ start_date }}), toDate({{ end_date }})) + 1))
    )) as date_day
    from {{ relation }}
{%- endmacro %}

{% macro bigquery__date_series(start_date, end_date, relation) -%}
    select date_day
    from {{ relation }},
        unnest(generate_date_array(cast({{ start_date }} as date), cast({{ end_date }} as date)))
            as date_day
{%- endmacro %}

{% macro snowflake__date_series(start_date, end_date, relation) -%}
    {#- generator() needs a constant row count: 100,000 days is over 270 years. -#}
    select dateadd(day, series.day_offset, cast({{ start_date }} as date)) as date_day
    from {{ relation }},
        (
            select row_number() over (order by seq4()) - 1 as day_offset
            from table(generator(rowcount => 100000))
        ) as series
    where series.day_offset <= datediff(day, {{ start_date }}, {{ end_date }})
{%- endmacro %}


{#
  A part of a date: year, quarter, month, day, isodow (1 Monday to 7 Sunday) or
  isoweek (the ISO 8601 week number).
#}
{% macro date_part(part, column) -%}
    {{ return(adapter.dispatch('date_part', 'chainstar')(part, column)) }}
{%- endmacro %}

{% macro default__date_part(part, column) -%}
    {#- In DuckDB and Postgres, week is the ISO week. -#}
    extract({{ {'isoweek': 'week'}.get(part, part) }} from {{ column }})
{%- endmacro %}

{% macro clickhouse__date_part(part, column) -%}
    {%- set functions = {
        'year': 'toYear', 'quarter': 'toQuarter', 'month': 'toMonth',
        'day': 'toDayOfMonth', 'isodow': 'toDayOfWeek', 'isoweek': 'toISOWeek',
    } -%}
    {{ functions[part] }}({{ column }})
{%- endmacro %}

{% macro bigquery__date_part(part, column) -%}
    {%- if part == 'isodow' -%}
        {#- BigQuery's dayofweek counts from 1 on Sunday. -#}
        (mod(extract(dayofweek from {{ column }}) + 5, 7) + 1)
    {%- else -%}
        extract({{ part }} from {{ column }})
    {%- endif -%}
{%- endmacro %}

{% macro snowflake__date_part(part, column) -%}
    {%- set functions = {
        'year': 'year', 'quarter': 'quarter', 'month': 'month',
        'day': 'day', 'isodow': 'dayofweekiso', 'isoweek': 'weekiso',
    } -%}
    {{ functions[part] }}({{ column }})
{%- endmacro %}


{#
  Type names that differ: BigQuery has STRING, BOOL, FLOAT64 and NUMERIC or
  BIGNUMERIC; the others take the standard names. bigint and integer are
  accepted everywhere.
#}
{% macro type_text() -%}
    {{ return(adapter.dispatch('type_text', 'chainstar')()) }}
{%- endmacro %}

{% macro default__type_text() -%}varchar{%- endmacro %}

{% macro bigquery__type_text() -%}string{%- endmacro %}

{% macro type_boolean() -%}
    {{ return(adapter.dispatch('type_boolean', 'chainstar')()) }}
{%- endmacro %}

{% macro default__type_boolean() -%}boolean{%- endmacro %}

{% macro bigquery__type_boolean() -%}bool{%- endmacro %}

{% macro type_double() -%}
    {{ return(adapter.dispatch('type_double', 'chainstar')()) }}
{%- endmacro %}

{% macro default__type_double() -%}double precision{%- endmacro %}

{% macro bigquery__type_double() -%}float64{%- endmacro %}

{% macro type_decimal(precision, scale) -%}
    {{ return(adapter.dispatch('type_decimal', 'chainstar')(precision, scale)) }}
{%- endmacro %}

{% macro default__type_decimal(precision, scale) -%}decimal({{ precision }}, {{ scale }}){%- endmacro %}

{% macro bigquery__type_decimal(precision, scale) -%}
    {#- NUMERIC holds at most 29 digits before the point. -#}
    {{ 'numeric' if precision - scale <= 29 else 'bignumeric' }}({{ precision }}, {{ scale }})
{%- endmacro %}


{# A column name quoted as the warehouse stores it, for names that are SQL keywords. #}
{% macro quoted(name) -%}
    {{ return(adapter.dispatch('quoted', 'chainstar')(name)) }}
{%- endmacro %}

{% macro default__quoted(name) -%}"{{ name }}"{%- endmacro %}

{% macro bigquery__quoted(name) -%}`{{ name }}`{%- endmacro %}

{#- chainstar creates Snowflake's raw tables with upper-case names, as Snowflake
    folds the unquoted names in these models. -#}
{% macro snowflake__quoted(name) -%}"{{ name | upper }}"{%- endmacro %}

