{#
  Incremental builds. `chainstar sync` loads blocks in order, so every incremental
  model keeps, per chain, the highest block it already holds (its watermark) and
  processes only rows above it. Watermarks are read from each model's own table,
  so a build that failed half-way resumes on the next run: each model catches up
  from where it stopped, and none counts a row twice.

  That holds only while blocks arrive above the watermarks. chainstar therefore
  rebuilds in full (`dbt build --full-refresh`) whenever older blocks change: on
  a chain's first sync, after a rollback for a reorganisation, and on every
  `chainstar run`.
#}

{# Per-chain watermark of `column` in this model's table; none in a full build. #}
{% macro watermarks(column) %}
    {%- if not is_incremental() -%}
        {{ return(none) }}
    {%- endif -%}
    {%- set marks = {} -%}
    {%- if execute -%}
        {#- One lookup per chain rather than a GROUP BY: on Postgres each uses the
            (chain, block) index instead of scanning the whole table. -#}
        {%- set lookups = [] -%}
        {%- for chain in var('chainstar_chains') -%}
            {%- do lookups.append(
                "select '" ~ chain ~ "' as chain, (select max(" ~ column ~ ") from "
                ~ this ~ " where chain = '" ~ chain ~ "') as mark"
            ) -%}
        {%- endfor -%}
        {%- for row in run_query(lookups | join(' union all ')).rows -%}
            {%- if row[1] is not none -%}
                {%- do marks.update({row[0]: row[1] | int}) -%}
            {%- endif -%}
        {%- endfor -%}
    {%- endif -%}
    {{ return(marks) }}
{% endmacro %}


{#
  A filter keeping rows above the watermarks, written against staging columns so
  the database applies it while reading the raw tables. EVM rows are matched on
  chain_id, which staging takes straight from the raw table, rather than on the
  chain name, which comes from a join. `family` is evm, bitcoin or solana. A
  chain with no watermark yet keeps all its rows.
#}
{% macro after_watermark(marks, family, block_column='block_number', chain_id_column='chain_id') -%}
    {%- if marks is none -%}
        (1 = 1)
    {%- elif family == 'evm' -%}
        ({%- for chain, chain_id in var('chainstar_chains').items() if chain_id is not none -%}
            ({{ chain_id_column }} = {{ chain_id }}
            {%- if chain in marks %} and {{ block_column }} > {{ marks[chain] }}{% endif -%})
            {%- if not loop.last %} or {% endif -%}
        {%- endfor -%})
    {%- elif family in marks -%}
        ({{ block_column }} > {{ marks[family] }})
    {%- else -%}
        (1 = 1)
    {%- endif -%}
{%- endmacro %}


{# Postgres indexes for an incremental table: its unique key, and the watermark lookup. #}
{% macro incremental_indexes(unique_key, watermark_column) -%}
    {%- if target.type == 'postgres' -%}
        {{ return([
            {'columns': [unique_key], 'unique': true},
            {'columns': ['chain', watermark_column]},
        ]) }}
    {%- endif -%}
    {{ return([]) }}
{%- endmacro %}
