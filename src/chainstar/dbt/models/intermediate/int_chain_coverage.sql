-- One row per chain: the first and last loaded block, and the earliest and latest
-- block date. dim_dates is built from these few rows instead of scanning every
-- block on every run. Bitcoin header times need not increase from block to
-- block, so the dates are merged as the earliest and latest seen, not taken from
-- the first and last block.
{{ config(
    materialized='incremental',
    unique_key='chain',
    incremental_strategy=('merge' if target.type == 'bigquery' else 'delete+insert'),
    on_schema_change='fail',
) }}

{%- set marks = chainstar.watermarks('last_block') %}

with new_blocks as (
    select
        chain,
        min(block_number) as first_block,
        max(block_number) as last_block,
        min(block_date) as first_date,
        max(block_date) as last_date
    from ({{ chainstar.int_blocks(marks) }}) as blocks
    group by chain
)

{% if is_incremental() %}
select
    new_blocks.chain as chain,
    coalesce(existing.first_block, new_blocks.first_block) as first_block,
    new_blocks.last_block as last_block,
    case
        when existing.first_date is null or existing.first_date > new_blocks.first_date
            then new_blocks.first_date
        else existing.first_date
    end as first_date,
    case
        when existing.last_date is null or existing.last_date < new_blocks.last_date
            then new_blocks.last_date
        else existing.last_date
    end as last_date
from new_blocks
left join {{ this }} as existing
    on existing.chain = new_blocks.chain
{% else %}
select chain, first_block, last_block, first_date, last_date
from new_blocks
{% endif %}
