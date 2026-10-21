-- One row per pool that emitted a swap event on Ethereum or Base, with what its
-- contract and its factory said about it (read once, at the chain head). Solana
-- swaps are derived per transaction, not per pool, so it has no rows here.
{{ config(
    unique_key='pool_id',
    indexes=chainstar.incremental_indexes('pool_id', 'last_seen_block'),
) }}

{%- set marks = chainstar.watermarks('last_seen_block') %}

with new_pools as (
    select
        chain,
        pool_address,
        min(block_number) as first_seen_block,
        max(block_number) as last_seen_block,
        cast(count(*) as bigint) as swap_events
    from {{ ref('stg_dex_swaps') }}
    where {{ chainstar.after_watermark(marks, 'evm') }}
    group by chain, pool_address
)

select
    {{ chainstar.chain_key('new_pools.chain', 'new_pools.pool_address') }} as pool_id,
    new_pools.chain as chain,
    new_pools.pool_address as pool_address,
    coalesce(venues.venue, 'unknown') as venue,
    pools.event_family as event_family,
    pools.factory_address as factory_address,
    pools.factory_confirms as factory_confirms,
    cast(
        coalesce(pools.factory_confirms, false) and venues.venue is not null
        as {{ chainstar.type_boolean() }}
    ) as is_verified,
    pools.token0_address as token0_address,
    pools.token1_address as token1_address,
    case when pools.token0_address is not null
        then {{ chainstar.chain_key('new_pools.chain', 'pools.token0_address') }}
    end as token0_id,
    case when pools.token1_address is not null
        then {{ chainstar.chain_key('new_pools.chain', 'pools.token1_address') }}
    end as token1_id,
    pools.fee as fee,
    pools.tick_spacing as tick_spacing,
    pools.is_stable as is_stable,
    pools.metadata_block_number as metadata_block_number,
{% if is_incremental() %}
    -- New rows are all above the watermark, so anything already here is older.
    case
        when existing.pool_id is null then new_pools.first_seen_block
        else existing.first_seen_block
    end as first_seen_block,
    new_pools.last_seen_block as last_seen_block,
    cast(coalesce(existing.swap_events, 0) + new_pools.swap_events as bigint) as swap_events
{% else %}
    new_pools.first_seen_block as first_seen_block,
    new_pools.last_seen_block as last_seen_block,
    new_pools.swap_events as swap_events
{% endif %}
from new_pools
left join {{ ref('stg_dex_pools') }} as pools
    on pools.chain = new_pools.chain
    and pools.pool_address = new_pools.pool_address
left join {{ ref('dex_venues') }} as venues
    on venues.chain = pools.chain
    and venues.factory_address = pools.factory_address
    and venues.event_family = pools.event_family
{% if is_incremental() %}
left join {{ this }} as existing
    on existing.pool_id = {{ chainstar.chain_key('new_pools.chain', 'new_pools.pool_address') }}
{% endif %}
