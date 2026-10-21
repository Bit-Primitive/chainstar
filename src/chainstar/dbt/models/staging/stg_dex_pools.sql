-- A pool can have several rows if it was read in more than one run; keep the
-- most recently read one.
with ranked as (
    select
        pools.*,
        row_number() over (
            partition by pools.chain_id, pools.pool
            order by pools.block_number desc
        ) as recency
    from {{ source('chainstar_raw', 'dex_pools') }} as pools
)

select
    chains.chain as chain,
    ranked.chain_id as chain_id,
    ranked.pool as pool_address,
    ranked.event_family as event_family,
    ranked.factory as factory_address,
    ranked.token0 as token0_address,
    ranked.token1 as token1_address,
    ranked.fee as fee,
    ranked.tick_spacing as tick_spacing,
    ranked.stable as is_stable,
    ranked.factory_confirms as factory_confirms,
    ranked.block_number as metadata_block_number
from ranked
inner join {{ ref('chains') }} as chains
    on chains.chain_id = ranked.chain_id
where ranked.recency = 1
