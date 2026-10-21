-- A token can have several metadata rows if it was fetched in more than one run;
-- keep the most recently read one.
with ranked as (
    select
        metadata.*,
        row_number() over (
            partition by metadata.chain_id, metadata.erc20
            order by metadata.block_number desc
        ) as recency
    from {{ source('chainstar_raw', 'erc20_metadata') }} as metadata
)

select
    chains.chain,
    ranked.chain_id,
    ranked.erc20 as token_address,
    nullif(ranked.name, '') as name,
    nullif(ranked.symbol, '') as symbol,
    ranked.decimals,
    ranked.block_number as metadata_block_number
from ranked
inner join {{ ref('chains') }} as chains
    on chains.chain_id = ranked.chain_id
where ranked.recency = 1
