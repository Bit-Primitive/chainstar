-- One row per token contract that emitted at least one ERC-20 Transfer in the loaded data.
with tokens_seen as (
    select distinct chain, token_address
    from {{ ref('stg_erc20_transfers') }}
)

select
    {{ chainstar.chain_key('tokens_seen.chain', 'tokens_seen.token_address') }} as token_id,
    tokens_seen.chain,
    tokens_seen.token_address,
    metadata.name,
    metadata.symbol,
    metadata.decimals,
    metadata.metadata_block_number,
    coalesce(
        metadata.name is not null or metadata.symbol is not null or metadata.decimals is not null,
        false
    ) as has_metadata
from tokens_seen
left join {{ ref('stg_erc20_metadata') }} as metadata
    on metadata.chain = tokens_seen.chain
    and metadata.token_address = tokens_seen.token_address
