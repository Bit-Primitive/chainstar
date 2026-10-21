-- One row per asset moved in the loaded data: ERC-20 token contracts, SPL mints,
-- and each chain's native asset (BTC, SOL) under the token address 'native'.
select
    {{ chainstar.chain_key('tokens.chain', 'tokens.token_address') }} as token_id,
    tokens.chain,
    tokens.token_address,
    tokens.token_standard,
    tokens.name,
    tokens.symbol,
    tokens.decimals,
    tokens.metadata_block_number,
    coalesce(
        tokens.name is not null or tokens.symbol is not null or tokens.decimals is not null,
        false
    ) as has_metadata
from {{ ref('int_tokens') }} as tokens
