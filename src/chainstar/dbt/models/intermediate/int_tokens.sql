-- Every asset that appears in int_transfers, in the columns all chains share.

-- EVM: token contracts that emitted an ERC-20 Transfer, with metadata cryo read.
with evm_tokens as (
    select distinct chain, token_address
    from {{ ref('stg_erc20_transfers') }}
),

-- Solana: SPL mints moved by a successful transfer. Decimals come from the data;
-- names and symbols live in separate metadata programs chainstar does not read.
spl_tokens as (
    select
        chain,
        mint as token_address,
        min(program) as token_standard,
        max(decimals) as decimals
    from {{ ref('stg_solana_transfers') }}
    where success and program <> 'system'
    group by chain, mint
),

native_assets as (
    select distinct chain
    from {{ ref('int_transfers') }}
    where token_address = 'native'
)

select
    evm_tokens.chain,
    evm_tokens.token_address,
    'erc20' as token_standard,
    metadata.name,
    metadata.symbol,
    metadata.decimals,
    metadata.metadata_block_number
from evm_tokens
left join {{ ref('stg_erc20_metadata') }} as metadata
    on metadata.chain = evm_tokens.chain
    and metadata.token_address = evm_tokens.token_address

union all

select
    chain,
    token_address,
    token_standard,
    cast(null as varchar),
    cast(null as varchar),
    decimals,
    cast(null as bigint)
from spl_tokens

union all

-- BTC and SOL themselves, under the token address 'native'.
select
    native_assets.chain,
    'native',
    'native',
    cast(null as varchar),
    chains.native_symbol,
    cast(chains.native_decimals as bigint),
    cast(null as bigint)
from native_assets
inner join {{ ref('chains') }} as chains
    on chains.chain = native_assets.chain
