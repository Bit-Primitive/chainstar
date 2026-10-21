-- Every address that sent or received an ERC-20 transfer or sent a transaction.
-- "Wallet" means any address here: some are contracts, which this release does not
-- distinguish.
with appearances as (
    select chain, from_address as address, block_number, 1 as transfers_out, 0 as transfers_in, 0 as transactions_sent
    from {{ ref('stg_erc20_transfers') }}
    union all
    select chain, to_address, block_number, 0, 1, 0
    from {{ ref('stg_erc20_transfers') }}
    union all
    select chain, from_address, block_number, 0, 0, 1
    from {{ ref('stg_transactions') }}
),

wallets as (
    select
        chain,
        address,
        min(block_number) as first_seen_block,
        max(block_number) as last_seen_block,
        -- Cast explicitly: DuckDB widens a sum of integers past bigint, and
        -- that type arrives in Parquet and downstream tools as a float.
        cast(sum(transfers_out) as bigint) as transfers_out,
        cast(sum(transfers_in) as bigint) as transfers_in,
        cast(sum(transactions_sent) as bigint) as transactions_sent
    from appearances
    group by chain, address
)

select
    {{ chainstar.chain_key('wallets.chain', 'wallets.address') }} as wallet_id,
    wallets.chain,
    wallets.address,
    -- The zero address stands for mints (as sender) and burns (as recipient).
    wallets.address = '0x0000000000000000000000000000000000000000' as is_zero_address,
    wallets.first_seen_block,
    first_block.block_timestamp as first_seen_at,
    wallets.last_seen_block,
    last_block.block_timestamp as last_seen_at,
    wallets.transfers_out,
    wallets.transfers_in,
    wallets.transactions_sent
from wallets
left join {{ ref('stg_blocks') }} as first_block
    on first_block.chain = wallets.chain
    and first_block.block_number = wallets.first_seen_block
left join {{ ref('stg_blocks') }} as last_block
    on last_block.chain = wallets.chain
    and last_block.block_number = wallets.last_seen_block
