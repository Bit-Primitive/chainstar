-- Every address that sent or received a transfer, or paid for a transaction.
-- "Wallet" means any address here: some are contracts or programs, which this
-- release does not distinguish.
with appearances as (
    -- Bitcoin senders are counted from inputs instead (below): an output names a
    -- sender only when one address funded the whole transaction.
    select chain, from_address as address, block_number, 1 as transfers_out, 0 as transfers_in, 0 as transactions_sent
    from {{ ref('int_transfers') }}
    where chain <> 'bitcoin'
    union all
    select chain, to_address, block_number, 0, 1, 0
    from {{ ref('int_transfers') }}
    union all
    select chain, from_address, block_number, 0, 0, 1
    from {{ ref('int_fees') }}
    where chain <> 'bitcoin'
    union all
    -- Bitcoin: every input spends from an address, and each transaction an address
    -- helped fund counts once as sent.
    select chain, address, block_number, 1, 0, 0
    from {{ ref('stg_bitcoin_inputs') }}
    union all
    select chain, address, block_number, 0, 0, 1
    from (
        select distinct chain, address, block_number, transaction_hash
        from {{ ref('stg_bitcoin_inputs') }}
    ) as funders
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
    -- Bitcoin outputs without an address (OP_RETURN data, bare scripts) and
    -- Solana token accounts whose owner is unknown belong to no wallet.
    where address is not null
    group by chain, address
)

select
    {{ chainstar.chain_key('wallets.chain', 'wallets.address') }} as wallet_id,
    wallets.chain,
    wallets.address,
    -- The EVM zero address stands for mints (as sender) and burns (as recipient).
    wallets.address = '0x0000000000000000000000000000000000000000' as is_zero_address,
    wallets.first_seen_block,
    first_block.block_timestamp as first_seen_at,
    wallets.last_seen_block,
    last_block.block_timestamp as last_seen_at,
    wallets.transfers_out,
    wallets.transfers_in,
    wallets.transactions_sent
from wallets
left join {{ ref('int_blocks') }} as first_block
    on first_block.chain = wallets.chain
    and first_block.block_number = wallets.first_seen_block
left join {{ ref('int_blocks') }} as last_block
    on last_block.chain = wallets.chain
    and last_block.block_number = wallets.last_seen_block
