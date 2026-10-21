-- Every address that sent or received a transfer, or paid for a transaction.
-- "Wallet" means any address here: some are contracts or programs, which this
-- release does not distinguish.
{{ config(
    unique_key='wallet_id',
    indexes=chainstar.incremental_indexes('wallet_id', 'last_seen_block'),
) }}

{%- set marks = chainstar.watermarks('last_seen_block') %}

with transfers as (
    {{ chainstar.int_transfers(marks) }}
),

appearances as (
    -- Bitcoin senders are counted from inputs instead (below): an output names a
    -- sender only when one address funded the whole transaction.
    select chain, from_address as address, block_number, 1 as transfers_out, 0 as transfers_in, 0 as transactions_sent
    from transfers
    where chain <> 'bitcoin'
    union all
    select chain, to_address, block_number, 0, 1, 0
    from transfers
    union all
    select chain, from_address, block_number, 0, 0, 1
    from ({{ chainstar.int_fees(marks) }}) as fees
    where chain <> 'bitcoin'
    union all
    -- Bitcoin: every input spends from an address, and each transaction an address
    -- helped fund counts once as sent.
    select chain, address, block_number, 1, 0, 0
    from {{ ref('stg_bitcoin_inputs') }}
    where {{ chainstar.after_watermark(marks, 'bitcoin') }}
    union all
    select chain, address, block_number, 0, 0, 1
    from (
        select distinct chain, address, block_number, transaction_hash
        from {{ ref('stg_bitcoin_inputs') }}
        where {{ chainstar.after_watermark(marks, 'bitcoin') }}
    ) as funders
),

new_wallets as (
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
),

blocks as (
    {{ chainstar.int_blocks(marks) }}
)

select
    {{ chainstar.chain_key('new_wallets.chain', 'new_wallets.address') }} as wallet_id,
    new_wallets.chain as chain,
    new_wallets.address as address,
    -- The EVM zero address stands for mints (as sender) and burns (as recipient).
    cast(new_wallets.address = '0x0000000000000000000000000000000000000000' as {{ chainstar.type_boolean() }})
        as is_zero_address,
{% if is_incremental() %}
    -- New rows are all above the watermark, so a wallet already here keeps its
    -- first appearance and adds the new counts to its old ones.
    case
        when existing.wallet_id is null then new_wallets.first_seen_block
        else existing.first_seen_block
    end as first_seen_block,
    case
        when existing.wallet_id is null then first_block.block_timestamp
        else existing.first_seen_at
    end as first_seen_at,
    new_wallets.last_seen_block as last_seen_block,
    last_block.block_timestamp as last_seen_at,
    cast(coalesce(existing.transfers_out, 0) + new_wallets.transfers_out as bigint) as transfers_out,
    cast(coalesce(existing.transfers_in, 0) + new_wallets.transfers_in as bigint) as transfers_in,
    cast(coalesce(existing.transactions_sent, 0) + new_wallets.transactions_sent as bigint)
        as transactions_sent
{% else %}
    new_wallets.first_seen_block as first_seen_block,
    first_block.block_timestamp as first_seen_at,
    new_wallets.last_seen_block as last_seen_block,
    last_block.block_timestamp as last_seen_at,
    new_wallets.transfers_out as transfers_out,
    new_wallets.transfers_in as transfers_in,
    new_wallets.transactions_sent as transactions_sent
{% endif %}
from new_wallets
left join blocks as first_block
    on first_block.chain = new_wallets.chain
    and first_block.block_number = new_wallets.first_seen_block
left join blocks as last_block
    on last_block.chain = new_wallets.chain
    and last_block.block_number = new_wallets.last_seen_block
{% if is_incremental() %}
left join {{ this }} as existing
    on existing.wallet_id = {{ chainstar.chain_key('new_wallets.chain', 'new_wallets.address') }}
{% endif %}
