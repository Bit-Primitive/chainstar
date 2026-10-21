-- One row per transaction: the execution fee its sender paid.
-- execution_fee_wei = gas_used * effective gas price. On Ethereum this excludes the
-- separate blob fee paid by type-3 (blob) transactions; on Base it excludes the L1
-- data fee. cryo does not extract either.
with fees as (
    select
        txs.*,
        cast(
            cast(txs.gas_used as decimal(38, 0)) * cast(txs.effective_gas_price_wei as decimal(38, 0))
            as decimal(38, 0)
        ) as execution_fee_wei
    from {{ ref('stg_transactions') }} as txs
)

select
    {{ chainstar.chain_key('fees.chain', 'fees.transaction_hash') }} as fee_id,
    fees.chain,
    fees.block_number,
    blocks.block_timestamp,
    blocks.block_date as date_day,
    fees.transaction_hash,
    fees.transaction_index,
    {{ chainstar.chain_key('fees.chain', 'fees.from_address') }} as payer_wallet_id,
    fees.from_address,
    fees.to_address,
    fees.transaction_type,
    fees.success,
    fees.gas_limit,
    fees.gas_used,
    fees.effective_gas_price_wei,
    blocks.base_fee_per_gas as base_fee_per_gas_wei,
    fees.execution_fee_wei,
    chains.native_symbol,
    cast(fees.execution_fee_wei as double precision) / power(10, chains.native_decimals) as execution_fee_native
from fees
left join {{ ref('stg_blocks') }} as blocks
    on blocks.chain = fees.chain
    and blocks.block_number = fees.block_number
inner join {{ ref('chains') }} as chains
    on chains.chain = fees.chain
