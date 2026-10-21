-- One row per fee-paying transaction: what its payer paid, in the chain's smallest
-- unit (fee_raw) and in whole native units (fee_native). See int_fees for how each
-- chain's fee is defined. The gas columns apply to EVM chains only.
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
    fees.is_vote,
    fees.gas_limit,
    fees.gas_used,
    fees.effective_gas_price_wei,
    fees.base_fee_per_gas_wei,
    fees.execution_fee_wei,
    fees.fee_raw,
    chains.native_symbol,
    cast(fees.fee_raw as double precision) / power(10, chains.native_decimals) as fee_native,
    cast(fees.execution_fee_wei as double precision) / power(10, chains.native_decimals) as execution_fee_native
from {{ ref('int_fees') }} as fees
left join {{ ref('int_blocks') }} as blocks
    on blocks.chain = fees.chain
    and blocks.block_number = fees.block_number
inner join {{ ref('chains') }} as chains
    on chains.chain = fees.chain
