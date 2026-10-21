-- One row per fee-paying transaction: what its payer paid, in the chain's smallest
-- unit (fee_raw) and in whole native units (fee_native). See int_fees for how each
-- chain's fee is defined. The gas columns apply to EVM chains only.
{{ config(
    unique_key='fee_id',
    indexes=chainstar.incremental_indexes('fee_id', 'block_number'),
) }}

{%- set marks = chainstar.watermarks('block_number') %}

select
    {{ chainstar.chain_key('fees.chain', 'fees.transaction_hash') }} as fee_id,
    fees.chain as chain,
    fees.block_number as block_number,
    blocks.block_timestamp as block_timestamp,
    blocks.block_date as date_day,
    fees.transaction_hash as transaction_hash,
    fees.transaction_index as transaction_index,
    {{ chainstar.chain_key('fees.chain', 'fees.from_address') }} as payer_wallet_id,
    fees.from_address as from_address,
    fees.to_address as to_address,
    fees.transaction_type as transaction_type,
    fees.success as success,
    fees.is_vote as is_vote,
    fees.gas_limit as gas_limit,
    fees.gas_used as gas_used,
    fees.effective_gas_price_wei as effective_gas_price_wei,
    fees.base_fee_per_gas_wei as base_fee_per_gas_wei,
    fees.execution_fee_wei as execution_fee_wei,
    fees.fee_raw as fee_raw,
    chains.native_symbol as native_symbol,
    cast(fees.fee_raw as {{ chainstar.type_double() }}) / power(10, chains.native_decimals) as fee_native,
    cast(fees.execution_fee_wei as {{ chainstar.type_double() }}) / power(10, chains.native_decimals) as execution_fee_native
from ({{ chainstar.int_fees(marks) }}) as fees
left join ({{ chainstar.int_blocks(marks) }}) as blocks
    on blocks.chain = fees.chain
    and blocks.block_number = fees.block_number
inner join {{ ref('chains') }} as chains
    on chains.chain = fees.chain
