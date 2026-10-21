select
    chains.chain as chain,
    txs.chain_id as chain_id,
    txs.block_number as block_number,
    txs.transaction_index as transaction_index,
    txs.transaction_hash as transaction_hash,
    txs.from_address as from_address,
    txs.to_address as to_address,
    txs.value_string as value_wei_raw,
    txs.transaction_type as transaction_type,
    txs.success as success,
    txs.gas_limit as gas_limit,
    txs.gas_used as gas_used,
    -- cryo's gas_price is the price actually paid per unit of gas; checked against
    -- receipt effectiveGasPrice on Ethereum and Base for this release.
    txs.gas_price as effective_gas_price_wei,
    txs.max_fee_per_gas as max_fee_per_gas,
    txs.max_priority_fee_per_gas as max_priority_fee_per_gas
from {{ source('chainstar_raw', 'transactions') }} as txs
inner join {{ ref('chains') }} as chains
    on chains.chain_id = txs.chain_id
