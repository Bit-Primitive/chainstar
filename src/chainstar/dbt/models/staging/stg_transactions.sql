select
    chains.chain,
    txs.chain_id,
    txs.block_number,
    txs.transaction_index,
    txs.transaction_hash,
    txs.from_address,
    txs.to_address,
    txs.value_string as value_wei_raw,
    txs.transaction_type,
    txs.success,
    txs.gas_limit,
    txs.gas_used,
    -- cryo's gas_price is the price actually paid per unit of gas; checked against
    -- receipt effectiveGasPrice on Ethereum and Base for this release.
    txs.gas_price as effective_gas_price_wei,
    txs.max_fee_per_gas,
    txs.max_priority_fee_per_gas
from {{ source('chainstar_raw', 'transactions') }} as txs
inner join {{ ref('chains') }} as chains
    on chains.chain_id = txs.chain_id
