select
    txs.chain as chain,
    txs.block_number as block_number,
    txs.transaction_index as transaction_index,
    txs.transaction_hash as transaction_hash,
    txs.is_coinbase as is_coinbase,
    txs.input_count as input_count,
    txs.output_count as output_count,
    txs.fee_sat as node_fee_sat,
    txs.size as size,
    txs.vsize as vsize,
    txs.weight as weight
from {{ source('chainstar_raw', 'bitcoin_transactions') }} as txs
