select
    txs.chain,
    txs.block_number,
    txs.transaction_index,
    txs.transaction_hash,
    txs.is_coinbase,
    txs.input_count,
    txs.output_count,
    txs.fee_sat as node_fee_sat,
    txs.size,
    txs.vsize,
    txs.weight
from {{ source('chainstar_raw', 'bitcoin_transactions') }} as txs
