select
    txs.chain,
    txs.slot,
    txs.transaction_index,
    txs.signature,
    txs.fee_payer,
    txs.fee_lamports,
    txs.success,
    txs.error,
    txs.transaction_version,
    txs.is_vote,
    txs.compute_units_consumed
from {{ source('chainstar_raw', 'solana_transactions') }} as txs
