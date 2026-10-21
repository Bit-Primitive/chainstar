select
    txs.chain as chain,
    txs.slot as slot,
    txs.transaction_index as transaction_index,
    txs.signature as signature,
    txs.fee_payer as fee_payer,
    txs.fee_lamports as fee_lamports,
    txs.success as success,
    txs.error as error,
    txs.transaction_version as transaction_version,
    txs.is_vote as is_vote,
    txs.compute_units_consumed as compute_units_consumed
from {{ source('chainstar_raw', 'solana_transactions') }} as txs
