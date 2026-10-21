select
    transfers.chain as chain,
    transfers.slot as slot,
    transfers.transaction_index as transaction_index,
    transfers.signature as signature,
    transfers.instruction_position as instruction_position,
    transfers.program as program,
    transfers.instruction_type as instruction_type,
    transfers.mint as mint,
    transfers.source as source,
    transfers.destination as destination,
    transfers.source_owner as source_owner,
    transfers.destination_owner as destination_owner,
    -- Exact base-10 integer as text: SPL amounts are u64, beyond BIGINT.
    transfers.amount as amount_raw,
    transfers.decimals as decimals,
    transfers.success as success
from {{ source('chainstar_raw', 'solana_transfers') }} as transfers
