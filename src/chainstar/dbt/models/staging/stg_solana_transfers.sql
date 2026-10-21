select
    transfers.chain,
    transfers.slot,
    transfers.transaction_index,
    transfers.signature,
    transfers.instruction_position,
    transfers.program,
    transfers.instruction_type,
    transfers.mint,
    transfers.source,
    transfers.destination,
    transfers.source_owner,
    transfers.destination_owner,
    -- Exact base-10 integer as text: SPL amounts are u64, beyond BIGINT.
    transfers.amount as amount_raw,
    transfers.decimals,
    transfers.success
from {{ source('chainstar_raw', 'solana_transfers') }} as transfers
