select
    chains.chain as chain,
    transfers.chain_id as chain_id,
    transfers.block_number as block_number,
    transfers.transaction_index as transaction_index,
    transfers.log_index as log_index,
    transfers.transaction_hash as transaction_hash,
    transfers.erc20 as token_address,
    transfers.from_address as from_address,
    transfers.to_address as to_address,
    -- Exact base-10 integer as text: ERC-20 values are 256-bit and do not fit any
    -- numeric type both warehouses share.
    transfers.value_string as amount_raw
from {{ source('chainstar_raw', 'erc20_transfers') }} as transfers
inner join {{ ref('chains') }} as chains
    on chains.chain_id = transfers.chain_id
