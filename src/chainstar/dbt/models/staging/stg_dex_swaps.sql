select
    chains.chain as chain,
    swaps.chain_id as chain_id,
    swaps.block_number as block_number,
    swaps.transaction_index as transaction_index,
    swaps.log_index as log_index,
    swaps.transaction_hash as transaction_hash,
    swaps.pool as pool_address,
    swaps.event_family as event_family,
    swaps.sender as sender,
    swaps.recipient as recipient,
    swaps.zero_for_one as zero_for_one,
    -- Exact base-10 integers as text, like ERC-20 values.
    swaps.amount_in as amount_in_raw,
    swaps.amount_out as amount_out_raw
from {{ source('chainstar_raw', 'dex_swaps') }} as swaps
inner join {{ ref('chains') }} as chains
    on chains.chain_id = swaps.chain_id
