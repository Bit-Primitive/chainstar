select
    swaps.chain as chain,
    swaps.slot as slot,
    swaps.transaction_index as transaction_index,
    swaps.signature as signature,
    swaps.trader as trader,
    swaps.venues as venues,
    swaps.token_in as token_in,
    swaps.amount_in as amount_in_raw,
    swaps.decimals_in as decimals_in,
    swaps.token_out as token_out,
    swaps.amount_out as amount_out_raw,
    swaps.decimals_out as decimals_out
from {{ source('chainstar_raw', 'solana_swaps') }} as swaps
