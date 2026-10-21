select
    blocks.chain,
    blocks.slot,
    blocks.block_height,
    blocks.block_hash,
    blocks.parent_slot,
    {{ chainstar.unix_to_utc_timestamp('blocks.block_time') }} as block_timestamp,
    cast({{ chainstar.unix_to_utc_timestamp('blocks.block_time') }} as date) as block_date,
    blocks.transaction_count
from {{ source('chainstar_raw', 'solana_blocks') }} as blocks
