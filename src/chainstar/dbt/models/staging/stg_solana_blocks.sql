select
    blocks.chain as chain,
    blocks.slot as slot,
    blocks.block_height as block_height,
    blocks.block_hash as block_hash,
    blocks.parent_slot as parent_slot,
    blocks.block_time as block_time_unix,
    {{ chainstar.unix_to_utc_timestamp('blocks.block_time') }} as block_timestamp,
    cast({{ chainstar.unix_to_utc_timestamp('blocks.block_time') }} as date) as block_date,
    blocks.transaction_count as transaction_count
from {{ source('chainstar_raw', 'solana_blocks') }} as blocks
