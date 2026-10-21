select
    blocks.chain,
    blocks.block_number,
    blocks.block_hash,
    -- The header time set by the miner, as block explorers show it. It is not
    -- strictly increasing; median_time is.
    {{ chainstar.unix_to_utc_timestamp('blocks."timestamp"') }} as block_timestamp,
    cast({{ chainstar.unix_to_utc_timestamp('blocks."timestamp"') }} as date) as block_date,
    {{ chainstar.unix_to_utc_timestamp('blocks.median_time') }} as median_timestamp,
    blocks.transaction_count,
    blocks.size,
    blocks.weight
from {{ source('chainstar_raw', 'bitcoin_blocks') }} as blocks
