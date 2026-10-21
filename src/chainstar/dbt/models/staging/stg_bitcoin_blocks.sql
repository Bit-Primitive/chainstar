select
    blocks.chain as chain,
    blocks.block_number as block_number,
    blocks.block_hash as block_hash,
    -- The header time set by the miner, as block explorers show it. It is not
    -- strictly increasing; median_time is.
    {{ chainstar.unix_to_utc_timestamp('blocks.' ~ chainstar.quoted('timestamp')) }} as block_timestamp,
    cast({{ chainstar.unix_to_utc_timestamp('blocks.' ~ chainstar.quoted('timestamp')) }} as date) as block_date,
    {{ chainstar.unix_to_utc_timestamp('blocks.median_time') }} as median_timestamp,
    blocks.transaction_count as transaction_count,
    blocks.size as size,
    blocks.weight as weight
from {{ source('chainstar_raw', 'bitcoin_blocks') }} as blocks
