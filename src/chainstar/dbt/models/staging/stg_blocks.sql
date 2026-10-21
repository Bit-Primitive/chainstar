select
    chains.chain,
    blocks.chain_id,
    blocks.block_number,
    blocks.block_hash,
    {{ chainstar.unix_to_utc_timestamp('blocks."timestamp"') }} as block_timestamp,
    cast({{ chainstar.unix_to_utc_timestamp('blocks."timestamp"') }} as date) as block_date,
    blocks.base_fee_per_gas,
    blocks.gas_used
from {{ source('chainstar_raw', 'blocks') }} as blocks
inner join {{ ref('chains') }} as chains
    on chains.chain_id = blocks.chain_id
