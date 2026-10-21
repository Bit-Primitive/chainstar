select
    chains.chain as chain,
    blocks.chain_id as chain_id,
    blocks.block_number as block_number,
    blocks.block_hash as block_hash,
    {{ chainstar.unix_to_utc_timestamp('blocks.' ~ chainstar.quoted('timestamp')) }} as block_timestamp,
    cast({{ chainstar.unix_to_utc_timestamp('blocks.' ~ chainstar.quoted('timestamp')) }} as date) as block_date,
    blocks.base_fee_per_gas as base_fee_per_gas,
    blocks.gas_used as gas_used
from {{ source('chainstar_raw', 'blocks') }} as blocks
inner join {{ ref('chains') }} as chains
    on chains.chain_id = blocks.chain_id
