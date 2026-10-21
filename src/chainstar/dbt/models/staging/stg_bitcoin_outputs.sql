select
    outputs.chain as chain,
    outputs.block_number as block_number,
    outputs.transaction_index as transaction_index,
    outputs.transaction_hash as transaction_hash,
    outputs.output_index as output_index,
    outputs.address as address,
    outputs.script_type as script_type,
    outputs.value_sat as value_sat
from {{ source('chainstar_raw', 'bitcoin_outputs') }} as outputs
