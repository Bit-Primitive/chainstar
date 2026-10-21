select
    outputs.chain,
    outputs.block_number,
    outputs.transaction_index,
    outputs.transaction_hash,
    outputs.output_index,
    outputs.address,
    outputs.script_type,
    outputs.value_sat
from {{ source('chainstar_raw', 'bitcoin_outputs') }} as outputs
