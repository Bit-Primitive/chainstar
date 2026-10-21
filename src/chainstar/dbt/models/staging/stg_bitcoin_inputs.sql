select
    inputs.chain,
    inputs.block_number,
    inputs.transaction_index,
    inputs.transaction_hash,
    inputs.input_index,
    inputs.spent_transaction_hash,
    inputs.spent_output_index,
    inputs.address,
    inputs.script_type,
    inputs.value_sat
from {{ source('chainstar_raw', 'bitcoin_inputs') }} as inputs
