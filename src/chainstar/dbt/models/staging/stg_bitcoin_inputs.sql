select
    inputs.chain as chain,
    inputs.block_number as block_number,
    inputs.transaction_index as transaction_index,
    inputs.transaction_hash as transaction_hash,
    inputs.input_index as input_index,
    inputs.spent_transaction_hash as spent_transaction_hash,
    inputs.spent_output_index as spent_output_index,
    inputs.address as address,
    inputs.script_type as script_type,
    inputs.value_sat as value_sat
from {{ source('chainstar_raw', 'bitcoin_inputs') }} as inputs
