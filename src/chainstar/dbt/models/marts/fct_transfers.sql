-- One row per value movement: an ERC-20 Transfer event (EVM), a transaction output
-- (Bitcoin), or a transfer instruction of a successful transaction (Solana).
-- Left joins on purpose: a transfer whose block or token is missing should fail the
-- not_null and relationship tests, not quietly disappear.
{{ config(
    unique_key='transfer_id',
    indexes=chainstar.incremental_indexes('transfer_id', 'block_number'),
) }}

{%- set marks = chainstar.watermarks('block_number') %}

select
    {{ chainstar.chain_key('transfers.chain', 'transfers.transaction_hash', 'transfers.transfer_position') }} as transfer_id,
    transfers.chain,
    transfers.block_number,
    blocks.block_timestamp,
    blocks.block_date as date_day,
    transfers.transaction_hash,
    transfers.transaction_index,
    transfers.transfer_position,
    transfers.log_index,
    {{ chainstar.chain_key('transfers.chain', 'transfers.token_address') }} as token_id,
    {{ chainstar.chain_key('transfers.chain', 'transfers.from_address') }} as from_wallet_id,
    {{ chainstar.chain_key('transfers.chain', 'transfers.to_address') }} as to_wallet_id,
    transfers.token_address,
    transfers.from_address,
    transfers.to_address,
    transfers.input_address_count,
    transfers.from_token_account,
    transfers.to_token_account,
    transfers.script_type,
    transfers.amount_raw,
    -- Convenience value in whole tokens. Double precision keeps about 15 significant
    -- digits; use amount_raw when exact values matter. NULL when decimals are unknown
    -- or outside the uint8 range the ERC-20 standard allows. Decimals come from
    -- dim_tokens, which is built first and so already holds this run's new tokens.
    case
        when tokens.decimals between 0 and 255
            then cast(transfers.amount_raw as double precision) / power(10, tokens.decimals)
    end as amount
from ({{ chainstar.int_transfers(marks) }}) as transfers
left join ({{ chainstar.int_blocks(marks) }}) as blocks
    on blocks.chain = transfers.chain
    and blocks.block_number = transfers.block_number
left join {{ ref('dim_tokens') }} as tokens
    on tokens.chain = transfers.chain
    and tokens.token_address = transfers.token_address
