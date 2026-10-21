-- One row per ERC-20 Transfer event.
-- Left joins on purpose: a transfer whose block or token is missing should fail the
-- not_null and relationship tests, not quietly disappear.
select
    {{ chainstar.chain_key('transfers.chain', 'transfers.transaction_hash', 'transfers.log_index') }} as transfer_id,
    transfers.chain,
    transfers.block_number,
    blocks.block_timestamp,
    blocks.block_date as date_day,
    transfers.transaction_hash,
    transfers.transaction_index,
    transfers.log_index,
    {{ chainstar.chain_key('transfers.chain', 'transfers.token_address') }} as token_id,
    {{ chainstar.chain_key('transfers.chain', 'transfers.from_address') }} as from_wallet_id,
    {{ chainstar.chain_key('transfers.chain', 'transfers.to_address') }} as to_wallet_id,
    transfers.token_address,
    transfers.from_address,
    transfers.to_address,
    transfers.amount_raw,
    -- Convenience value in whole tokens. Double precision keeps about 15 significant
    -- digits; use amount_raw when exact values matter. NULL when decimals are unknown
    -- or outside the uint8 range the ERC-20 standard allows.
    case
        when tokens.decimals between 0 and 255
            then cast(transfers.amount_raw as double precision) / power(10, tokens.decimals)
    end as amount
from {{ ref('stg_erc20_transfers') }} as transfers
left join {{ ref('stg_blocks') }} as blocks
    on blocks.chain = transfers.chain
    and blocks.block_number = transfers.block_number
left join {{ ref('stg_erc20_metadata') }} as tokens
    on tokens.chain = transfers.chain
    and tokens.token_address = transfers.token_address
