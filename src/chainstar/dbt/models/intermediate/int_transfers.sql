-- Value movements of every chain, one row each, in the columns all chains share.
-- transfer_position identifies the row within its transaction: the log index on
-- EVM chains, the output index on Bitcoin, the instruction position on Solana.

-- EVM: one row per ERC-20 Transfer event.
select
    chain,
    block_number,
    transaction_hash,
    transaction_index,
    cast(log_index as varchar) as transfer_position,
    log_index,
    token_address,
    from_address,
    to_address,
    cast(null as bigint) as input_address_count,
    cast(null as varchar) as from_token_account,
    cast(null as varchar) as to_token_account,
    cast(null as varchar) as script_type,
    amount_raw
from {{ ref('stg_erc20_transfers') }}

union all

-- Bitcoin: one row per output, the coinbase's included. from_address is set only
-- when the transaction has a single sender (see int_bitcoin_senders); the
-- coinbase spends nothing, so its input_address_count is 0.
select
    outputs.chain,
    outputs.block_number,
    outputs.transaction_hash,
    outputs.transaction_index,
    cast(outputs.output_index as varchar),
    cast(null as bigint),
    'native',
    senders.sole_input_address,
    outputs.address,
    coalesce(senders.input_address_count, 0),
    cast(null as varchar),
    cast(null as varchar),
    outputs.script_type,
    cast(outputs.value_sat as varchar)
from {{ ref('stg_bitcoin_outputs') }} as outputs
left join {{ ref('int_bitcoin_senders') }} as senders
    on senders.chain = outputs.chain
    and senders.transaction_hash = outputs.transaction_hash

union all

-- Solana: one row per transfer instruction of a successful transaction. A failed
-- transaction moves nothing (its fee is in fct_fees). For SPL tokens the
-- addresses are the wallets owning the token accounts.
select
    chain,
    slot,
    signature,
    transaction_index,
    instruction_position,
    cast(null as bigint),
    case when program = 'system' then 'native' else mint end,
    case when program = 'system' then source else source_owner end,
    case when program = 'system' then destination else destination_owner end,
    cast(null as bigint),
    case when program <> 'system' then source end,
    case when program <> 'system' then destination end,
    cast(null as varchar),
    amount_raw
from {{ ref('stg_solana_transfers') }}
where success
