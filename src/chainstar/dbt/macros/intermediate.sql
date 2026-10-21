{#
  The intermediate queries: each chain's staging rows in the columns all chains
  share. Each is a macro so the int_* views (marks = none, every row) and the
  incremental marts (marks = watermarks, new rows only) run the same SQL. The
  marts inline them with their filters, because a filter applied to a finished
  view of a UNION and joins cannot reach the raw tables' indexes on Postgres.
#}

{# Every loaded block of every chain. On Solana, block_number is the slot. #}
{% macro int_blocks(marks=none) %}
select chain, block_number, block_hash, block_timestamp, block_date
from {{ ref('stg_blocks') }}
where {{ chainstar.after_watermark(marks, 'evm') }}

union all

select chain, block_number, block_hash, block_timestamp, block_date
from {{ ref('stg_bitcoin_blocks') }}
where {{ chainstar.after_watermark(marks, 'bitcoin') }}

union all

select chain, slot, block_hash, block_timestamp, block_date
from {{ ref('stg_solana_blocks') }}
where {{ chainstar.after_watermark(marks, 'solana', 'slot') }}
{% endmacro %}


{#
  Who funded each Bitcoin transaction. A transaction spends one or more earlier
  outputs, possibly from different addresses, so it has a single sender only when
  every input has an address and they are all the same one. block_number is a
  grouping column (a transaction lives in one block) so block filters reach the
  inputs table.
#}
{% macro int_bitcoin_senders(marks=none) %}
select
    chain,
    block_number,
    transaction_hash,
    cast(count(distinct address) as bigint) as input_address_count,
    case
        when count(distinct address) = 1 and count(*) = count(address) then min(address)
    end as sole_input_address,
    cast(sum(value_sat) as {{ chainstar.type_decimal(38, 0) }}) as input_value_sat
from {{ ref('stg_bitcoin_inputs') }}
where {{ chainstar.after_watermark(marks, 'bitcoin') }}
group by chain, block_number, transaction_hash
{% endmacro %}


{#
  Value movements of every chain, one row each. transfer_position identifies the
  row within its transaction: the log index on EVM chains, the output index on
  Bitcoin, the instruction position on Solana.
#}
{% macro int_transfers(marks=none) %}
-- EVM: one row per ERC-20 Transfer event.
select
    chain,
    block_number,
    transaction_hash,
    transaction_index,
    cast(log_index as {{ chainstar.type_text() }}) as transfer_position,
    log_index,
    token_address,
    from_address,
    to_address,
    cast(null as bigint) as input_address_count,
    cast(null as {{ chainstar.type_text() }}) as from_token_account,
    cast(null as {{ chainstar.type_text() }}) as to_token_account,
    cast(null as {{ chainstar.type_text() }}) as script_type,
    amount_raw
from {{ ref('stg_erc20_transfers') }}
where {{ chainstar.after_watermark(marks, 'evm') }}

union all

-- Bitcoin: one row per output, the coinbase's included. from_address is set only
-- when the transaction has a single sender (see int_bitcoin_senders); the
-- coinbase spends nothing, so its input_address_count is 0.
select
    outputs.chain as chain,
    outputs.block_number as block_number,
    outputs.transaction_hash as transaction_hash,
    outputs.transaction_index as transaction_index,
    cast(outputs.output_index as {{ chainstar.type_text() }}),
    cast(null as bigint),
    'native',
    senders.sole_input_address as sole_input_address,
    outputs.address as address,
    coalesce(senders.input_address_count, 0),
    cast(null as {{ chainstar.type_text() }}),
    cast(null as {{ chainstar.type_text() }}),
    outputs.script_type as script_type,
    cast(outputs.value_sat as {{ chainstar.type_text() }})
from {{ ref('stg_bitcoin_outputs') }} as outputs
left join ({{ chainstar.int_bitcoin_senders(marks) }}) as senders
    on senders.chain = outputs.chain
    and senders.block_number = outputs.block_number
    and senders.transaction_hash = outputs.transaction_hash
where {{ chainstar.after_watermark(marks, 'bitcoin', 'outputs.block_number') }}

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
    cast(null as {{ chainstar.type_text() }}),
    amount_raw
from {{ ref('stg_solana_transfers') }}
where success and {{ chainstar.after_watermark(marks, 'solana', 'slot') }}
{% endmacro %}


{#
  One row per fee-paying transaction of every chain. fee_raw is the whole fee in
  the chain's smallest unit: wei, satoshis or lamports.
#}
{% macro int_fees(marks=none) %}
-- EVM: execution fee = gas_used * effective gas price. This excludes Ethereum's
-- separate blob fee and Base's L1 data fee; cryo extracts neither.
with evm as (
    select
        txs.chain as chain,
        txs.block_number as block_number,
        txs.transaction_hash as transaction_hash,
        txs.transaction_index as transaction_index,
        txs.from_address as from_address,
        txs.to_address as to_address,
        txs.transaction_type as transaction_type,
        txs.success as success,
        txs.gas_limit as gas_limit,
        txs.gas_used as gas_used,
        txs.effective_gas_price_wei as effective_gas_price_wei,
        blocks.base_fee_per_gas as base_fee_per_gas_wei,
        cast(
            cast(txs.gas_used as {{ chainstar.type_decimal(38, 0) }}) * cast(txs.effective_gas_price_wei as {{ chainstar.type_decimal(38, 0) }})
            as {{ chainstar.type_decimal(38, 0) }}
        ) as execution_fee_wei
    from {{ ref('stg_transactions') }} as txs
    left join (
        select * from {{ ref('stg_blocks') }}
        where {{ chainstar.after_watermark(marks, 'evm') }}
    ) as blocks
        on blocks.chain = txs.chain
        and blocks.block_number = txs.block_number
    where {{ chainstar.after_watermark(marks, 'evm', 'txs.block_number', 'txs.chain_id') }}
),

bitcoin_output_values as (
    select
        chain,
        block_number,
        transaction_hash,
        cast(sum(value_sat) as {{ chainstar.type_decimal(38, 0) }}) as output_value_sat
    from {{ ref('stg_bitcoin_outputs') }}
    where {{ chainstar.after_watermark(marks, 'bitcoin') }}
    group by chain, block_number, transaction_hash
)

select
    chain,
    block_number,
    transaction_hash,
    transaction_index,
    from_address,
    to_address,
    transaction_type,
    success,
    cast(null as {{ chainstar.type_boolean() }}) as is_vote,
    gas_limit,
    gas_used,
    effective_gas_price_wei,
    base_fee_per_gas_wei,
    execution_fee_wei,
    execution_fee_wei as fee_raw
from evm

union all

-- Bitcoin: fee = value of the outputs spent - value of the outputs created. The
-- coinbase pays no fee and is left out. The payer is known only when a single
-- address funded the transaction.
select
    txs.chain as chain,
    txs.block_number as block_number,
    txs.transaction_hash as transaction_hash,
    txs.transaction_index as transaction_index,
    senders.sole_input_address as sole_input_address,
    cast(null as {{ chainstar.type_text() }}),
    cast(null as bigint),
    true,
    cast(null as {{ chainstar.type_boolean() }}),
    cast(null as bigint),
    cast(null as bigint),
    cast(null as {{ chainstar.type_decimal(20, 0) }}),
    cast(null as {{ chainstar.type_decimal(20, 0) }}),
    cast(null as {{ chainstar.type_decimal(38, 0) }}),
    cast(senders.input_value_sat - outputs.output_value_sat as {{ chainstar.type_decimal(38, 0) }})
from {{ ref('stg_bitcoin_transactions') }} as txs
left join ({{ chainstar.int_bitcoin_senders(marks) }}) as senders
    on senders.chain = txs.chain
    and senders.block_number = txs.block_number
    and senders.transaction_hash = txs.transaction_hash
left join bitcoin_output_values as outputs
    on outputs.chain = txs.chain
    and outputs.block_number = txs.block_number
    and outputs.transaction_hash = txs.transaction_hash
where not txs.is_coinbase
    and {{ chainstar.after_watermark(marks, 'bitcoin', 'txs.block_number') }}

union all

-- Solana: meta.fee, paid by the first account, whether or not the transaction
-- succeeded. Vote transactions are included and flagged.
select
    chain,
    slot,
    signature,
    transaction_index,
    fee_payer,
    cast(null as {{ chainstar.type_text() }}),
    cast(null as bigint),
    success,
    is_vote,
    cast(null as bigint),
    cast(null as bigint),
    cast(null as {{ chainstar.type_decimal(20, 0) }}),
    cast(null as {{ chainstar.type_decimal(20, 0) }}),
    cast(null as {{ chainstar.type_decimal(38, 0) }}),
    cast(fee_lamports as {{ chainstar.type_decimal(38, 0) }})
from {{ ref('stg_solana_transactions') }}
where {{ chainstar.after_watermark(marks, 'solana', 'slot') }}
{% endmacro %}


{#
  Every asset that appears in int_transfers, with the first and last block it
  moved in, in the columns all chains share.
#}
{% macro int_tokens(marks=none) %}
-- EVM: token contracts that emitted an ERC-20 Transfer, with metadata cryo read.
with evm_tokens as (
    select
        chain,
        token_address,
        min(block_number) as first_seen_block,
        max(block_number) as last_seen_block
    from {{ ref('stg_erc20_transfers') }}
    where {{ chainstar.after_watermark(marks, 'evm') }}
    group by chain, token_address
),

-- Solana: SPL mints moved by a successful transfer. Decimals come from the data;
-- names and symbols live in separate metadata programs chainstar does not read.
spl_tokens as (
    select
        chain,
        mint as token_address,
        min(program) as token_standard,
        max(decimals) as decimals,
        min(slot) as first_seen_block,
        max(slot) as last_seen_block
    from {{ ref('stg_solana_transfers') }}
    where success and program <> 'system'
        and {{ chainstar.after_watermark(marks, 'solana', 'slot') }}
    group by chain, mint
),

-- BTC and SOL themselves: every Bitcoin output, and successful SOL transfers.
native_assets as (
    select chain, min(block_number) as first_seen_block, max(block_number) as last_seen_block
    from {{ ref('stg_bitcoin_outputs') }}
    where {{ chainstar.after_watermark(marks, 'bitcoin') }}
    group by chain

    union all

    select chain, min(slot), max(slot)
    from {{ ref('stg_solana_transfers') }}
    where success and program = 'system'
        and {{ chainstar.after_watermark(marks, 'solana', 'slot') }}
    group by chain
)

select
    evm_tokens.chain as chain,
    evm_tokens.token_address as token_address,
    'erc20' as token_standard,
    metadata.name as name,
    metadata.symbol as symbol,
    metadata.decimals as decimals,
    metadata.metadata_block_number as metadata_block_number,
    evm_tokens.first_seen_block as first_seen_block,
    evm_tokens.last_seen_block as last_seen_block
from evm_tokens
left join {{ ref('stg_erc20_metadata') }} as metadata
    on metadata.chain = evm_tokens.chain
    and metadata.token_address = evm_tokens.token_address

union all

select
    chain,
    token_address,
    token_standard,
    cast(null as {{ chainstar.type_text() }}),
    cast(null as {{ chainstar.type_text() }}),
    decimals,
    cast(null as bigint),
    first_seen_block,
    last_seen_block
from spl_tokens

union all

-- Under the token address 'native'.
select
    native_assets.chain as chain,
    'native',
    'native',
    cast(null as {{ chainstar.type_text() }}),
    chains.native_symbol as native_symbol,
    cast(chains.native_decimals as bigint),
    cast(null as bigint),
    native_assets.first_seen_block as first_seen_block,
    native_assets.last_seen_block as last_seen_block
from native_assets
inner join {{ ref('chains') }} as chains
    on chains.chain = native_assets.chain
{% endmacro %}
