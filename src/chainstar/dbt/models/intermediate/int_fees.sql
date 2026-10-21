-- One row per fee-paying transaction of every chain. fee_raw is the whole fee in
-- the chain's smallest unit: wei, satoshis or lamports.

-- EVM: execution fee = gas_used * effective gas price. This excludes Ethereum's
-- separate blob fee and Base's L1 data fee; cryo extracts neither.
with evm as (
    select
        txs.chain,
        txs.block_number,
        txs.transaction_hash,
        txs.transaction_index,
        txs.from_address,
        txs.to_address,
        txs.transaction_type,
        txs.success,
        txs.gas_limit,
        txs.gas_used,
        txs.effective_gas_price_wei,
        blocks.base_fee_per_gas as base_fee_per_gas_wei,
        cast(
            cast(txs.gas_used as decimal(38, 0)) * cast(txs.effective_gas_price_wei as decimal(38, 0))
            as decimal(38, 0)
        ) as execution_fee_wei
    from {{ ref('stg_transactions') }} as txs
    left join {{ ref('stg_blocks') }} as blocks
        on blocks.chain = txs.chain
        and blocks.block_number = txs.block_number
),

bitcoin_output_values as (
    select chain, transaction_hash, cast(sum(value_sat) as decimal(38, 0)) as output_value_sat
    from {{ ref('stg_bitcoin_outputs') }}
    group by chain, transaction_hash
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
    cast(null as boolean) as is_vote,
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
    txs.chain,
    txs.block_number,
    txs.transaction_hash,
    txs.transaction_index,
    senders.sole_input_address,
    cast(null as varchar),
    cast(null as bigint),
    true,
    cast(null as boolean),
    cast(null as bigint),
    cast(null as bigint),
    cast(null as decimal(20, 0)),
    cast(null as decimal(20, 0)),
    cast(null as decimal(38, 0)),
    cast(senders.input_value_sat - outputs.output_value_sat as decimal(38, 0))
from {{ ref('stg_bitcoin_transactions') }} as txs
left join {{ ref('int_bitcoin_senders') }} as senders
    on senders.chain = txs.chain
    and senders.transaction_hash = txs.transaction_hash
left join bitcoin_output_values as outputs
    on outputs.chain = txs.chain
    and outputs.transaction_hash = txs.transaction_hash
where not txs.is_coinbase

union all

-- Solana: meta.fee, paid by the first account, whether or not the transaction
-- succeeded. Vote transactions are included and flagged.
select
    chain,
    slot,
    signature,
    transaction_index,
    fee_payer,
    cast(null as varchar),
    cast(null as bigint),
    success,
    is_vote,
    cast(null as bigint),
    cast(null as bigint),
    cast(null as decimal(20, 0)),
    cast(null as decimal(20, 0)),
    cast(null as decimal(38, 0)),
    cast(fee_lamports as decimal(38, 0))
from {{ ref('stg_solana_transactions') }}
