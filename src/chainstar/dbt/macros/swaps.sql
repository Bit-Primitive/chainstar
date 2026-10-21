{#
  Swaps of every chain in the columns all chains share: what went into the trade
  (token_in) and what came out (token_out), from the trader's side. A macro, like
  those in intermediate.sql, so the int_swaps view and the incremental marts run
  the same SQL.

  EVM: one row per decoded swap event whose amounts show one direction, in a pool
  whose token0() and token1() could be read. pool_verified: the pool's factory
  is one of the venues in the dex_venues seed and confirms the pool (see
  swaps.py); only those are used for prices. trader is the transaction's sender
  (NULL in a wallet history, which does not read transactions).

  Solana: one row per transaction with a swap derived from its balance changes
  (see solana.py); pool, sender and recipient are not known.
#}
{% macro int_swaps(marks=none) %}
select
    swaps.chain as chain,
    swaps.block_number as block_number,
    swaps.transaction_hash as transaction_hash,
    swaps.transaction_index as transaction_index,
    cast(swaps.log_index as {{ chainstar.type_text() }}) as swap_position,
    swaps.log_index as log_index,
    coalesce(venues.venue, 'unknown') as venue,
    swaps.event_family as swap_method,
    swaps.pool_address as pool_address,
    cast(
        coalesce(pools.factory_confirms, false) and venues.venue is not null
        as {{ chainstar.type_boolean() }}
    ) as pool_verified,
    txs.from_address as trader,
    swaps.sender as sender,
    swaps.recipient as recipient,
    case when swaps.zero_for_one then pools.token0_address else pools.token1_address end as token_in,
    case when swaps.zero_for_one then pools.token1_address else pools.token0_address end as token_out,
    swaps.amount_in_raw as amount_in_raw,
    swaps.amount_out_raw as amount_out_raw,
    case when swaps.zero_for_one then metadata0.decimals else metadata1.decimals end as decimals_in,
    case when swaps.zero_for_one then metadata1.decimals else metadata0.decimals end as decimals_out
from {{ ref('stg_dex_swaps') }} as swaps
inner join {{ ref('stg_dex_pools') }} as pools
    on pools.chain = swaps.chain
    and pools.pool_address = swaps.pool_address
left join {{ ref('dex_venues') }} as venues
    on venues.chain = pools.chain
    and venues.factory_address = pools.factory_address
    and venues.event_family = pools.event_family
left join {{ ref('stg_erc20_metadata') }} as metadata0
    on metadata0.chain = pools.chain
    and metadata0.token_address = pools.token0_address
left join {{ ref('stg_erc20_metadata') }} as metadata1
    on metadata1.chain = pools.chain
    and metadata1.token_address = pools.token1_address
left join (
    select chain, block_number, transaction_hash, from_address
    from {{ ref('stg_transactions') }}
    where {{ chainstar.after_watermark(marks, 'evm') }}
) as txs
    on txs.chain = swaps.chain
    and txs.block_number = swaps.block_number
    and txs.transaction_hash = swaps.transaction_hash
where swaps.zero_for_one is not null
    and pools.token0_address is not null
    and pools.token1_address is not null
    and {{ chainstar.after_watermark(marks, 'evm', 'swaps.block_number', 'swaps.chain_id') }}

union all

select
    chain,
    slot,
    signature,
    transaction_index,
    '0',
    cast(null as bigint),
    venues,
    'balance_changes',
    cast(null as {{ chainstar.type_text() }}),
    cast(null as {{ chainstar.type_boolean() }}),
    trader,
    cast(null as {{ chainstar.type_text() }}),
    cast(null as {{ chainstar.type_text() }}),
    token_in,
    token_out,
    amount_in_raw,
    amount_out_raw,
    decimals_in,
    decimals_out
from {{ ref('stg_solana_swaps') }}
where {{ chainstar.after_watermark(marks, 'solana', 'slot') }}
{% endmacro %}


{# amount_raw / 10^decimals as double precision; NULL when the decimals are unknown. #}
{% macro adjusted_amount(raw, decimals) -%}
    case
        when {{ decimals }} between 0 and 255
            then cast({{ raw }} as {{ chainstar.type_double() }}) / power(10, {{ decimals }})
    end
{%- endmacro %}
