-- Contracts chainstar can identify from the data it extracts. In this release that is
-- only token contracts that emitted ERC-20 Transfer events; other contracts appear
-- in dim_wallets, because telling a contract from a wallet needs extra RPC calls.
{{ config(
    unique_key='contract_id',
    indexes=chainstar.incremental_indexes('contract_id', 'last_seen_block'),
) }}

{%- set marks = chainstar.watermarks('last_seen_block') %}

with new_contracts as (
    select
        chain,
        token_address as contract_address,
        min(block_number) as first_seen_block,
        max(block_number) as last_seen_block,
        -- Cast: ClickHouse counts as an unsigned integer.
        cast(count(*) as bigint) as transfer_events
    from {{ ref('stg_erc20_transfers') }}
    where {{ chainstar.after_watermark(marks, 'evm') }}
    group by chain, token_address
)

select
    {{ chainstar.chain_key('new_contracts.chain', 'new_contracts.contract_address') }} as contract_id,
    new_contracts.chain as chain,
    new_contracts.contract_address as contract_address,
    'erc20' as contract_type,
{% if is_incremental() %}
    -- New rows are all above the watermark, so anything already here is older.
    case
        when existing.contract_id is null then new_contracts.first_seen_block
        else existing.first_seen_block
    end as first_seen_block,
    new_contracts.last_seen_block as last_seen_block,
    coalesce(existing.transfer_events, 0) + new_contracts.transfer_events as transfer_events
from new_contracts
left join {{ this }} as existing
    on existing.contract_id
        = {{ chainstar.chain_key('new_contracts.chain', 'new_contracts.contract_address') }}
{% else %}
    new_contracts.first_seen_block as first_seen_block,
    new_contracts.last_seen_block as last_seen_block,
    new_contracts.transfer_events as transfer_events
from new_contracts
{% endif %}
