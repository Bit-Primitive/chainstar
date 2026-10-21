-- One row per asset moved in the loaded data: ERC-20 token contracts, SPL mints,
-- and each chain's native asset (BTC, SOL) under the token address 'native'.
{{ config(
    unique_key='token_id',
    indexes=chainstar.incremental_indexes('token_id', 'last_seen_block'),
) }}

{%- set marks = chainstar.watermarks('last_seen_block') %}

with new_tokens as (
    {{ chainstar.int_tokens(marks) }}
),

merged as (
{% if is_incremental() %}
    -- A token seen before keeps its first block. ERC-20 metadata and native
    -- symbols are taken as they are now, as a full build would; an SPL mint's
    -- decimals are the largest seen and its standard the smallest, as min() and
    -- max() over all its transfers would give.
    select
        new_tokens.chain as chain,
        new_tokens.token_address as token_address,
        case
            when existing.token_standard < new_tokens.token_standard then existing.token_standard
            else new_tokens.token_standard
        end as token_standard,
        new_tokens.name as name,
        new_tokens.symbol as symbol,
        case
            when new_tokens.token_standard not in ('spl-token', 'spl-token-2022')
                then new_tokens.decimals
            when new_tokens.decimals is null or existing.decimals > new_tokens.decimals
                then existing.decimals
            else new_tokens.decimals
        end as decimals,
        new_tokens.metadata_block_number as metadata_block_number,
        case
            when existing.token_id is null then new_tokens.first_seen_block
            else existing.first_seen_block
        end as first_seen_block,
        new_tokens.last_seen_block as last_seen_block
    from new_tokens
    left join {{ this }} as existing
        on existing.token_id
            = {{ chainstar.chain_key('new_tokens.chain', 'new_tokens.token_address') }}
{% else %}
    select * from new_tokens
{% endif %}
)

select
    {{ chainstar.chain_key('merged.chain', 'merged.token_address') }} as token_id,
    merged.chain as chain,
    merged.token_address as token_address,
    merged.token_standard as token_standard,
    merged.name as name,
    merged.symbol as symbol,
    merged.decimals as decimals,
    merged.metadata_block_number as metadata_block_number,
    -- Cast: ClickHouse gives a comparison as a number.
    cast(coalesce(
        merged.name is not null or merged.symbol is not null or merged.decimals is not null,
        false
    ) as {{ chainstar.type_boolean() }}) as has_metadata,
    merged.first_seen_block as first_seen_block,
    merged.last_seen_block as last_seen_block
from merged
