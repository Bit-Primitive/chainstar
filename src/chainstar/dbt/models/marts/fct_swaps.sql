-- One row per DEX swap. Ethereum and Base: a decoded Swap event (Uniswap v2 and
-- v3, SushiSwap v2 and v3, Aerodrome and Aerodrome Slipstream pools, and any
-- other contract emitting the same events, as venue 'unknown'). Solana: what one
-- signer gave and got in a transaction calling a known DEX program, from its
-- balance changes. See macros/swaps.sql and the README for what is covered.
--
-- usd_value: the swap's value at the hourly USD price (fct_token_prices) of one
-- side, preferring a USD stablecoin, then the native asset, then token_in, then
-- token_out; NULL when neither side has a usable price, and on EVM when the pool
-- is not verified (its tokens are only what the contract claims).
{{ config(
    unique_key='swap_id',
    indexes=chainstar.incremental_indexes('swap_id', 'block_number'),
) }}

{%- set marks = chainstar.valuation_marks(chainstar.watermarks('block_number')) %}

with swaps as (
    {{ chainstar.int_swaps(marks) }}
),

valued as (
    select
        swaps.chain as chain,
        swaps.block_number as block_number,
        swaps.transaction_hash as transaction_hash,
        swaps.transaction_index as transaction_index,
        swaps.swap_position as swap_position,
        swaps.log_index as log_index,
        swaps.venue as venue,
        swaps.swap_method as swap_method,
        swaps.pool_address as pool_address,
        swaps.pool_verified as pool_verified,
        swaps.trader as trader,
        swaps.sender as sender,
        swaps.recipient as recipient,
        swaps.token_in as token_in,
        swaps.token_out as token_out,
        swaps.amount_in_raw as amount_in_raw,
        swaps.amount_out_raw as amount_out_raw,
        {{ chainstar.adjusted_amount('swaps.amount_in_raw', 'swaps.decimals_in') }} as amount_in,
        {{ chainstar.adjusted_amount('swaps.amount_out_raw', 'swaps.decimals_out') }} as amount_out,
        blocks.block_timestamp as block_timestamp,
        blocks.block_date as block_date,
        prices_in.usd_price as price_in,
        prices_out.usd_price as price_out,
        prices_in.price_method as method_in,
        prices_out.price_method as method_out,
        prices_in.confidence as confidence_in,
        prices_out.confidence as confidence_out,
        prices_in.bucket_start as bucket_in,
        prices_out.bucket_start as bucket_out,
        -- Lower is preferred: 1 a stablecoin, 2 the native asset, 3 anything else.
        case
            when assets_in.role = 'usd_stablecoin' then 1
            when swaps.token_in = 'native' or assets_in.role = 'wrapped_native' then 2
            else 3
        end as rank_in,
        case
            when assets_out.role = 'usd_stablecoin' then 1
            when swaps.token_out = 'native' or assets_out.role = 'wrapped_native' then 2
            else 3
        end as rank_out
    from swaps
    left join ({{ chainstar.int_blocks(marks) }}) as blocks
        on blocks.chain = swaps.chain
        and blocks.block_number = swaps.block_number
    left join {{ ref('price_assets') }} as assets_in
        on assets_in.chain = swaps.chain
        and assets_in.token_address = swaps.token_in
    left join {{ ref('price_assets') }} as assets_out
        on assets_out.chain = swaps.chain
        and assets_out.token_address = swaps.token_out
    left join {{ ref('fct_token_prices') }} as prices_in
        on prices_in.price_id = {{ chainstar.hourly_price_id(
            'swaps.chain', chainstar.priced_asset('swaps.token_in', 'assets_in'), 'blocks.block_time_unix'
        ) }}
        and prices_in.usd_price is not null
        and (swaps.chain = 'solana' or swaps.pool_verified)
    left join {{ ref('fct_token_prices') }} as prices_out
        on prices_out.price_id = {{ chainstar.hourly_price_id(
            'swaps.chain', chainstar.priced_asset('swaps.token_out', 'assets_out'), 'blocks.block_time_unix'
        ) }}
        and prices_out.usd_price is not null
        and (swaps.chain = 'solana' or swaps.pool_verified)
),

chosen as (
    select
        valued.*,
        case
            when price_in is not null and amount_in is not null
                and (price_out is null or amount_out is null or rank_in <= rank_out) then 'in'
            when price_out is not null and amount_out is not null then 'out'
        end as usd_value_side
    from valued
)

select
    {{ chainstar.chain_key('chosen.chain', 'chosen.transaction_hash', 'chosen.swap_position') }} as swap_id,
    chosen.chain as chain,
    chosen.block_number as block_number,
    chosen.block_timestamp as block_timestamp,
    chosen.block_date as date_day,
    chosen.transaction_hash as transaction_hash,
    chosen.transaction_index as transaction_index,
    chosen.swap_position as swap_position,
    chosen.log_index as log_index,
    chosen.venue as venue,
    chosen.swap_method as swap_method,
    case when chosen.pool_address is not null
        then {{ chainstar.chain_key('chosen.chain', 'chosen.pool_address') }}
    end as pool_id,
    chosen.pool_address as pool_address,
    chosen.pool_verified as pool_verified,
    chosen.trader as trader,
    chosen.sender as sender,
    chosen.recipient as recipient,
    {{ chainstar.chain_key('chosen.chain', 'chosen.token_in') }} as token_in_id,
    {{ chainstar.chain_key('chosen.chain', 'chosen.token_out') }} as token_out_id,
    chosen.token_in as token_in_address,
    chosen.token_out as token_out_address,
    chosen.amount_in_raw as amount_in_raw,
    chosen.amount_out_raw as amount_out_raw,
    chosen.amount_in as amount_in,
    chosen.amount_out as amount_out,
    case
        when chosen.usd_value_side = 'in' then chosen.amount_in * chosen.price_in
        when chosen.usd_value_side = 'out' then chosen.amount_out * chosen.price_out
    end as usd_value,
    chosen.usd_value_side as usd_value_side,
    case when chosen.usd_value_side = 'in' then chosen.method_in
        when chosen.usd_value_side = 'out' then chosen.method_out end as price_method,
    case when chosen.usd_value_side = 'in' then chosen.confidence_in
        when chosen.usd_value_side = 'out' then chosen.confidence_out end as price_confidence,
    case when chosen.usd_value_side = 'in' then chosen.bucket_in
        when chosen.usd_value_side = 'out' then chosen.bucket_out end as price_bucket_start
from chosen
