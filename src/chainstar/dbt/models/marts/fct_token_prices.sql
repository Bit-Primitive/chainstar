-- USD prices per asset and hour (and day), derived from on-chain swaps only. No
-- price API is asked; nothing is assumed except that the USD stablecoins in the
-- price_assets seed are worth 1 USD.
--
-- Per chain and time bucket (UTC hour, UTC day):
--   1. Stablecoins (price_assets, role usd_stablecoin): 1 USD ("usd_stablecoin_peg").
--      Their swaps against other stablecoins give observed_price, which a test
--      checks stays near 1.
--   2. Every asset swapped against a stablecoin gets one observation per swap:
--      the stablecoin amount (USD) divided by the asset amount. The native asset
--      (ETH, SOL) is priced so, from its wrapped token's swaps (WETH, wrapped SOL).
--   3. Assets swapped against the native asset get observations from the
--      native asset's price in the same bucket (when its confidence is high or
--      medium). Nothing is priced through a third asset: a token traded only
--      against other tokens has no price.
--   4. Observations under price_min_swap_usd are dropped; those more than
--      price_outlier_band from the bucket's lower median are rejected; the price
--      is the lower median of the rest (see macros/prices.sql).
--   5. Confidence: high with at least 5 observations, 10,000 USD of volume and 2
--      sources (pools, or Solana traders); medium with 3 and 1,000 USD; low
--      otherwise, when usd_price is NULL and observed_price shows the median.
--   6. Bitcoin has no DEX. Its price is that of wrapped BTC (WBTC, cbBTC) on
--      Ethereum and Base in the same bucket ("cross_chain_wrapped_btc"), at most
--      medium confidence, and only when those chains' swaps are in the warehouse.
--
-- Pools count only when verified (dex_pools: their factory is a known venue and
-- confirms them). Every chain gets a row per bucket in which it has blocks for
-- its native asset and its stablecoins, priced or not; other assets get rows
-- only for buckets with observations.
{{ config(
    unique_key='price_id',
    indexes=chainstar.incremental_indexes('price_id', 'last_block_number'),
) }}

{%- set window = chainstar.price_window() %}
{%- set marks = window.marks if window else none %}

with blocks as (
    select chain, block_number, block_time_unix
    from ({{ chainstar.int_blocks(marks) }}) as all_blocks
    where block_time_unix is not null
),

granularities as (
    select 'hour' as granularity, 3600 as seconds
    union all
    select 'day', 86400
),

bucket_blocks as (
    select
        blocks.chain as chain,
        granularities.granularity as granularity,
        {{ chainstar.bucket_unix('blocks.block_time_unix', 'granularities.seconds') }} as bucket_unix,
        max(blocks.block_number) as last_block_number
    from blocks
    cross join granularities
    group by
        blocks.chain,
        granularities.granularity,
        {{ chainstar.bucket_unix('blocks.block_time_unix', 'granularities.seconds') }}
),

assets as (
    select chain, token_address, role
    from {{ ref('price_assets') }}
),

swaps as (
    select
        swaps.chain as chain,
        blocks.block_time_unix as block_time_unix,
        coalesce(swaps.pool_address, swaps.trader) as source,
        {{ chainstar.priced_asset('swaps.token_in', 'assets_in') }} as asset_in,
        case
            when swaps.token_in = 'native' or assets_in.role = 'wrapped_native' then 'native'
            else assets_in.role
        end as role_in,
        {{ chainstar.adjusted_amount('swaps.amount_in_raw', 'swaps.decimals_in') }} as amount_in,
        {{ chainstar.priced_asset('swaps.token_out', 'assets_out') }} as asset_out,
        case
            when swaps.token_out = 'native' or assets_out.role = 'wrapped_native' then 'native'
            else assets_out.role
        end as role_out,
        {{ chainstar.adjusted_amount('swaps.amount_out_raw', 'swaps.decimals_out') }} as amount_out
    from ({{ chainstar.int_swaps(marks) }}) as swaps
    inner join blocks
        on blocks.chain = swaps.chain
        and blocks.block_number = swaps.block_number
    left join assets as assets_in
        on assets_in.chain = swaps.chain
        and assets_in.token_address = swaps.token_in
    left join assets as assets_out
        on assets_out.chain = swaps.chain
        and assets_out.token_address = swaps.token_out
    -- EVM: verified pools only. Solana swaps have no pool to verify.
    where swaps.chain = 'solana' or swaps.pool_verified
),

-- Each swap prices each of its two sides by the other.
legs as (
    select
        chain, block_time_unix, source,
        asset_in as subject, role_in as subject_role, amount_in as subject_amount,
        asset_out as counter, role_out as counter_role, amount_out as counter_amount
    from swaps
    union all
    select
        chain, block_time_unix, source,
        asset_out, role_out, amount_out,
        asset_in, role_in, amount_in
    from swaps
),

bucketed_legs as (
    select
        legs.chain as chain,
        granularities.granularity as granularity,
        {{ chainstar.bucket_unix('legs.block_time_unix', 'granularities.seconds') }} as bucket_unix,
        legs.source as source,
        legs.subject as subject,
        legs.subject_role as subject_role,
        legs.subject_amount as subject_amount,
        legs.counter as counter,
        legs.counter_role as counter_role,
        legs.counter_amount as counter_amount
    from legs
    cross join granularities
    where legs.subject <> legs.counter
        and legs.subject_amount > 0
        and legs.counter_amount > 0
),

against_stablecoins as (
    select
        chain, granularity, bucket_unix,
        subject as asset,
        counter_amount as usd,
        counter_amount / subject_amount as price,
        source,
        'stablecoin' as tag
    from bucketed_legs
    where counter_role = 'usd_stablecoin'
),

{{ chainstar.price_aggregate_ctes('direct', 'against_stablecoins') }},

native_prices as (
    select chain, granularity, bucket_unix, median_price
    from direct
    where asset = 'native'
        and {{ chainstar.price_confidence('direct') }} in ('high', 'medium')
),

against_native as (
    select
        legs.chain as chain,
        legs.granularity as granularity,
        legs.bucket_unix as bucket_unix,
        legs.subject as asset,
        legs.counter_amount * native_prices.median_price as usd,
        legs.counter_amount * native_prices.median_price / legs.subject_amount as price,
        legs.source as source,
        'native' as tag
    from bucketed_legs as legs
    inner join native_prices
        on native_prices.chain = legs.chain
        and native_prices.granularity = legs.granularity
        and native_prices.bucket_unix = legs.bucket_unix
    where legs.counter = 'native'
        and (legs.subject_role is null or legs.subject_role = 'wrapped_btc')
),

observations as (
    select chain, granularity, bucket_unix, asset, usd, price, source, tag
    from against_stablecoins
    union all
    select chain, granularity, bucket_unix, asset, usd, price, source, tag
    from against_native
),

{{ chainstar.price_aggregate_ctes('combined', 'observations') }},

-- Bitcoin: the observations of wrapped BTC on the EVM chains, as Bitcoin's own.
wrapped_btc_observations as (
    select
        'bitcoin' as chain,
        observations.granularity as granularity,
        observations.bucket_unix as bucket_unix,
        'native' as asset,
        observations.usd as usd,
        observations.price as price,
        observations.chain || ':' || observations.source as source,
        'cross_chain_wrapped_btc' as tag
    from observations
    inner join assets
        on assets.chain = observations.chain
        and assets.token_address = observations.asset
        and assets.role = 'wrapped_btc'
),

{{ chainstar.price_aggregate_ctes('bitcoin_wrapped', 'wrapped_btc_observations') }},

-- One aggregate per chain, asset and bucket: every chain's own, and Bitcoin's.
aggregates as (
    select
        chain, asset, granularity, bucket_unix, median_price, observations, rejected,
        volume_usd, sources, price_low, price_high, tag_low, tag_high,
        {{ chainstar.price_confidence('combined') }} as confidence
    from combined
    union all
    -- At most medium: wrapped BTC is not BTC, and trades on another chain.
    select
        chain, asset, granularity, bucket_unix, median_price, observations, rejected,
        volume_usd, sources, price_low, price_high, tag_low, tag_high,
        case
            when {{ chainstar.price_confidence('bitcoin_wrapped') }} = 'high' then 'medium'
            else {{ chainstar.price_confidence('bitcoin_wrapped') }}
        end
    from bitcoin_wrapped
),

-- The rows every bucket with blocks gets, priced or not: each chain's native
-- asset, and its USD stablecoins.
skeleton as (
    select chain, 'native' as asset, granularity, bucket_unix, last_block_number,
        'native' as kind
    from bucket_blocks
    union all
    select buckets.chain, assets.token_address, buckets.granularity, buckets.bucket_unix,
        buckets.last_block_number, 'peg'
    from bucket_blocks as buckets
    inner join assets
        on assets.chain = buckets.chain
        and assets.role = 'usd_stablecoin'
),

priced as (
    select
        coalesce(skeleton.chain, aggregates.chain) as chain,
        coalesce(skeleton.asset, aggregates.asset) as asset,
        coalesce(skeleton.granularity, aggregates.granularity) as granularity,
        coalesce(skeleton.bucket_unix, aggregates.bucket_unix) as bucket_unix,
        coalesce(skeleton.last_block_number, buckets.last_block_number) as last_block_number,
        aggregates.median_price as median_price,
        aggregates.observations as observations,
        aggregates.rejected as rejected,
        aggregates.volume_usd as volume_usd,
        aggregates.sources as sources,
        aggregates.price_low as price_low,
        aggregates.price_high as price_high,
        aggregates.tag_low as tag_low,
        aggregates.tag_high as tag_high,
        case when skeleton.kind = 'peg' then 'peg' else aggregates.confidence end as confidence
    from skeleton
    full outer join aggregates
        on aggregates.chain = skeleton.chain
        and aggregates.asset = skeleton.asset
        and aggregates.granularity = skeleton.granularity
        and aggregates.bucket_unix = skeleton.bucket_unix
    left join bucket_blocks as buckets
        on buckets.chain = aggregates.chain
        and buckets.granularity = aggregates.granularity
        and buckets.bucket_unix = aggregates.bucket_unix
    left join assets
        on assets.chain = aggregates.chain
        and assets.token_address = aggregates.asset
    -- Other assets only where they had observations and the chain had blocks;
    -- the native asset and stablecoins only through the skeleton.
    where skeleton.chain is not null
        or (
            buckets.chain is not null
            and aggregates.asset <> 'native'
            and (assets.role is null or assets.role = 'wrapped_btc')
        )
)

select
    {{ chainstar.chain_key('priced.chain', 'priced.asset', 'priced.granularity', 'priced.bucket_unix') }} as price_id,
    priced.chain as chain,
    priced.asset as token_address,
    {{ chainstar.chain_key('priced.chain', 'priced.asset') }} as token_id,
    priced.granularity as granularity,
    {{ chainstar.unix_to_utc_timestamp('priced.bucket_unix') }} as bucket_start,
    cast({{ chainstar.unix_to_utc_timestamp('priced.bucket_unix') }} as date) as date_day,
    priced.bucket_unix as bucket_start_unix,
    -- The price chainstar values transfers, fees and swaps at: NULL when there
    -- is no price or only a low-confidence one.
    cast(case
        when priced.confidence = 'peg' then 1.0
        when priced.confidence in ('high', 'medium') then priced.median_price
    end as {{ chainstar.type_double() }}) as usd_price,
    priced.median_price as observed_price,
    priced.price_low as price_low,
    priced.price_high as price_high,
    case
        when priced.confidence = 'peg' then 'usd_stablecoin_peg'
        when priced.tag_low is null then null
        when priced.tag_low = 'cross_chain_wrapped_btc' then 'cross_chain_wrapped_btc'
        when priced.tag_low <> priced.tag_high then 'swaps_vs_stablecoins_and_native'
        when priced.tag_low = 'stablecoin' then 'swaps_vs_stablecoins'
        else 'swaps_vs_native'
    end as price_method,
    coalesce(priced.confidence, 'none') as confidence,
    cast(coalesce(priced.observations, 0) as bigint) as observations,
    cast(coalesce(priced.rejected, 0) as bigint) as rejected_observations,
    priced.volume_usd as volume_usd,
    cast(coalesce(priced.sources, 0) as bigint) as sources,
    priced.last_block_number as last_block_number
from priced
{% if window is not none %}
where priced.bucket_unix >= {{ window.start if window.start is not none else chainstar.no_blocks() }}
{% endif %}
