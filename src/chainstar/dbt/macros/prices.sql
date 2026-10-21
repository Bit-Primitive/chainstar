{#
  USD prices from on-chain swaps. The method is described in models/marts/fct_token_prices.sql
  and the README; these are its building blocks.
#}

{# Start of the bucket of `seconds` (3600, 86400) holding a Unix time, in Unix seconds (UTC). #}
{% macro bucket_unix(column, seconds) -%}
    {{ return(adapter.dispatch('bucket_unix', 'chainstar')(column, seconds)) }}
{%- endmacro %}

{% macro default__bucket_unix(column, seconds) -%}
    ({{ column }} - mod({{ column }}, {{ seconds }}))
{%- endmacro %}

{% macro clickhouse__bucket_unix(column, seconds) -%}
    ({{ column }} - modulo({{ column }}, {{ seconds }}))
{%- endmacro %}


{# A floating-point value rounded to cents, as double precision. #}
{% macro round_to_cents(value) -%}
    {{ return(adapter.dispatch('round_to_cents', 'chainstar')(value)) }}
{%- endmacro %}

{% macro default__round_to_cents(value) -%}
    round({{ value }}, 2)
{%- endmacro %}

{% macro postgres__round_to_cents(value) -%}
    {#- Postgres rounds to a number of places only as numeric. -#}
    cast(round(cast({{ value }} as numeric), 2) as double precision)
{%- endmacro %}


{#
  CTEs that turn observations into one price per (chain, granularity, bucket,
  asset), ending in a CTE called `name`. `observations` has columns chain,
  granularity, bucket_unix, asset, usd (the swap's value in USD), price (USD per
  unit of the asset), source (the pool, or the Solana trader) and tag (what the
  asset was priced against).

  1. Keep observations worth at least price_min_swap_usd (dust swaps are mostly
     rounding) with a positive price.
  2. Take their lower median, and reject every observation more than
     price_outlier_band (10%) away from it.
  3. The price is the lower median of the observations kept. Also: how many were
     kept and rejected, their USD volume, distinct sources, lowest and highest.

  The lower median of n values is the k-th smallest, k = n/2 rounded up (for an
  even n, the lower of the two middle values): always one of the observed
  values, so it is the same number, bit for bit, on every warehouse and in every
  build, where an interpolated median or a mean of floating-point values could
  differ in the last digits. It is found with window functions (the k-th row is
  the one where 2k is n or n + 1), which every warehouse has, and each CTE reads
  the one before it once: ClickHouse expands a CTE wherever it is referenced.
#}
{% macro price_aggregate_ctes(name, observations) -%}
{%- set group = 'chain, granularity, bucket_unix, asset' -%}
{{ name }}_ranked as (
    select
        chain, granularity, bucket_unix, asset, usd, price, source, tag,
        row_number() over (partition by {{ group }} order by price) as price_rank,
        count(*) over (partition by {{ group }}) as total
    from {{ observations }}
    where usd >= {{ var('price_min_swap_usd') }} and price > 0
),

{{ name }}_medians as (
    select
        chain, granularity, bucket_unix, asset, usd, price, source, tag, total,
        max(case when 2 * price_rank = total or 2 * price_rank = total + 1 then price end)
            over (partition by {{ group }}) as median_price
    from {{ name }}_ranked
),

{{ name }}_kept as (
    select
        chain, granularity, bucket_unix, asset, usd, price, source, tag, total,
        row_number() over (partition by {{ group }} order by price) as price_rank,
        count(*) over (partition by {{ group }}) as kept
    from {{ name }}_medians
    where price >= (1 - {{ var('price_outlier_band') }}) * median_price
        and price <= (1 + {{ var('price_outlier_band') }}) * median_price
),

{{ name }}_kept_medians as (
    select
        chain, granularity, bucket_unix, asset, usd, price, source, tag, total, kept,
        max(case when 2 * price_rank = kept or 2 * price_rank = kept + 1 then price end)
            over (partition by {{ group }}) as median_price
    from {{ name }}_kept
),

{{ name }} as (
    select
        chain, granularity, bucket_unix, asset,
        max(median_price) as median_price,
        cast(count(*) as bigint) as observations,
        cast(max(total) - count(*) as bigint) as rejected,
        -- Rounded to cents: a sum of floating-point values can differ in its last
        -- digits between builds and warehouses; cents do not.
        {{ chainstar.round_to_cents('sum(usd)') }} as volume_usd,
        cast(count(distinct source) as bigint) as sources,
        min(price) as price_low,
        max(price) as price_high,
        min(tag) as tag_low,
        max(tag) as tag_high
    from {{ name }}_kept_medians
    group by {{ group }}
)
{%- endmacro %}


{# The confidence of an aggregate from price_aggregate_ctes, as SQL over its columns. #}
{% macro price_confidence(relation) -%}
    case
        when {{ relation }}.observations >= {{ var('price_high_min_observations') }}
            and {{ relation }}.volume_usd >= {{ var('price_high_min_volume_usd') }}
            and {{ relation }}.sources >= {{ var('price_high_min_sources') }}
            then 'high'
        when {{ relation }}.observations >= {{ var('price_medium_min_observations') }}
            and {{ relation }}.volume_usd >= {{ var('price_medium_min_volume_usd') }}
            then 'medium'
        else 'low'
    end
{%- endmacro %}


{# A mark above every block: a chain given it contributes no rows. #}
{% macro no_blocks() -%}9000000000000000000{%- endmacro %}


{#
  Incremental builds of fct_token_prices. A price bucket is computed from every
  swap in it, so new blocks change the buckets they fall in, which may already
  hold rows. Returns none on a full build; otherwise {'start': W, 'marks': ...}:
  W is the start of the UTC day of the earliest block not yet included (any
  chain), and marks keep, per chain, the blocks from the first one at or after
  W (a chain with none gets a mark above every block). Every bucket from W on is
  recomputed from all of its swaps, for every chain at once, because Bitcoin's
  price comes from Ethereum's and Base's swaps.
#}
{% macro price_window() %}
    {%- if not is_incremental() -%}
        {{ return(none) }}
    {%- endif -%}
    {%- set marks = chainstar.watermarks('last_block_number') -%}
    {%- set window = {'start': none, 'marks': {}} -%}
    {%- for chain in var('chainstar_chains') -%}
        {%- do window.marks.update({chain: chainstar.no_blocks() | int}) -%}
    {%- endfor -%}
    {%- if execute -%}
        {%- set rows = run_query(
            "select min(block_time_unix) as first_time from ("
            ~ chainstar.int_blocks(marks) ~ ") as new_blocks"
        ).rows -%}
        {%- set first_time = rows[0][0] if rows | length > 0 else none -%}
        {%- if first_time is not none -%}
            {%- set start = ((first_time | int) // 86400) * 86400 -%}
            {%- do window.update({'start': start}) -%}
            {%- for row in run_query(
                "select chain, min(block_number) as first_block from ("
                ~ chainstar.int_blocks() ~ ") as all_blocks where block_time_unix >= "
                ~ start ~ " group by chain"
            ).rows -%}
                {%- do window.marks.update({row[0]: (row[1] | int) - 1}) -%}
            {%- endfor -%}
        {%- endif -%}
    {%- endif -%}
    {{ return(window) }}
{% endmacro %}


{#
  Incremental builds of the facts valued in USD (fct_transfers, fct_fees,
  fct_swaps). They are valued at the hourly price of their block's hour, and an
  hour's price changes while blocks of that hour keep arriving. So besides new
  blocks, each rebuilds the rows of the hour its first new block falls in, from
  that hour's first block: the watermark is moved back to just before it. For
  Bitcoin, whose price comes from Ethereum's and Base's swaps, the hour is the
  earliest of the three chains' new hours. Rows rebuilt are replaced by key.
#}
{% macro valuation_marks(marks) %}
    {%- if marks is none or not execute -%}
        {{ return(marks) }}
    {%- endif -%}
    {%- set starts = {} -%}
    {%- for row in run_query(
        "select chain, min(block_time_unix) as first_time from ("
        ~ chainstar.int_blocks(marks) ~ ") as new_blocks group by chain"
    ).rows -%}
        {%- if row[1] is not none -%}
            {%- do starts.update({row[0]: ((row[1] | int) // 3600) * 3600}) -%}
        {%- endif -%}
    {%- endfor -%}
    {%- set sources = [] -%}
    {%- for chain in ('bitcoin', 'ethereum', 'base') if chain in starts -%}
        {%- do sources.append(starts[chain]) -%}
    {%- endfor -%}
    {%- if sources -%}
        {%- do starts.update({'bitcoin': sources | min}) -%}
    {%- endif -%}
    {%- if not starts -%}
        {{ return(marks) }}
    {%- endif -%}
    {%- set conditions = [] -%}
    {%- for chain, start in starts.items() -%}
        {#- An hour is far fewer than 20,000 blocks on Ethereum, Base and Solana, so
            the lookup reads only the newest blocks; Bitcoin's few are read whole. -#}
        {%- set bound = '' -%}
        {%- if chain in marks and chain != 'bitcoin' -%}
            {%- set bound = ' and block_number > ' ~ (marks[chain] - 20000) -%}
        {%- endif -%}
        {%- do conditions.append(
            "(chain = '" ~ chain ~ "' and block_time_unix >= " ~ start ~ bound ~ ")"
        ) -%}
    {%- endfor -%}
    {%- set moved = {} -%}
    {%- do moved.update(marks) -%}
    {%- for row in run_query(
        "select chain, min(block_number) as first_block from ("
        ~ chainstar.int_blocks() ~ ") as all_blocks where "
        ~ conditions | join(' or ') ~ " group by chain"
    ).rows -%}
        {%- if row[0] in marks and row[1] is not none -%}
            {%- do moved.update({row[0]: [marks[row[0]], (row[1] | int) - 1] | min}) -%}
        {%- endif -%}
    {%- endfor -%}
    {{ return(moved) }}
{% endmacro %}


{#
  The asset a token is priced as: each chain's wrapped native token (WETH,
  wrapped SOL) as the native asset, 'native'; anything else as itself. `assets`
  is a join to the price_assets seed on the token.
#}
{% macro priced_asset(token_column, assets) -%}
    case when {{ assets }}.role = 'wrapped_native' then 'native' else {{ token_column }} end
{%- endmacro %}


{#
  The key of the hourly price row for a token at a Unix time, to join
  fct_token_prices on price_id.
#}
{% macro hourly_price_id(chain_column, asset_sql, time_column) -%}
    {{ chainstar.chain_key(chain_column, asset_sql, "'hour'", chainstar.bucket_unix(time_column, 3600)) }}
{%- endmacro %}
