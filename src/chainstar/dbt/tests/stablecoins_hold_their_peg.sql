-- Stablecoins are taken at 1 USD. Where at least price_medium_min_observations
-- swaps against other stablecoins say otherwise by more than 2% in an hour, this
-- warns: the peg may have broken, and every value in that stablecoin with it.
{{ config(severity='warn') }}
select price_id, observed_price, observations
from {{ ref('fct_token_prices') }}
where price_method = 'usd_stablecoin_peg'
    and observations >= {{ var('price_medium_min_observations') }}
    and (observed_price < 0.98 or observed_price > 1.02)
