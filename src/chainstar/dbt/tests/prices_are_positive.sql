-- Every USD price and value is positive where present, and a price's kept
-- observations lie between its lowest and highest.
select price_id as id, 'fct_token_prices' as source
from {{ ref('fct_token_prices') }}
where usd_price <= 0
    or observed_price <= 0
    or observed_price < price_low
    or observed_price > price_high
    or volume_usd < 0
union all
select transfer_id, 'fct_transfers'
from {{ ref('fct_transfers') }}
where amount_usd < 0 or usd_price <= 0
union all
select fee_id, 'fct_fees'
from {{ ref('fct_fees') }}
where fee_usd < 0 or native_usd_price <= 0
union all
select swap_id, 'fct_swaps'
from {{ ref('fct_swaps') }}
where usd_value < 0
