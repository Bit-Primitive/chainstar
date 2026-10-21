-- ETH is priced separately on Ethereum and Base, from each chain's own pools.
-- Where both have a high-confidence price for the same hour, they should agree
-- within 2%; this warns when they do not.
{{ config(severity='warn') }}
select ethereum.bucket_start, ethereum.usd_price as ethereum_price, base.usd_price as base_price
from {{ ref('fct_token_prices') }} as ethereum
inner join {{ ref('fct_token_prices') }} as base
    on base.chain = 'base'
    and base.token_address = 'native'
    and base.granularity = ethereum.granularity
    and base.bucket_start_unix = ethereum.bucket_start_unix
where ethereum.chain = 'ethereum'
    and ethereum.token_address = 'native'
    and ethereum.granularity = 'hour'
    and ethereum.confidence = 'high'
    and base.confidence = 'high'
    and (base.usd_price < 0.98 * ethereum.usd_price or base.usd_price > 1.02 * ethereum.usd_price)
