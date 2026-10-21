-- Who funded each Bitcoin transaction. A transaction spends one or more earlier
-- outputs, possibly from different addresses, so it has a single sender only when
-- every input has an address and they are all the same one.
select
    chain,
    transaction_hash,
    cast(count(distinct address) as bigint) as input_address_count,
    case
        when count(distinct address) = 1 and count(*) = count(address) then min(address)
    end as sole_input_address,
    cast(sum(value_sat) as decimal(38, 0)) as input_value_sat
from {{ ref('stg_bitcoin_inputs') }}
group by chain, transaction_hash
