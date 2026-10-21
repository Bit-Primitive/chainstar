-- chainstar computes each Bitcoin fee as input value minus output value; the node
-- also reports the fee. Any difference means inputs or outputs were extracted
-- wrongly, so this returns the rows that disagree.
select fees.fee_id, fees.fee_raw, txs.node_fee_sat
from {{ ref('fct_fees') }} as fees
inner join {{ ref('stg_bitcoin_transactions') }} as txs
    on txs.chain = fees.chain
    and txs.transaction_hash = fees.transaction_hash
where txs.node_fee_sat is not null
    and fees.fee_raw <> txs.node_fee_sat
