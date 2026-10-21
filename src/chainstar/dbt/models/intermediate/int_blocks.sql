-- Every loaded block of every chain, with the columns all chains share.
-- On Solana, block_number is the slot.
select chain, block_number, block_hash, block_timestamp, block_date
from {{ ref('stg_blocks') }}

union all

select chain, block_number, block_hash, block_timestamp, block_date
from {{ ref('stg_bitcoin_blocks') }}

union all

select chain, slot, block_hash, block_timestamp, block_date
from {{ ref('stg_solana_blocks') }}
