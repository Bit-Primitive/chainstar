-- Contracts chainstar can identify from the data it extracts. In this release that is
-- only token contracts that emitted ERC-20 Transfer events; other contracts appear
-- in dim_wallets, because telling a contract from a wallet needs extra RPC calls.
select
    {{ chainstar.chain_key('transfers.chain', 'transfers.token_address') }} as contract_id,
    transfers.chain,
    transfers.token_address as contract_address,
    'erc20' as contract_type,
    min(transfers.block_number) as first_seen_block,
    max(transfers.block_number) as last_seen_block,
    count(*) as transfer_events
from {{ ref('stg_erc20_transfers') }} as transfers
group by transfers.chain, transfers.token_address
