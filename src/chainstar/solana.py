"""Solana extraction over the standard Solana JSON-RPC.

A range START:END counts slots. Not every slot produces a block, so each chunk
first asks getBlocks which slots in it have a finalized block, then fetches those
with getBlock. Skipped slots therefore produce no rows and no failed requests.

Transfers come from parsed instructions, top-level and inner, rather than from
pre/post token balance differences. Balance differences net out every movement
within a transaction and cannot say who paid whom; instructions keep each
transfer with its sender and recipient. Checked against real blocks, the parsed
instructions account for every token balance change except wrapped-SOL
wrapping and unwrapping (syncNative, closeAccount) and the fee that Token-2022
mints with the transfer-fee extension withhold from the recipient.

Swaps are the one place balance changes are the right tool: a swap through an
aggregator crosses several pools and programs, and what matters is what the
trader gave and got. For each successful transaction that calls a known DEX
program (venues.SOLANA_DEX_PROGRAMS), chainstar takes the first signer whose
balances changed like a swap: exactly one token went down and one went up.

- Token changes are the signer's token accounts (by owner) in pre/postTokenBalances.
- SOL and wrapped SOL count as one asset, "native": the signer's lamport change
  plus the transaction fee it paid, plus the lamport changes of the token
  accounts it owns. That nets out wrapping and unwrapping, and the rent a new
  token account takes and a closed one returns. A temporary wrapped-SOL account
  created and closed in the transaction starts and ends with no lamports, so it
  needs no special case.
- With one token down and one up, SOL is ignored (it moved only for fees, tips
  or rent). With only one token changed, the swap is against SOL when SOL moved
  the other way. Anything else (several tokens each way, an arbitrage that ends
  where it started) is not one swap and is left out.

Limitations: a SOL amount includes any tips or rent for accounts the signer does
not own (other programs' accounts) paid in the same transaction; programs not in
the list are not read; and a transaction makes at most one swap row.
"""

import json
from typing import Any

from chainstar.chunks import ExtractError, Rows, extract_chunks
from chainstar.config import RunSettings
from chainstar.datasets import (
    SOLANA_BLOCKS,
    SOLANA_DATASETS,
    SOLANA_SWAPS,
    SOLANA_TRANSACTIONS,
    SOLANA_TRANSFERS,
)
from chainstar.rpc import SOLANA_COMMITMENT, RpcClient
from chainstar.venues import SOLANA_DEX_PROGRAMS, WRAPPED_SOL

SYSTEM_PROGRAM = "11111111111111111111111111111111"
VOTE_PROGRAM = "Vote111111111111111111111111111111111111111"
# jsonParsed labels instructions of both token programs "spl-token", so the
# program is told apart by its id.
TOKEN_PROGRAMS = {
    "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA": "spl-token",
    "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb": "spl-token-2022",
}
TOKEN_TRANSFER_TYPES = {"transfer", "transferChecked"}
SYSTEM_TRANSFER_TYPES = {"transfer"}
# These create a token account and name its mint and owner. An account created and
# closed within one transaction never shows up in the token balances, so this is
# the only place its mint and owner can be read.
INITIALIZE_ACCOUNT_TYPES = {"initializeAccount", "initializeAccount2", "initializeAccount3"}

GET_BLOCK_OPTIONS = {
    "encoding": "jsonParsed",
    "transactionDetails": "full",
    # Version 1 transactions are live on mainnet. A lower value fails the whole
    # request (error -32015) for any block that contains one.
    "maxSupportedTransactionVersion": 1,
    "rewards": False,
    "commitment": SOLANA_COMMITMENT,
}


def extract(settings: RunSettings) -> dict[str, int]:
    client = RpcClient(settings.rpc_url, requests_per_second=settings.requests_per_second)
    chain = settings.chain.name

    def fetch_chunk(first: int, last: int) -> dict[str, Rows]:
        rows: dict[str, Rows] = {dataset.name: [] for dataset in SOLANA_DATASETS}
        slots = client.call("getBlocks", [first, last, {"commitment": SOLANA_COMMITMENT}])
        for slot in slots:
            block = client.call("getBlock", [slot, GET_BLOCK_OPTIONS])
            if block is None:
                raise ExtractError(f"getBlock returned no block for finalized slot {slot}")
            for name, block_rows in decode_block(slot, block, chain).items():
                rows[name].extend(block_rows)
        return rows

    return extract_chunks(
        data_dir=settings.data_dir,
        chain=chain,
        datasets=SOLANA_DATASETS,
        blocks=settings.blocks,
        chunk_size=settings.chunk_size,
        fetch_chunk=fetch_chunk,
    )


def decode_block(slot: int, block: dict[str, Any], chain: str) -> dict[str, Rows]:
    """Turn one jsonParsed getBlock result into rows for the Solana datasets."""
    transactions, transfers, swaps = [], [], []
    for tx_index, tx in enumerate(block["transactions"]):
        meta = tx["meta"]
        message = tx["transaction"]["message"]
        signature = tx["transaction"]["signatures"][0]
        success = meta["err"] is None
        # With jsonParsed, accountKeys includes accounts loaded from lookup tables,
        # so token-balance account indexes resolve against it. The first account
        # is the fee payer.
        keys = [key["pubkey"] for key in message["accountKeys"]]
        instructions = list(_instructions(message, meta))
        transactions.append(
            {
                "chain": chain,
                "slot": slot,
                "transaction_index": tx_index,
                "signature": signature,
                "fee_payer": keys[0],
                "fee_lamports": meta["fee"],
                "success": success,
                "error": None if success else json.dumps(meta["err"], sort_keys=True),
                "transaction_version": str(tx.get("version", "legacy")),
                "is_vote": any(i.get("programId") == VOTE_PROGRAM for _, i in instructions),
                "compute_units_consumed": meta.get("computeUnitsConsumed"),
            }
        )
        accounts = _token_accounts(keys, meta, instructions)
        common = {"chain": chain, "slot": slot, "transaction_index": tx_index}
        if success:
            swap = derive_swap(message, meta, instructions)
            if swap is not None:
                swaps.append({**common, "signature": signature, **swap})
        for position, instruction in instructions:
            row = _transfer(instruction, accounts)
            if row is not None:
                transfers.append(
                    {
                        **common,
                        "signature": signature,
                        "instruction_position": position,
                        "success": success,
                        **row,
                    }
                )
    block_row = {
        "chain": chain,
        "slot": slot,
        "block_height": block.get("blockHeight"),
        "block_hash": block["blockhash"],
        "parent_slot": block["parentSlot"],
        "block_time": block.get("blockTime"),
        "transaction_count": len(block["transactions"]),
    }
    return {
        SOLANA_BLOCKS.name: [block_row],
        SOLANA_TRANSACTIONS.name: transactions,
        SOLANA_TRANSFERS.name: transfers,
        SOLANA_SWAPS.name: swaps,
    }


NATIVE = "native"
SOL_DECIMALS = 9


def derive_swap(message: dict, meta: dict, instructions: list) -> dict[str, Any] | None:
    """The swap a successful transaction made, from its balance changes (see the module doc)."""
    venues = sorted(
        {
            SOLANA_DEX_PROGRAMS[program]
            for _, instruction in instructions
            if (program := instruction.get("programId")) in SOLANA_DEX_PROGRAMS
        }
    )
    if not venues:
        return None
    keys = [key["pubkey"] for key in message["accountKeys"]]
    signers = [key["pubkey"] for key in message["accountKeys"] if key.get("signer")]
    pre_lamports, post_lamports = meta.get("preBalances") or [], meta.get("postBalances") or []
    if len(pre_lamports) != len(keys) or len(post_lamports) != len(keys):
        return None
    for signer in signers:
        legs, decimals, owned = _token_changes(meta, signer)
        index = keys.index(signer)
        sol = post_lamports[index] - pre_lamports[index] + (meta["fee"] if index == 0 else 0)
        sol += sum(post_lamports[i] - pre_lamports[i] for i in owned)
        down = [mint for mint, change in legs.items() if change < 0]
        up = [mint for mint, change in legs.items() if change > 0]
        if len(down) == 1 and len(up) == 1:
            token_in, token_out = down[0], up[0]
        elif len(legs) == 1 and down and sol > 0:
            token_in, token_out = down[0], NATIVE
        elif len(legs) == 1 and up and sol < 0:
            token_in, token_out = NATIVE, up[0]
        else:
            continue
        legs[NATIVE] = sol
        decimals[NATIVE] = SOL_DECIMALS
        return {
            "trader": signer,
            "venues": "+".join(venues),
            "token_in": token_in,
            "amount_in": str(-legs[token_in]),
            "decimals_in": decimals[token_in],
            "token_out": token_out,
            "amount_out": str(legs[token_out]),
            "decimals_out": decimals[token_out],
        }
    return None


def _token_changes(meta: dict, owner: str) -> tuple[dict[str, int], dict[str, int], set[int]]:
    """Per mint, how much `owner`'s token accounts gained; their decimals; their indexes.

    Wrapped SOL is left out: its lamports are counted with SOL.
    """
    changes: dict[str, int] = {}
    decimals: dict[str, int] = {}
    owned: set[int] = set()
    for sign, balances in ((-1, meta.get("preTokenBalances")), (1, meta.get("postTokenBalances"))):
        for balance in balances or []:
            if balance.get("owner") != owner:
                continue
            owned.add(balance["accountIndex"])
            mint = balance["mint"]
            if mint == WRAPPED_SOL:
                continue
            amount = int(balance["uiTokenAmount"]["amount"])
            changes[mint] = changes.get(mint, 0) + sign * amount
            decimals[mint] = balance["uiTokenAmount"]["decimals"]
    return {mint: change for mint, change in changes.items() if change}, decimals, owned


def _instructions(message: dict, meta: dict):
    """Yield (position, instruction) for top-level and inner instructions, in order."""
    inner_by_parent: dict[int, list] = {}
    for group in meta.get("innerInstructions") or []:
        inner_by_parent[group["index"]] = group["instructions"]
    for index, instruction in enumerate(message["instructions"]):
        yield str(index), instruction
        for inner_index, inner in enumerate(inner_by_parent.get(index, [])):
            yield f"{index}.{inner_index}", inner


def _token_accounts(keys: list[str], meta: dict, instructions: list) -> dict[str, dict]:
    """Mint, owner and decimals for each token account the transaction touches."""
    accounts: dict[str, dict] = {}
    for _, instruction in instructions:
        parsed = _parsed(instruction)
        if (
            instruction.get("programId") in TOKEN_PROGRAMS
            and parsed
            and parsed["type"] in INITIALIZE_ACCOUNT_TYPES
        ):
            info = parsed["info"]
            accounts[info["account"]] = {"mint": info["mint"], "owner": info.get("owner")}
    # Balances are authoritative where present; post before pre, so the owner is
    # the one in place after the transaction.
    for balance in (meta.get("preTokenBalances") or []) + (meta.get("postTokenBalances") or []):
        accounts[keys[balance["accountIndex"]]] = {
            "mint": balance["mint"],
            "owner": balance.get("owner"),
            "decimals": balance["uiTokenAmount"]["decimals"],
        }
    return accounts


def _transfer(instruction: dict, accounts: dict[str, dict]) -> dict[str, Any] | None:
    parsed = _parsed(instruction)
    if parsed is None:
        return None
    program_id = instruction.get("programId")
    info = parsed["info"]
    if program_id == SYSTEM_PROGRAM and parsed["type"] in SYSTEM_TRANSFER_TYPES:
        return {
            "program": "system",
            "instruction_type": parsed["type"],
            "mint": None,
            "source": info["source"],
            "destination": info["destination"],
            "source_owner": None,
            "destination_owner": None,
            "amount": str(info["lamports"]),
            "decimals": 9,
        }
    if program_id in TOKEN_PROGRAMS and parsed["type"] in TOKEN_TRANSFER_TYPES:
        source = accounts.get(info["source"], {})
        destination = accounts.get(info["destination"], {})
        # transferChecked carries mint and decimals; plain transfer carries neither.
        token_amount = info.get("tokenAmount")
        mint = info.get("mint") or source.get("mint") or destination.get("mint")
        decimals = (
            token_amount["decimals"]
            if token_amount
            else source.get("decimals", destination.get("decimals"))
        )
        return {
            "program": TOKEN_PROGRAMS[program_id],
            "instruction_type": parsed["type"],
            "mint": mint,
            "source": info["source"],
            "destination": info["destination"],
            "source_owner": source.get("owner"),
            "destination_owner": destination.get("owner"),
            "amount": token_amount["amount"] if token_amount else info["amount"],
            "decimals": decimals,
        }
    return None


def _parsed(instruction: dict) -> dict | None:
    # Unparsed instructions have no "parsed"; memo instructions parse to a string.
    parsed = instruction.get("parsed")
    return parsed if isinstance(parsed, dict) else None
