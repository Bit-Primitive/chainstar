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
"""

import json
from typing import Any

from chainstar.chunks import ExtractError, Rows, extract_chunks
from chainstar.config import RunSettings
from chainstar.datasets import SOLANA_BLOCKS, SOLANA_DATASETS, SOLANA_TRANSACTIONS, SOLANA_TRANSFERS
from chainstar.rpc import SOLANA_COMMITMENT, RpcClient

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
    """Turn one jsonParsed getBlock result into rows for the three Solana datasets."""
    transactions, transfers = [], []
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
    }


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
