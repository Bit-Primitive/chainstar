"""Swaps derived from Solana balance changes: real transactions, and the edge cases by hand."""

import json
from pathlib import Path

from chainstar.solana import decode_block, derive_swap

RPC_FIXTURES = Path(__file__).parent / "rpc_fixtures"
FEE_PAYER = "Trader1111111111111111111111111111111111111"
RAYDIUM_CPMM = "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C"
WSOL = "So11111111111111111111111111111111111111112"
USDC = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
BONK = "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263"


def _swaps(slot: int) -> dict[int, dict]:
    block = json.loads((RPC_FIXTURES / f"solana_getBlock_{slot}.json").read_text())["result"]
    return {
        row["transaction_index"]: row for row in decode_block(slot, block, "solana")["solana_swaps"]
    }


def test_real_swaps_match_their_transfer_instructions():
    first, second = _swaps(459564295), _swaps(459564296)
    assert sorted(first) == [2, 3] and sorted(second) == [4]
    # Raydium CPMM: the signer's 1,145,087,249 units of a 6-decimal token went in
    # (transfer 1.0) and 1,688,532,896 lamports of wrapped SOL came back (1.1) to
    # its own wrapped-SOL account; its own lamports moved only by the fee.
    assert first[2] | {"signature": ""} == {
        "chain": "solana",
        "slot": 459564295,
        "transaction_index": 2,
        "signature": "",
        "trader": "FkaLnX17cXZGyeu3kZGdHCNdFMJJzBrPPYVvd18B3MZp",
        "venues": "raydium_cpmm",
        "token_in": "Dz9mQ9NzkBcCsuGPFJ3r1bS4wgqKMHBPiVuniW8Mbonk",
        "amount_in": "1145087249",
        "decimals_in": 6,
        "token_out": "native",
        "amount_out": "1688532896",
        "decimals_out": 9,
    }
    # Orca: 100 of a 9-decimal token for 12.56821 USDC (the USDC transfer in fct_transfers).
    assert (first[3]["venues"], first[3]["token_out"], first[3]["amount_out"]) == (
        "orca_whirlpool",
        USDC,
        "12568210",
    )
    # PumpSwap: what the signer gave includes the two fees it paid beside the pool
    # (516,789,427,432 + 2 x 128,875,169); the SOL it got, 868,123,631 lamports,
    # is its lamport change (+868,118,030) plus the 5,601 fee it paid.
    assert (second[4]["amount_in"], second[4]["amount_out"]) == ("517047177770", "868123631")


def _tx(
    pre_tokens, post_tokens, pre_lamports, post_lamports, *, fee=5000, programs=(RAYDIUM_CPMM,)
):
    keys = [FEE_PAYER, "TokenAcct1", "TokenAcct2", "WsolAcct", "Pool"]
    message = {
        "accountKeys": [{"pubkey": key, "signer": key == FEE_PAYER} for key in keys],
        "instructions": [{"programId": program} for program in programs],
    }

    def balances(entries):
        return [
            {
                "accountIndex": index,
                "mint": mint,
                "owner": owner,
                "uiTokenAmount": {"amount": str(amount), "decimals": decimals},
            }
            for index, mint, owner, amount, decimals in entries
        ]

    meta = {
        "err": None,
        "fee": fee,
        "preBalances": pre_lamports,
        "postBalances": post_lamports,
        "preTokenBalances": balances(pre_tokens),
        "postTokenBalances": balances(post_tokens),
    }
    instructions = [(str(i), instruction) for i, instruction in enumerate(message["instructions"])]
    return message, meta, instructions


RENT = 2_039_280


def test_sol_for_a_token_nets_out_the_fee_and_new_account_rent():
    # Pays 1 SOL and the fee, and the rent of a new BONK account it owns; gets
    # 50,000 BONK. Its lamports drop by 1 SOL + fee + rent; the new account's
    # lamports rise by the rent, which is netted out.
    swap = derive_swap(
        *_tx(
            [],
            [(1, BONK, FEE_PAYER, 50_000, 5)],
            [10_000_000_000, 0, 0, 0, 0],
            [10_000_000_000 - 1_000_000_000 - 5000 - RENT, RENT, 0, 0, 0],
        )
    )
    assert (swap["token_in"], swap["amount_in"], swap["decimals_in"]) == ("native", "1000000000", 9)
    assert (swap["token_out"], swap["amount_out"]) == (BONK, "50000")


def test_wrapped_sol_counts_as_sol():
    # Sells 50,000 BONK; 2 SOL arrive in its own wrapped-SOL account (its lamports
    # and its token balance both rise by 2 SOL: counted once, as SOL).
    swap = derive_swap(
        *_tx(
            [(1, BONK, FEE_PAYER, 50_000, 5), (3, WSOL, FEE_PAYER, 0, 9)],
            [(1, BONK, FEE_PAYER, 0, 5), (3, WSOL, FEE_PAYER, 2_000_000_000, 9)],
            [1_000_000, RENT, 0, RENT, 0],
            [1_000_000 - 5000, RENT, 0, RENT + 2_000_000_000, 0],
        )
    )
    assert (swap["token_in"], swap["amount_in"]) == (BONK, "50000")
    assert (swap["token_out"], swap["amount_out"]) == ("native", "2000000000")


def test_two_tokens_ignore_sol_moved_for_tips():
    swap = derive_swap(
        *_tx(
            [(1, BONK, FEE_PAYER, 50_000, 5), (2, USDC, FEE_PAYER, 0, 6)],
            [(1, BONK, FEE_PAYER, 0, 5), (2, USDC, FEE_PAYER, 1_500_000, 6)],
            [1_000_000, RENT, RENT, 0, 0],
            [1_000_000 - 5000 - 100_000, RENT, RENT, 0, 0],  # a 0.0001 SOL tip
        )
    )
    assert (swap["token_in"], swap["token_out"], swap["amount_out"]) == (BONK, USDC, "1500000")


def test_what_is_not_one_swap_is_left_out():
    # Two tokens out and none in: a deposit, not a swap.
    assert (
        derive_swap(
            *_tx(
                [(1, BONK, FEE_PAYER, 50_000, 5), (2, USDC, FEE_PAYER, 9, 6)],
                [(1, BONK, FEE_PAYER, 0, 5), (2, USDC, FEE_PAYER, 0, 6)],
                [1_000_000, RENT, RENT, 0, 0],
                [1_000_000 - 5000, RENT, RENT, 0, 0],
            )
        )
        is None
    )
    # An arbitrage that ends in SOL where it started has one leg only.
    assert (
        derive_swap(*_tx([], [], [1_000_000, 0, 0, 0, 0], [1_000_000 - 5000 + 70_000, 0, 0, 0, 0]))
        is None
    )
    # A transaction that calls no known DEX program.
    assert (
        derive_swap(
            *_tx(
                [(1, BONK, FEE_PAYER, 50_000, 5)],
                [(1, BONK, FEE_PAYER, 0, 5)],
                [1_000_000, RENT, 0, 0, 0],
                [1_000_000 - 5000 + 9_000_000, RENT, 0, 0, 0],
                programs=("11111111111111111111111111111111",),
            )
        )
        is None
    )
