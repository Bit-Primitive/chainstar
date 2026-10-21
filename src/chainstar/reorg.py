"""Finding where a chain reorganised, from the block hashes the warehouse stored.

Blocks link to their parents by hash, so if a stored block still has the hash
the chain gives it now, so do all blocks before it. Everything after the newest
such block may have changed and is rolled back (see sync.py).
"""

from collections.abc import Callable, Sequence

from chainstar.load import StoredBlock

# Stored blocks compared with the chain before giving up. Far deeper than any
# confirmation default; a mismatch that deep more likely means the warehouse holds
# another chain's data, which a person should look at.
MAX_REORG_SEARCH = 128


class ReorgTooDeep(RuntimeError):
    pass


def first_changed_block(
    stored: Sequence[StoredBlock],
    current_hash: Callable[[int], str | None],
    limit: int = MAX_REORG_SEARCH,
) -> int | None:
    """The first block to roll back: the one after the newest stored block that still matches.

    `stored` is newest first, at most `limit` blocks; `current_hash` asks the chain
    (None when it has no block there). Returns None when no stored block matches
    and all were checked: everything synced must go. Raises ReorgTooDeep when
    `limit` blocks were checked without a match.
    """
    for block in stored[:limit]:
        if current_hash(block.number) == block.hash:
            return block.number + 1
    if len(stored) >= limit:
        raise ReorgTooDeep(
            f"none of the newest {limit} stored blocks has the hash the chain gives it now "
            f"(checked down to block {stored[limit - 1].number:,}). That is deeper than any "
            "reorganisation chainstar expects; check that the RPC serves the right chain, or "
            "start again with a new --target and --data-dir"
        )
    return None
