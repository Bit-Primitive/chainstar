import pytest

from chainstar.load import StoredBlock
from chainstar.reorg import ReorgTooDeep, first_changed_block


def _stored(*numbers: int) -> list[StoredBlock]:
    return [StoredBlock(n, f"old-{n}", None) for n in numbers]


def test_rolls_back_after_the_newest_match():
    chain = {100: "new-100", 99: "new-99", 98: "old-98"}
    assert first_changed_block(_stored(100, 99, 98, 97), chain.get) == 99


def test_nothing_changed_resumes_after_the_newest():
    assert first_changed_block(_stored(100, 99), lambda n: f"old-{n}") == 101


def test_sparse_blocks_are_compared_as_stored():
    # A wallet's warehouse holds only blocks with its transfers.
    chain = {5000: "new", 1200: "old-1200"}
    assert first_changed_block(_stored(5000, 1200, 30), chain.get) == 1201


def test_no_match_among_all_stored_means_everything():
    assert first_changed_block(_stored(3, 2, 1), lambda n: None) is None
    assert first_changed_block([], lambda n: None) is None


def test_gives_up_past_the_limit():
    with pytest.raises(ReorgTooDeep, match="newest 3 stored blocks"):
        first_changed_block(_stored(5, 4, 3), lambda n: "different", limit=3)
