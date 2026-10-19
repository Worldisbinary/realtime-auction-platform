"""100 simultaneous bids against one auction: optimistic locking must serialise them."""

from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

from app import service


def fire(factory, auction_id, amounts):
    barrier = Barrier(len(amounts))

    def go(i):
        barrier.wait()
        return service.place_bid(factory, auction_id, f"bidder-{i}", amounts[i])

    with ThreadPoolExecutor(max_workers=len(amounts)) as ex:
        return list(ex.map(go, range(len(amounts))))


def test_100_concurrent_bids_one_winner_per_level_no_lost_updates(factory):
    a = service.create_auction(factory, "english", "lot", start_price=10, min_increment=1)
    amounts = [10 + (i % 20) for i in range(100)]  # 20 price levels, 5 bidders contending each
    results = fire(factory, a["id"], amounts)

    accepted = [r for r in results if r.accepted]
    assert len(accepted) + sum(not r.accepted for r in results) == 100
    final = service.get_state(factory, a["id"])
    history = service.bid_history(factory, a["id"])

    # no lost updates: every accepted bid is persisted and bumped the version exactly once
    assert len(history) == len(accepted) == final["bid_count"] == final["version"]
    assert [h["version"] for h in history] == list(range(1, len(history) + 1))
    # monotonic price history, so at most one accepted bid per price level
    prices = [h["amount"] for h in history]
    assert all(b > a_ for a_, b in zip(prices, prices[1:], strict=False))
    assert max(Counter(prices).values()) == 1
    # the highest bid always wins and the stored price equals it
    assert final["current_price"] == prices[-1] == max(amounts)
    assert final["leader"] == history[-1]["bidder"]


def test_100_identical_bids_exactly_one_wins(factory):
    a = service.create_auction(factory, "english", "lot", start_price=10)
    results = fire(factory, a["id"], [50] * 100)
    assert sum(r.accepted for r in results) == 1
    assert service.get_state(factory, a["id"])["version"] == 1


def test_100_concurrent_dutch_buyers_exactly_one_wins(factory):
    a = service.create_auction(
        factory, "dutch", "lot", start_price=100, floor_price=50, decrement=1, interval_seconds=1000
    )
    results = fire(factory, a["id"], [100] * 100)
    assert sum(r.accepted for r in results) == 1
    st = service.get_state(factory, a["id"])
    assert st["status"] == "closed" and st["final_price"] == 100


def test_concurrent_sealed_bids_all_recorded(factory):
    a = service.create_auction(factory, "sealed", "lot", start_price=1)
    results = fire(factory, a["id"], list(range(1, 101)))
    assert all(r.accepted for r in results)
    st = service.close_auction(factory, a["id"], force=True)
    assert st["bid_count"] == 100 and st["winner"] == "bidder-99" and st["final_price"] == 99
