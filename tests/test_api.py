"""Service + API + WebSocket fan-out tests."""

import pytest
from fastapi.testclient import TestClient

from app import service
from app.main import create_app


def test_idempotent_bid_applied_once(factory):
    a = service.create_auction(factory, "english", "lot", start_price=10)
    r1 = service.place_bid(factory, a["id"], "x", 20, key="k1")
    r2 = service.place_bid(factory, a["id"], "x", 20, key="k1")
    assert r1.accepted and r2.accepted and r2.idempotent
    assert service.get_state(factory, a["id"])["version"] == 1


def test_anti_sniping_persists_new_end_time(factory):
    a = service.create_auction(
        factory,
        "english",
        "lot",
        start_price=10,
        duration=100,
        now=0,
        snipe_window=30,
        extension=60,
    )
    service.place_bid(factory, a["id"], "x", 20, now=90)
    assert service.get_state(factory, a["id"])["ends_at"] == 150


def test_close_english_sets_winner_and_blocks_further_bids(factory):
    a = service.create_auction(factory, "english", "lot", start_price=10)
    service.place_bid(factory, a["id"], "x", 20)
    st = service.close_auction(factory, a["id"], force=True)
    assert (st["winner"], st["final_price"]) == ("x", 20)
    assert service.place_bid(factory, a["id"], "y", 99).reason == "closed"
    with pytest.raises(ValueError):
        service.close_auction(factory, service.create_auction(factory, "english", "t", 1)["id"])


def test_sealed_state_hides_prices_until_close(factory):
    a = service.create_auction(factory, "sealed", "lot", start_price=10)
    service.place_bid(factory, a["id"], "x", 50)
    service.place_bid(factory, a["id"], "y", 70)
    assert service.place_bid(factory, a["id"], "y", 90).reason == "duplicate_bidder"
    st = service.get_state(factory, a["id"])
    assert st["current_price"] is None and st["leader"] is None and st["bid_count"] == 2
    st = service.close_auction(factory, a["id"], force=True)
    assert (st["winner"], st["final_price"]) == ("y", 50)


def drain_until(ws, kind, limit=10):
    for _ in range(limit):
        m = ws.receive_json()
        if m["type"] == kind:
            return m
    raise AssertionError(f"no {kind} message")


def test_rest_and_websocket_fanout_through_redis(db_url, redis_factory):
    with TestClient(create_app(db_url, redis_factory)) as c:
        aid = c.post("/auctions", json={"kind": "english", "title": "t", "start_price": 10}).json()[
            "id"
        ]
        with (
            c.websocket_connect(f"/ws/auctions/{aid}") as w1,
            c.websocket_connect(f"/ws/auctions/{aid}") as w2,
        ):
            assert drain_until(w1, "state")["version"] == 0
            assert drain_until(w2, "state")["version"] == 0
            w1.send_json({"type": "bid", "bidder": "ann", "amount": 15, "key": "a"})
            assert drain_until(w1, "bid_result")["accepted"] is True
            for w in (w1, w2):  # both sockets see the update via Redis pub/sub
                m = drain_until(w, "state")
                assert (m["version"], m["current_price"], m["leader"]) == (1, 15, "ann")
            r = c.post(f"/auctions/{aid}/bids", json={"bidder": "bob", "amount": 16})
            assert r.json()["accepted"] is True
            assert drain_until(w2, "state")["version"] == 2
            w2.send_json({"type": "bogus"})
            assert drain_until(w2, "error")
        assert c.get(f"/auctions/{aid}").json()["current_price"] == 16  # served from cache
        assert [b["amount"] for b in c.get(f"/auctions/{aid}/bids").json()] == [15, 16]
        assert c.get("/auctions/999").status_code == 404
        assert c.get("/health").json()["status"] == "ok"
