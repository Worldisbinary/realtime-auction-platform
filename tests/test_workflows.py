"""Unit tests for the pure rules of each workflow (no database)."""

from types import SimpleNamespace as NS

from app.workflows import (
    dutch_decide,
    dutch_price_at,
    english_decide,
    sealed_decide,
    settle_sealed,
)


def auction(**kw):
    base = dict(
        status="open",
        start_price=100,
        current_price=0,
        min_increment=5,
        floor_price=40,
        decrement=10,
        interval_seconds=10.0,
        reserve_price=0,
        snipe_window=30.0,
        extension=60.0,
        starts_at=1000.0,
        ends_at=2000.0,
        bid_count=0,
        leader=None,
    )
    base.update(kw)
    return NS(**base)


# --- English ---------------------------------------------------------------------------
def test_english_first_bid_must_meet_start_price():
    a = auction()
    assert english_decide(a, "a", 99, 1500).reason == "too_low"
    assert english_decide(a, "a", 100, 1500).accepted


def test_english_requires_min_increment_over_current():
    a = auction(current_price=120, bid_count=1, leader="a")
    assert english_decide(a, "b", 124, 1500).reason == "too_low"
    assert english_decide(a, "b", 125, 1500).updates["current_price"] == 125


def test_english_leader_cannot_outbid_self():
    a = auction(current_price=120, bid_count=1, leader="a")
    assert english_decide(a, "a", 500, 1500).reason == "already_leading"


def test_english_rejects_before_start_and_after_end():
    a = auction()
    assert english_decide(a, "a", 100, 999).reason == "not_started"
    assert english_decide(a, "a", 100, 2000).reason == "closed"
    assert english_decide(auction(status="closed"), "a", 100, 1500).reason == "closed"


def test_english_anti_sniping_extends_only_inside_window():
    early = english_decide(auction(), "a", 100, 1500)
    assert "ends_at" not in early.updates
    sniped = english_decide(auction(), "a", 100, 1980)  # 20s left <= 30s window
    assert sniped.updates["ends_at"] == 1980 + 60.0


# --- Dutch -----------------------------------------------------------------------------
def test_dutch_price_steps_down_and_stops_at_floor():
    a = auction()
    assert dutch_price_at(a, 1000) == 100
    assert dutch_price_at(a, 1009.9) == 100
    assert dutch_price_at(a, 1010) == 90
    assert dutch_price_at(a, 1035) == 70
    assert dutch_price_at(a, 9999) == 40


def test_dutch_first_acceptable_bid_wins_at_clock_price():
    a = auction()
    assert dutch_decide(a, "a", 79, 1025).reason == "too_low"  # clock is 80
    d = dutch_decide(a, "a", 95, 1025)
    assert d.accepted and d.updates["final_price"] == 80 and d.updates["status"] == "closed"
    assert dutch_decide(auction(status="closed"), "b", 95, 1025).reason == "closed"


# --- Sealed-bid second price -------------------------------------------------------------
def test_sealed_bid_hidden_rules():
    a = auction(start_price=50)
    assert sealed_decide(a, "a", 49, 1500).reason == "too_low"
    d = sealed_decide(a, "a", 50, 1500)
    assert d.accepted and set(d.updates) == {"bid_count"}  # no price leaks into columns
    assert sealed_decide(a, "a", 80, 2001).reason == "closed"


def test_vickrey_winner_pays_second_highest():
    assert settle_sealed([("a", 100), ("b", 150), ("c", 120)], 0) == ("b", 120)


def test_vickrey_single_bid_pays_reserve():
    assert settle_sealed([("a", 100)], 60) == ("a", 60)


def test_vickrey_reserve_not_met_and_no_bids():
    assert settle_sealed([("a", 50), ("b", 40)], 60) == (None, None)
    assert settle_sealed([], 0) == (None, None)


def test_vickrey_tie_goes_to_earliest_and_pays_tied_amount():
    assert settle_sealed([("a", 100), ("b", 100)], 0) == ("a", 100)


def test_vickrey_second_price_never_below_reserve():
    assert settle_sealed([("a", 100), ("b", 10)], 50) == ("a", 50)
