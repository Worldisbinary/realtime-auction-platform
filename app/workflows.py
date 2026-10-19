"""Pure auction rules. No I/O: each decide() returns a Decision describing the column updates."""
from dataclasses import dataclass, field


@dataclass
class Decision:
    accepted: bool
    reason: str = "ok"
    updates: dict = field(default_factory=dict)


def _reject(reason: str) -> Decision:
    return Decision(False, reason)


def english_decide(a, bidder: str, amount: int, now: float) -> Decision:
    """Ascending auction with anti-sniping: a bid inside the final window extends the end."""
    if a.status != "open" or now >= a.ends_at:
        return _reject("closed")
    if now < a.starts_at:
        return _reject("not_started")
    if a.leader == bidder:
        return _reject("already_leading")
    minimum = a.start_price if a.bid_count == 0 else a.current_price + a.min_increment
    if amount < minimum:
        return _reject("too_low")
    upd = {"current_price": amount, "leader": bidder, "bid_count": a.bid_count + 1}
    if a.ends_at - now <= a.snipe_window:
        upd["ends_at"] = now + a.extension
    return Decision(True, updates=upd)


def dutch_price_at(a, now: float) -> int:
    """Descending clock price: drops by `decrement` every `interval_seconds`, never below floor."""
    steps = int(max(0.0, now - a.starts_at) // a.interval_seconds)
    return max(a.floor_price, a.start_price - steps * a.decrement)


def dutch_decide(a, bidder: str, amount: int, now: float) -> Decision:
    """First bidder willing to pay the current clock price wins at that price."""
    if a.status != "open":
        return _reject("closed")
    if now < a.starts_at:
        return _reject("not_started")
    price = dutch_price_at(a, now)
    if amount < price:
        return _reject("too_low")
    return Decision(True, updates={
        "status": "closed", "winner": bidder, "leader": bidder, "final_price": price,
        "current_price": price, "bid_count": a.bid_count + 1,
    })


def sealed_decide(a, bidder: str, amount: int, now: float) -> Decision:
    """Sealed bid: hidden until close; the price is only revealed by settle_sealed()."""
    if a.status != "open" or now >= a.ends_at:
        return _reject("closed")
    if now < a.starts_at:
        return _reject("not_started")
    if amount < a.start_price:
        return _reject("too_low")
    return Decision(True, updates={"bid_count": a.bid_count + 1})


def settle_sealed(bids: list[tuple[str, int]], reserve: int):
    """Vickrey: highest bid wins and pays max(second-highest, reserve).

    `bids` is in arrival order, so ties go to the earlier bid.
    Returns (winner, price) or (None, None).
    """
    if not bids:
        return None, None
    ranked = sorted(enumerate(bids), key=lambda t: (-t[1][1], t[0]))
    winner, top = ranked[0][1]
    if top < reserve:
        return None, None
    second = ranked[1][1][1] if len(ranked) > 1 else 0
    return winner, max(second, reserve)


DECIDERS = {"english": english_decide, "dutch": dutch_decide, "sealed": sealed_decide}
