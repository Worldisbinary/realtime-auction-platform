"""Transactional bid placement with optimistic locking and bounded retry."""
import random
import threading
import time
from dataclasses import dataclass, field

from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError, OperationalError

from .db import Auction, Bid
from .workflows import DECIDERS, settle_sealed

STATS = {"conflict_retries": 0, "accepted": 0, "rejected": 0}
_lock = threading.Lock()


def _count(key: str) -> None:
    with _lock:
        STATS[key] += 1


class NotFound(Exception):
    pass


class _Conflict(Exception):
    pass


@dataclass
class BidResult:
    accepted: bool
    reason: str
    state: dict = field(default_factory=dict)
    idempotent: bool = False


def public_state(a: Auction) -> dict:
    hidden = a.kind == "sealed" and a.status == "open"
    return {
        "id": a.id, "kind": a.kind, "title": a.title, "status": a.status, "version": a.version,
        "bid_count": a.bid_count, "current_price": None if hidden else a.current_price,
        "leader": None if hidden else a.leader, "start_price": a.start_price,
        "floor_price": a.floor_price, "decrement": a.decrement,
        "interval_seconds": a.interval_seconds, "starts_at": a.starts_at, "ends_at": a.ends_at,
        "winner": a.winner, "final_price": a.final_price,
    }


def create_auction(factory, kind: str, title: str, start_price: int, duration: float = 3600,
                   now: float | None = None, **opts) -> dict:
    now = time.time() if now is None else now
    with factory.begin() as s:
        a = Auction(kind=kind, title=title, start_price=start_price, starts_at=now,
                    ends_at=now + duration, current_price=0 if kind != "dutch" else start_price,
                    **opts)
        s.add(a)
        s.flush()
        return public_state(a)


def get_state(factory, auction_id: int) -> dict:
    with factory() as s:
        a = s.get(Auction, auction_id)
        if a is None:
            raise NotFound(auction_id)
        return public_state(a)


def list_auctions(factory) -> list[dict]:
    with factory() as s:
        return [public_state(a) for a in s.scalars(select(Auction).order_by(Auction.id))]


def bid_history(factory, auction_id: int) -> list[dict]:
    with factory() as s:
        rows = s.scalars(select(Bid).where(Bid.auction_id == auction_id).order_by(Bid.version))
        return [{"bidder": b.bidder, "amount": b.amount, "version": b.version} for b in rows]


def _transact(factory, auction_id: int, work, max_attempts: int = 80):
    """Run work(session, auction) in a transaction; retry on version conflicts / lock errors."""
    for attempt in range(max_attempts):
        try:
            with factory.begin() as s:
                a = s.get(Auction, auction_id)
                if a is None:
                    raise NotFound(auction_id)
                return work(s, a)
        except (_Conflict, IntegrityError, OperationalError):
            if attempt == max_attempts - 1:
                raise
            _count("conflict_retries")
            time.sleep(random.uniform(0, 0.005 * min(attempt + 1, 10)))


def _apply(s, a: Auction, values: dict) -> None:
    """The optimistic lock: succeeds only if nobody changed the row since we read it."""
    res = s.execute(
        update(Auction).where(Auction.id == a.id, Auction.version == a.version)
        .values(version=a.version + 1, **values).execution_options(synchronize_session=False)
    )
    if res.rowcount != 1:
        raise _Conflict
    s.refresh(a)


def place_bid(factory, auction_id: int, bidder: str, amount: int, key: str | None = None,
              now: float | None = None) -> BidResult:
    def work(s, a):
        t = time.time() if now is None else now
        if key:
            prev = s.scalar(select(Bid.id).where(Bid.auction_id == a.id, Bid.idem_key == key))
            if prev:
                return BidResult(True, "duplicate", public_state(a), idempotent=True)
        d = DECIDERS[a.kind](a, bidder, amount, t)
        if d.accepted and a.kind == "sealed" and s.scalar(
                select(Bid.id).where(Bid.auction_id == a.id, Bid.bidder == bidder)):
            d.accepted, d.reason = False, "duplicate_bidder"
        if not d.accepted:
            _count("rejected")
            return BidResult(False, d.reason, public_state(a))
        _apply(s, a, d.updates)
        s.add(Bid(auction_id=a.id, bidder=bidder, amount=amount, version=a.version,
                  idem_key=key, created_at=t))
        s.flush()
        _count("accepted")
        return BidResult(True, "ok", public_state(a))

    return _transact(factory, auction_id, work)


def close_auction(factory, auction_id: int, force: bool = False, now: float | None = None) -> dict:
    def work(s, a):
        t = time.time() if now is None else now
        if a.status == "closed":
            return public_state(a)
        if not force and t < a.ends_at:
            raise ValueError("auction has not ended")
        if a.kind == "sealed":
            rows = s.execute(select(Bid.bidder, Bid.amount).where(Bid.auction_id == a.id)
                             .order_by(Bid.version)).all()
            winner, price = settle_sealed([(r[0], r[1]) for r in rows], a.reserve_price)
            vals = {"winner": winner, "final_price": price, "current_price": price or 0,
                    "leader": winner}
        elif a.kind == "english":
            ok = a.bid_count > 0 and a.current_price >= a.reserve_price
            vals = {"winner": a.leader if ok else None,
                    "final_price": a.current_price if ok else None}
        else:
            vals = {}
        _apply(s, a, {"status": "closed", **vals})
        return public_state(a)

    return _transact(factory, auction_id, work)
