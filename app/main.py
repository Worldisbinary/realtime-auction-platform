"""FastAPI app: REST + WebSocket endpoints. Create with create_app() for DI in tests."""
import os
import socket
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Literal

import redis.asyncio as aioredis
from fastapi import FastAPI, Header, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from . import service
from .db import database_url, init_schema, make_engine, make_factory
from .realtime import Realtime

STATIC = Path(__file__).parent / "static"


class NewAuction(BaseModel):
    kind: Literal["english", "dutch", "sealed"]
    title: str = Field(max_length=200)
    start_price: int = Field(gt=0)
    duration: float = Field(default=3600, gt=0)
    min_increment: int = 1
    floor_price: int = 0
    decrement: int = 0
    interval_seconds: float = Field(default=1.0, gt=0)
    reserve_price: int = 0
    snipe_window: float = 0.0
    extension: float = 0.0


class BidIn(BaseModel):
    bidder: str = Field(min_length=1, max_length=64)
    amount: int = Field(gt=0)


def create_app(db_url: str | None = None, redis_factory=None) -> FastAPI:
    redis_factory = redis_factory or (lambda: aioredis.from_url(
        os.environ.get("REDIS_URL", "redis://localhost:6379/0"), decode_responses=True))

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        engine = make_engine(db_url or database_url())
        init_schema(engine)
        app.state.factory = make_factory(engine)
        app.state.rt = Realtime(redis_factory())
        await app.state.rt.start()
        yield
        await app.state.rt.stop()
        await app.state.rt.redis.aclose()
        engine.dispose()

    app = FastAPI(title="Realtime Auction Platform", lifespan=lifespan)

    async def state_of(auction_id: int) -> dict:
        rt: Realtime = app.state.rt
        cached = await rt.cache_get(auction_id)
        if cached:
            return cached
        try:
            st = await run_in_threadpool(service.get_state, app.state.factory, auction_id)
        except service.NotFound:
            raise HTTPException(404, "auction not found") from None
        await rt.cache_set(st)
        return st

    @app.get("/")
    def index():
        return FileResponse(STATIC / "index.html")

    @app.get("/health")
    def health():
        return {"status": "ok", "instance": socket.gethostname()}

    @app.get("/stats")
    def stats():
        return {**service.STATS, "instance": socket.gethostname()}

    @app.post("/auctions", status_code=201)
    async def create(body: NewAuction):
        opts = body.model_dump(exclude={"kind", "title", "start_price", "duration"})
        st = await run_in_threadpool(
            service.create_auction, app.state.factory, body.kind, body.title, body.start_price,
            body.duration, **opts)
        await app.state.rt.cache_set(st)
        return st

    @app.get("/auctions")
    async def list_all():
        return await run_in_threadpool(service.list_auctions, app.state.factory)

    @app.get("/auctions/{auction_id}")
    async def get_one(auction_id: int):
        return await state_of(auction_id)

    @app.get("/auctions/{auction_id}/bids")
    async def history(auction_id: int):
        return await run_in_threadpool(service.bid_history, app.state.factory, auction_id)

    @app.post("/auctions/{auction_id}/bids")
    async def bid(auction_id: int, body: BidIn, idempotency_key: str | None = Header(None)):
        try:
            res = await run_in_threadpool(
                service.place_bid, app.state.factory, auction_id, body.bidder, body.amount,
                idempotency_key)
        except service.NotFound:
            raise HTTPException(404, "auction not found") from None
        if res.accepted and not res.idempotent:
            await app.state.rt.publish(res.state)
        return {"accepted": res.accepted, "reason": res.reason, "state": res.state}

    @app.post("/auctions/{auction_id}/close")
    async def close(auction_id: int, force: bool = False):
        try:
            st = await run_in_threadpool(
                service.close_auction, app.state.factory, auction_id, force)
        except service.NotFound:
            raise HTTPException(404, "auction not found") from None
        except ValueError as e:
            raise HTTPException(409, str(e)) from None
        await app.state.rt.publish(st)
        return st

    @app.websocket("/ws/auctions/{auction_id}")
    async def ws(websocket: WebSocket, auction_id: int):
        rt: Realtime = app.state.rt
        await websocket.accept()
        rt.add(auction_id, websocket)  # register before the snapshot so no update is missed
        try:
            try:
                await websocket.send_json({"type": "state", **await state_of(auction_id)})
            except HTTPException:
                await websocket.send_json({"type": "error", "reason": "not_found"})
                return
            while True:
                msg = await websocket.receive_json()
                try:
                    if msg.get("type") != "bid":
                        raise ValueError("unknown message type")
                    b = BidIn(bidder=msg["bidder"], amount=msg["amount"])
                except (ValueError, KeyError) as e:
                    await websocket.send_json({"type": "error", "reason": str(e)[:200]})
                    continue
                key = msg.get("key")
                res = await run_in_threadpool(
                    service.place_bid, app.state.factory, auction_id, b.bidder, b.amount, key)
                await websocket.send_json({"type": "bid_result", "key": key,
                                           "accepted": res.accepted, "reason": res.reason})
                if res.accepted and not res.idempotent:
                    await rt.publish(res.state)
        except WebSocketDisconnect:
            pass
        finally:
            rt.remove(auction_id, websocket)

    return app


def app_factory() -> FastAPI:  # uvicorn --factory app.main:app_factory
    return create_app()
