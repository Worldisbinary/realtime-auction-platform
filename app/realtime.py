"""Redis-backed state cache and pub/sub fan-out to the WebSockets held by this instance."""
import asyncio
import json
import logging

from redis.exceptions import RedisError, WatchError

log = logging.getLogger("realtime")


class Realtime:
    def __init__(self, redis):
        self.redis = redis
        self.sockets: dict[int, set] = {}
        self._task: asyncio.Task | None = None
        self._pubsub = None

    # --- pub/sub -----------------------------------------------------------------------
    async def start(self) -> None:
        self._pubsub = self.redis.pubsub()
        await self._pubsub.psubscribe("auction:*:events")
        self._task = asyncio.create_task(self._pump())

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
        if self._pubsub:
            await self._pubsub.aclose()

    async def _pump(self) -> None:
        async for msg in self._pubsub.listen():
            if msg["type"] != "pmessage":
                continue
            auction_id = int(msg["channel"].split(":")[1])
            await asyncio.gather(
                *(self._send(ws, msg["data"]) for ws in list(self.sockets.get(auction_id, ()))))

    async def _send(self, ws, data: str) -> None:
        try:
            await ws.send_text(data)
        except Exception:  # client went away; the endpoint's finally block removes it
            pass

    def add(self, auction_id: int, ws) -> None:
        self.sockets.setdefault(auction_id, set()).add(ws)

    def remove(self, auction_id: int, ws) -> None:
        self.sockets.get(auction_id, set()).discard(ws)

    async def publish(self, state: dict) -> None:
        """Cache first, then publish: a client that registers late reads a cache >= any
        message it missed."""
        await self.cache_set(state)
        try:
            await self.redis.publish(f"auction:{state['id']}:events",
                                     json.dumps({"type": "state", **state}))
        except RedisError:
            log.exception("publish failed")

    # --- highest-bid cache (monotonic in version) ----------------------------------------
    async def cache_set(self, state: dict) -> None:
        key = f"auction:{state['id']}:state"
        try:
            async with self.redis.pipeline() as pipe:
                for _ in range(5):
                    try:
                        await pipe.watch(key)
                        cur = await pipe.get(key)
                        if cur and json.loads(cur)["version"] >= state["version"]:
                            await pipe.unwatch()
                            return
                        pipe.multi()
                        pipe.set(key, json.dumps(state), ex=3600)
                        await pipe.execute()
                        return
                    except WatchError:
                        continue
        except RedisError:
            log.exception("cache write failed")

    async def cache_get(self, auction_id: int) -> dict | None:
        try:
            raw = await self.redis.get(f"auction:{auction_id}:state")
        except RedisError:
            return None
        return json.loads(raw) if raw else None
