"""Load test: many WebSocket clients bid on one English auction across one or more instances.

Reports throughput, p50/p95 bid round-trip latency, and whether every client converged to the
same final state as the database. Exit code 1 if convergence or bid accounting fails.

  python scripts/simulate_bids.py --urls http://localhost:8000,http://localhost:8001 \
      --clients 50 --bids-per-client 24 --out results.json
"""

import argparse
import asyncio
import json
import random
import statistics
import time

import httpx
import websockets

KEYS = ("version", "current_price", "leader", "bid_count", "status")


async def run_client(idx, base, auction_id, n_bids, lat, tally, finals, rng, think_ms):
    ws_url = base.replace("http", "ws", 1) + f"/ws/auctions/{auction_id}"
    latest = {"version": -1}
    waiting: dict[str, asyncio.Future] = {}

    async with websockets.connect(ws_url, max_queue=None) as ws:

        async def reader():
            nonlocal latest
            async for raw in ws:
                m = json.loads(raw)
                if m["type"] == "state" and m["version"] > latest["version"]:
                    latest = m
                elif m["type"] == "bid_result" and m["key"] in waiting:
                    waiting.pop(m["key"]).set_result(m)

        task = asyncio.create_task(reader())
        while latest["version"] < 0:
            await asyncio.sleep(0.01)
        for i in range(n_bids):
            await asyncio.sleep(rng.uniform(0, think_ms) / 1000)  # human-ish think time
            key = f"c{idx}-{i}"
            waiting[key] = asyncio.get_running_loop().create_future()
            amount = (latest["current_price"] or 0) + rng.randint(1, 20)
            t0 = time.perf_counter()
            await ws.send(
                json.dumps({"type": "bid", "bidder": f"client-{idx}", "amount": amount, "key": key})
            )
            res = await waiting[key]
            lat.append((time.perf_counter() - t0) * 1000)
            tally[res["reason"] if not res["accepted"] else "accepted"] = (
                tally.get(res["reason"] if not res["accepted"] else "accepted", 0) + 1
            )
        await asyncio.sleep(2.0)  # let the last broadcasts arrive
        finals[idx] = latest
        task.cancel()


async def main(args):
    urls = args.urls.split(",")
    async with httpx.AsyncClient() as http:
        created = (
            await http.post(
                urls[0] + "/auctions",
                json={
                    "kind": "english",
                    "title": "load test",
                    "start_price": 1,
                    "duration": 7200,
                    "min_increment": 1,
                },
            )
        ).json()
        aid = created["id"]
        lat, tally, finals = [], {}, {}
        rng = random.Random(7)
        t0 = time.perf_counter()
        await asyncio.gather(
            *(
                run_client(
                    i,
                    urls[i % len(urls)],
                    aid,
                    args.bids_per_client,
                    lat,
                    tally,
                    finals,
                    rng,
                    args.think_ms,
                )
                for i in range(args.clients)
            )
        )
        elapsed = time.perf_counter() - t0
        truths = [(await http.get(u + f"/auctions/{aid}")).json() for u in urls]
        hist = (await http.get(urls[0] + f"/auctions/{aid}/bids")).json()
        stats = [(await http.get(u + "/stats")).json() for u in urls]

    truth = {k: truths[0][k] for k in KEYS}
    converged = sum(1 for f in finals.values() if {k: f[k] for k in KEYS} == truth)
    prices = [h["amount"] for h in hist]
    total = len(lat)
    lat.sort()
    out = {
        "instances": len(urls),
        "clients": args.clients,
        "bid_events": total,
        "outcomes": tally,
        "elapsed_s": round(elapsed, 2),
        "throughput_bids_per_s": round(total / elapsed, 1),
        "latency_ms_p50": round(statistics.median(lat), 1),
        "latency_ms_p95": round(lat[int(0.95 * (len(lat) - 1))], 1),
        "clients_converged": f"{converged}/{args.clients}",
        "instances_agree": all({k: t[k] for k in KEYS} == truth for t in truths),
        "persisted_bids_match_accepted": len(hist)
        == tally.get("accepted", 0)
        == truth["bid_count"],
        "price_history_monotonic": all(b > a for a, b in zip(prices, prices[1:], strict=False)),
        "optimistic_conflict_retries": sum(s["conflict_retries"] for s in stats),
        "final_state": truth,
    }
    print(json.dumps(out, indent=2))
    if args.out:
        with open(args.out, "w") as f:
            json.dump(out, f, indent=2)
    ok = (
        converged == args.clients
        and out["instances_agree"]
        and total >= 1000
        and out["persisted_bids_match_accepted"]
        and out["price_history_monotonic"]
    )
    raise SystemExit(0 if ok else 1)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--urls", default="http://localhost:8000")
    p.add_argument("--clients", type=int, default=50)
    p.add_argument("--bids-per-client", type=int, default=24)
    p.add_argument("--think-ms", type=float, default=50, help="max random pause between bids")
    p.add_argument("--out")
    asyncio.run(main(p.parse_args()))
