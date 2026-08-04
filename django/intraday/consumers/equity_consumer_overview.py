"""
equity_consumer_overview.py
============================
WebSocket consumer for the equity main dashboard (Portfolio Overview +
Market Data Health + Data Quality Issues). Mirrors equity_overview()
in views.py exactly -- both call the same build_equity_overview_payload()
so REST and WS can never silently drift apart.

Needs BOTH Redis stores (confirmed with user): the all-tickers store
("equity_stream" pubsub / "equity_latest_snapshot" key) for valuation +
freshness, and the synced store ("equity_stream_synced" pubsub /
"equity_latest_snapshot_synced" key), included in the payload for
whatever the frontend needs it for.

Import note: this file lives in intraday/consumers/, one level below
views.py (intraday/views.py) -- so the import must go up two package
levels (..views), not one (.views, which would incorrectly look for
intraday/consumers/views.py).
"""

import json
import asyncio
from redis.asyncio import Redis
from channels.generic.websocket import AsyncWebsocketConsumer
import os

from ..views import (
    build_equity_overview_payload,
    load_equity_universe_tickers,
    EQUITY_ALL_KEY,
    EQUITY_SYNCED_KEY,
)


def _parse_rows(raw):
    if not raw:
        return []
    try:
        rows = json.loads(raw)
        return rows if isinstance(rows, list) else []
    except Exception:
        return []


class EquityOverviewConsumer(AsyncWebsocketConsumer):

    async def connect(self):
        await self.accept()
        print("Equity Overview WS connected")

        self.redis = Redis(
            host=os.getenv("REDIS_HOST"),
            port=int(os.getenv("REDIS_PORT", 6379)),
            db=int(os.environ.get("REDIS_DB_STREAM", 1)),
            decode_responses=True
        )
        self.pubsub = self.redis.pubsub()
        # Subscribe to BOTH channels -- either one firing means the
        # combined payload (re-fetched fresh from both keys below) may
        # have changed.
        await self.pubsub.subscribe("equity_stream", "equity_stream_synced")

        self.stream_task = asyncio.create_task(self.stream())

    async def stream(self):
        async for message in self.pubsub.listen():

            if message["type"] != "message":
                continue

            # Always re-fetch BOTH keys fresh, regardless of which
            # channel triggered this wakeup -- the other store may not
            # have changed on this exact tick, but the combined payload
            # must always reflect current state of both.
            all_raw    = await self.redis.get(EQUITY_ALL_KEY)
            synced_raw = await self.redis.get(EQUITY_SYNCED_KEY)

            all_rows    = _parse_rows(all_raw)
            synced_rows = _parse_rows(synced_raw)

            if not all_rows:
                continue

            # Blocking call (cached S3 read, ~5 min TTL) -- same
            # accepted pattern as the yfinance call elsewhere in this
            # codebase; only actually hits S3 on a cache miss.
            universe_tickers = load_equity_universe_tickers()

            payload = build_equity_overview_payload(all_rows, synced_rows, universe_tickers)
            await self.send(json.dumps(payload, default=str))

    async def disconnect(self, code):
        print("Equity Overview WS disconnected")

        if hasattr(self, "stream_task"):
            self.stream_task.cancel()

        if hasattr(self, "pubsub"):
            await self.pubsub.close()

        if hasattr(self, "redis"):
            await self.redis.close()
