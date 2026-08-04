"""
equity_ticker_consumer.py
============================
WebSocket consumer for the individual equity ticker page. Mirrors
equity_ticker() in views.py -- both call build_equity_ticker_payload(),
which includes the yfinance marketCap fetch (same blocking-call pattern
already accepted elsewhere in this codebase).

ALL store only, per page scope confirmed with the user (no synced data
needed here).

Unlike the manager/currency consumers, this ALWAYS sends a payload, even
when the ticker currently has no row ("_not_found" case) -- a missing
ticker here means "no live price right now," which is exactly the kind
of thing that should update live in the UI (transitioning between
"Live" and "Missing market data" as availability changes), not something
to silently skip.
"""

import json
import asyncio
from redis.asyncio import Redis
from channels.generic.websocket import AsyncWebsocketConsumer
import os

from ..views import build_equity_ticker_payload, EQUITY_ALL_KEY


def _parse_rows(raw):
    if not raw:
        return []
    try:
        rows = json.loads(raw)
        return rows if isinstance(rows, list) else []
    except Exception:
        return []


class EquityTickerConsumer(AsyncWebsocketConsumer):

    async def connect(self):
        self.ticker = self.scope["url_route"]["kwargs"]["ticker"]

        await self.accept()
        print(f"Ticker WS connected: {self.ticker}")

        self.redis = Redis(
            host=os.getenv("REDIS_HOST"),
            port=int(os.getenv("REDIS_PORT", 6379)),
            db=int(os.environ.get("REDIS_DB_STREAM", 1)),
            decode_responses=True
        )
        self.pubsub = self.redis.pubsub()
        await self.pubsub.subscribe("equity_stream")

        self.stream_task = asyncio.create_task(self.stream())

    async def stream(self):
        async for message in self.pubsub.listen():

            if message["type"] != "message":
                continue

            all_raw  = await self.redis.get(EQUITY_ALL_KEY)
            all_rows = _parse_rows(all_raw)

            if not all_rows:
                continue

            payload = build_equity_ticker_payload(all_rows, self.ticker)
            payload.pop("_not_found", None)

            await self.send(json.dumps(payload, default=str))

    async def disconnect(self, code):
        print(f"Ticker WS disconnected: {self.ticker}")

        if hasattr(self, "stream_task"):
            self.stream_task.cancel()

        if hasattr(self, "pubsub"):
            await self.pubsub.close()

        if hasattr(self, "redis"):
            await self.redis.close()
