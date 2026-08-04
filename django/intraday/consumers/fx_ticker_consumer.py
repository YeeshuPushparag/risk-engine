"""
fx_ticker_consumer.py
========================
WebSocket consumer for the individual FX ticker page. Mirrors
fx_ticker_initial() in views.py -- both call build_fx_ticker_payload().

ALL-pairs store only ("fx_stream_all"), per page scope confirmed with
the user.

Ticker-scoped (like the equity ticker consumers): always sends, including
the "_not_found" case, so the client sees the ticker transition to a
"missing market data" state live.
"""

import json
import asyncio
from redis.asyncio import Redis
from channels.generic.websocket import AsyncWebsocketConsumer
import os

from ..views import build_fx_ticker_payload, FX_ALL_KEY


def _parse_rows(raw):
    if not raw:
        return []
    try:
        rows = json.loads(raw)
        return rows if isinstance(rows, list) else []
    except Exception:
        return []


class TickerConsumer(AsyncWebsocketConsumer):

    async def connect(self):
        self.ticker = self.scope["url_route"]["kwargs"]["ticker"]

        await self.accept()
        print("WS ticker connected:", self.ticker)

        self.redis = Redis(
            host=os.getenv("REDIS_HOST"),
            port=int(os.getenv("REDIS_PORT", 6379)),
            db=int(os.environ.get("REDIS_DB_STREAM", 1)),
            decode_responses=True
        )
        self.pubsub = self.redis.pubsub()
        await self.pubsub.subscribe("fx_stream_all")

        self.stream_task = asyncio.create_task(self.stream())

    async def stream(self):
        async for message in self.pubsub.listen():
            if message.get("type") != "message":
                continue

            all_raw  = await self.redis.get(FX_ALL_KEY)
            all_rows = _parse_rows(all_raw)

            if not all_rows:
                continue

            payload = build_fx_ticker_payload(all_rows, self.ticker)
            payload.pop("_not_found", None)

            await self.send(json.dumps(payload, default=str))

    async def disconnect(self, code):
        print("Ticker WS disconnected:", self.ticker)

        if hasattr(self, "stream_task"):
            self.stream_task.cancel()
        if hasattr(self, "pubsub"):
            await self.pubsub.close()
        if hasattr(self, "redis"):
            await self.redis.close()
