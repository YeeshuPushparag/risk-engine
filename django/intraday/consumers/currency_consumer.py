"""
currency_consumer.py
======================
WebSocket consumer for the FX currency page. Mirrors fx_currency_initial()
in views.py -- both call build_fx_currency_payload().

Needs BOTH Redis stores (per user's explicit instruction that fx_currency
should match equity_manager). Filtered to this currency pair.

Same group-scoped behavior as equity_manager_consumer.py: if this
currency isn't present in a given message's rows, the message is simply
skipped rather than sent.
"""

import json
import asyncio
from redis.asyncio import Redis
from channels.generic.websocket import AsyncWebsocketConsumer
import os

from ..views import (
    build_fx_currency_payload,
    FX_ALL_KEY,
    FX_SYNCED_KEY,
)


def _parse_rows(raw):
    if not raw:
        return []
    try:
        rows = json.loads(raw)
        return rows if isinstance(rows, list) else []
    except Exception:
        return []


class CurrencyConsumer(AsyncWebsocketConsumer):

    async def connect(self):
        self.currency = self.scope["url_route"]["kwargs"]["currency"]

        await self.accept()
        print("WS currency:", self.currency)

        self.redis = Redis(
            host=os.getenv("REDIS_HOST"),
            port=int(os.getenv("REDIS_PORT", 6379)),
            db=int(os.environ.get("REDIS_DB_STREAM", 1)),
            decode_responses=True
        )
        self.pubsub = self.redis.pubsub()
        await self.pubsub.subscribe("fx_stream", "fx_stream_all")

        self.stream_task = asyncio.create_task(self.stream())

    async def stream(self):
        async for message in self.pubsub.listen():

            if message.get("type") != "message":
                continue

            all_raw    = await self.redis.get(FX_ALL_KEY)
            synced_raw = await self.redis.get(FX_SYNCED_KEY)

            all_rows    = _parse_rows(all_raw)
            synced_rows = _parse_rows(synced_raw)

            if not all_rows:
                continue

            payload = build_fx_currency_payload(all_rows, synced_rows, self.currency)
            if payload is None:
                continue

            await self.send(json.dumps(payload, default=str))

    async def disconnect(self, code):
        print("Currency WS disconnected:", self.currency)

        if hasattr(self, "stream_task"):
            self.stream_task.cancel()

        if hasattr(self, "pubsub"):
            await self.pubsub.close()

        if hasattr(self, "redis"):
            await self.redis.close()
