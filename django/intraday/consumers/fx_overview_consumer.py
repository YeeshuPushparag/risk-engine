"""
fx_overview_consumer.py
=========================
WebSocket consumer for the FX main dashboard. Mirrors fx_overview_initial()
in views.py -- both call build_fx_overview_payload().

Needs BOTH Redis stores (confirmed with user). Note FX's key/channel
naming is the OPPOSITE direction of equity's: "fx_stream" is the SYNCED
store (pre-existing key name), "fx_stream_all" is the ALL-pairs store
(new key name) -- see views.py's module docstring for the full mapping.

FX has no universe file -- FX_CURRENCY_PAIRS (the static 7 pairs) IS the
universe, so there's no blocking S3/CSV read here at all.
"""

import json
import asyncio
from redis.asyncio import Redis
from channels.generic.websocket import AsyncWebsocketConsumer
import os

from ..views import (
    build_fx_overview_payload,
    FX_CURRENCY_PAIRS,
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


class OverviewConsumer(AsyncWebsocketConsumer):

    async def connect(self):
        await self.accept()
        print("Overview WS connected")

        self.redis = Redis(
            host=os.getenv("REDIS_HOST"),
            port=int(os.getenv("REDIS_PORT", 6379)),
            db=int(os.environ.get("REDIS_DB_STREAM", 1)),
            decode_responses=True
        )
        self.pubsub = self.redis.pubsub()
        # "fx_stream" = synced store, "fx_stream_all" = all-pairs store.
        await self.pubsub.subscribe("fx_stream", "fx_stream_all")

        self.stream_task = asyncio.create_task(self.stream_messages())

    async def stream_messages(self):
        async for message in self.pubsub.listen():

            if message.get("type") != "message":
                continue

            all_raw    = await self.redis.get(FX_ALL_KEY)
            synced_raw = await self.redis.get(FX_SYNCED_KEY)

            all_rows    = _parse_rows(all_raw)
            synced_rows = _parse_rows(synced_raw)

            if not all_rows:
                continue

            payload = build_fx_overview_payload(all_rows, synced_rows, FX_CURRENCY_PAIRS)
            await self.send(json.dumps(payload, default=str))

    async def disconnect(self, code):
        print("Overview WS disconnected")

        if hasattr(self, "stream_task"):
            self.stream_task.cancel()

        if hasattr(self, "pubsub"):
            await self.pubsub.close()

        if hasattr(self, "redis"):
            await self.redis.close()
