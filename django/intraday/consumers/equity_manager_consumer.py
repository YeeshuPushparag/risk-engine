"""
equity_manager_consumer.py
============================
WebSocket consumer for the equity manager page. Mirrors equity_manager()
in views.py -- both call build_equity_manager_payload().

Needs BOTH Redis stores (confirmed with user), filtered to this manager.

Unlike the ticker-scoped consumers, if this manager doesn't appear in a
given message's rows, the message is simply skipped (not sent) -- a
manager not being found is a URL/identity mismatch, not a transient
market-data-availability condition, so there's nothing meaningful to
push to the client for that tick.
"""

import json
import asyncio
from redis.asyncio import Redis
from channels.generic.websocket import AsyncWebsocketConsumer
import os

from ..views import (
    build_equity_manager_payload,
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


class EquityManagerConsumer(AsyncWebsocketConsumer):

    async def connect(self):
        raw_manager = self.scope["url_route"]["kwargs"]["manager"]
        self.manager = raw_manager.replace("-", " ").title()

        await self.accept()
        print(f"Manager WS connected: {self.manager}")

        self.redis = Redis(
            host=os.getenv("REDIS_HOST"),
            port=int(os.getenv("REDIS_PORT", 6379)),
            db=int(os.environ.get("REDIS_DB_STREAM", 1)),
            decode_responses=True
        )
        self.pubsub = self.redis.pubsub()
        await self.pubsub.subscribe("equity_stream", "equity_stream_synced")

        self.stream_task = asyncio.create_task(self.stream())

    async def stream(self):
        async for message in self.pubsub.listen():

            if message["type"] != "message":
                continue

            all_raw    = await self.redis.get(EQUITY_ALL_KEY)
            synced_raw = await self.redis.get(EQUITY_SYNCED_KEY)

            all_rows    = _parse_rows(all_raw)
            synced_rows = _parse_rows(synced_raw)

            if not all_rows:
                continue

            payload = build_equity_manager_payload(all_rows, synced_rows, self.manager)
            if payload is None:
                continue

            await self.send(json.dumps(payload, default=str))

    async def disconnect(self, code):
        print(f"Manager WS disconnected: {self.manager}")

        if hasattr(self, "stream_task"):
            self.stream_task.cancel()

        if hasattr(self, "pubsub"):
            await self.pubsub.close()

        if hasattr(self, "redis"):
            await self.redis.close()
