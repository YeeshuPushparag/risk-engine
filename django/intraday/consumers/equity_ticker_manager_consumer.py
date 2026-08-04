"""
equity_ticker_manager_consumer.py
====================================
WebSocket consumer for the ticker+manager position page. Mirrors
equity_ticker_manager() in views.py -- both call
build_equity_ticker_manager_payload().

ALL store only, per page scope confirmed with the user.

Ticker-scoped (like equity_ticker_consumer.py): always sends, including
the "_not_found" case, so the client sees the position transition to a
"missing market data" state live rather than freezing on stale data.
"""

import json
import asyncio
from redis.asyncio import Redis
from channels.generic.websocket import AsyncWebsocketConsumer
import os

from ..views import build_equity_ticker_manager_payload, EQUITY_ALL_KEY


def _parse_rows(raw):
    if not raw:
        return []
    try:
        rows = json.loads(raw)
        return rows if isinstance(rows, list) else []
    except Exception:
        return []


class EquityTickerManagerConsumer(AsyncWebsocketConsumer):

    async def connect(self):
        self.ticker = self.scope["url_route"]["kwargs"]["ticker"]
        raw_manager = self.scope["url_route"]["kwargs"]["manager"]

        self.manager = raw_manager.replace("-", " ").title()

        await self.accept()
        print(f"WS ticker+manager: {self.ticker} / {self.manager}")

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

            payload = build_equity_ticker_manager_payload(all_rows, self.ticker, self.manager)
            payload.pop("_not_found", None)

            await self.send(json.dumps(payload, default=str))

    async def disconnect(self, code):
        print(f"WS ticker+manager disconnected: {self.ticker} / {self.manager}")

        if hasattr(self, "stream_task"):
            self.stream_task.cancel()

        if hasattr(self, "pubsub"):
            await self.pubsub.close()

        if hasattr(self, "redis"):
            await self.redis.close()
