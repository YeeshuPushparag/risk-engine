import json
import asyncio
from redis.asyncio import Redis
from channels.generic.websocket import AsyncWebsocketConsumer
import os

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
        await self.pubsub.subscribe("fx_stream")

        self.stream_task = asyncio.create_task(self.stream())

    # -------------------------------------------------
    # SAFE NUMERIC COERCION (NO RENAMING)
    # -------------------------------------------------
    def _num(self, x):
        if x is None:
            return 0
        try:
            return float(x)
        except Exception:
            return 0

    async def stream(self):
        async for message in self.pubsub.listen():
            if message.get("type") != "message":
                continue

            try:
                rows = json.loads(message.get("data") or "[]")
            except Exception:
                continue

            if not rows:
                continue

            # ---------------------------------------------
            # FILTER BY CURRENCY
            # ---------------------------------------------
            filtered = [
                r for r in rows
                if r.get("currency_pair") == self.currency
            ]

            if not filtered:
                continue

            # ---------------------------------------------
            # AGGREGATES
            # ---------------------------------------------
            total_exposure = sum(
                self._num(r.get("position_size")) for r in filtered
            )

            total_pnl = sum(
                self._num(r.get("fx_pnl")) for r in filtered
            )

            worst_var = max(
                (self._num(r.get("VaR_95_15m")) for r in filtered),
                default=0
            )

            tickers_count = len(filtered)

            # ---------------------------------------------
            # MARKET SNAPSHOT (CURRENT + PREVIOUS)
            # ---------------------------------------------
            s = filtered[0]

            market = {
                "open": s.get("open"),
                "high": s.get("high"),
                "low": s.get("low"),
                "close": s.get("close"),

                "prev_open": s.get("prev_open"),
                "prev_high": s.get("prev_high"),
                "prev_low": s.get("prev_low"),
                "prev_close": s.get("prev_close"),

                "timestamp": s.get("timestamp"),
            }

            # ---------------------------------------------
            # TOP TICKERS
            # ---------------------------------------------
            filtered.sort(
                key=lambda r: self._num(r.get("position_size")),
                reverse=True
            )
            top_tickers = filtered[:5]

            # ---------------------------------------------
            # PAYLOAD (UNCHANGED SHAPE)
            # ---------------------------------------------
            payload = {
                "currency": self.currency,
                "market": market,
                "totals": {
                    "total_exposure": total_exposure,
                    "total_fx_pnl": total_pnl,
                    "worst_var_95": worst_var,
                    "ticker_count": tickers_count,
                },
                "tickers": top_tickers,
            }

            await self.send(json.dumps(payload))

    async def disconnect(self, code):
        print("Currency WS disconnected:", self.currency)

        if hasattr(self, "stream_task"):
            self.stream_task.cancel()

        if hasattr(self, "pubsub"):
            await self.pubsub.close()

        if hasattr(self, "redis"):
            await self.redis.close()
