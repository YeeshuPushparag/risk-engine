import json
import asyncio
from redis.asyncio import Redis
from channels.generic.websocket import AsyncWebsocketConsumer
import os

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
        await self.pubsub.subscribe("fx_stream")

        self.stream_task = asyncio.create_task(self.stream())

    # ---------------- SAFE NUMBER ----------------
    def _num(self, x):
        if x is None:
            return 0.0
        try:
            return float(x)
        except Exception:
            return 0.0

    async def stream(self):
        async for message in self.pubsub.listen():
            if message.get("type") != "message":
                continue

            try:
                rows = json.loads(message.get("data") or "[]")
            except Exception:
                continue

            # Find THIS ticker
            row = next(
                (r for r in rows if r.get("ticker") == self.ticker),
                None
            )

            if not row:
                continue

            position_size = self._num(row.get("position_size"))
            fx_pnl = self._num(row.get("fx_pnl"))
            var_95 = self._num(row.get("VaR_95_15m"))

            payload = {
                "ticker": self.ticker,
                "currency_pair": row.get("currency_pair"),
                "timestamp": row.get("timestamp"),

                "open": row.get("open"),
                "high": row.get("high"),
                "low": row.get("low"),
                "close": row.get("close"),
                "prev_close": row.get("prev_close"),

                "fx_return_1m": row.get("fx_return_1m"),
                "fx_return_5m": row.get("fx_return_5m"),
                "fx_vol_15m": row.get("fx_vol_15m"),

                "position_size": position_size,
                "fx_pnl": fx_pnl,
                "VaR_95_15m": var_95,

                "totals": {
                    "total_exposure": position_size,
                    "total_fx_pnl": fx_pnl,
                    "worst_var_95": var_95,
                },
            }

            await self.send(json.dumps(payload))

    async def disconnect(self, code):
        print("Ticker WS disconnected:", self.ticker)

        if hasattr(self, "stream_task"):
            self.stream_task.cancel()
        if hasattr(self, "pubsub"):
            await self.pubsub.close()
        if hasattr(self, "redis"):
            await self.redis.close()
