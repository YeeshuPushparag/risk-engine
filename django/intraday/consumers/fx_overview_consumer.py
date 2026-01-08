import json
import asyncio
from redis.asyncio import Redis
from channels.generic.websocket import AsyncWebsocketConsumer
from collections import defaultdict
import os

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
        await self.pubsub.subscribe("fx_stream")

        self.stream_task = asyncio.create_task(self.stream_messages())

    # -------------------------------------------------
    # SAFE HELPERS (NO RENAMING, NO SIDE EFFECTS)
    # -------------------------------------------------
    def _num(self, x):
        """Safe numeric coercion for WS aggregation"""
        if x is None:
            return 0
        try:
            return float(x)
        except Exception:
            return 0

    # -------------------------------------------------
    # STREAM LOOP
    # -------------------------------------------------
    async def stream_messages(self):
        async for message in self.pubsub.listen():

            if message.get("type") != "message":
                continue

            try:
                rows = json.loads(message.get("data") or "[]")
            except Exception:
                continue

            if not rows:
                continue

            # -------------------------------------------------
            # GLOBAL AGGREGATES
            # -------------------------------------------------
            timestamp = rows[0].get("timestamp")

            total_exposure = sum(self._num(r.get("position_size")) for r in rows)
            total_pnl = sum(self._num(r.get("fx_pnl")) for r in rows)

            worst_var = max(
                (self._num(r.get("VaR_95_15m")) for r in rows),
                default=0
            )

            active_pairs = len({
                r.get("currency_pair")
                for r in rows
                if r.get("currency_pair")
            })

            # -------------------------------------------------
            # TOP TICKERS
            # -------------------------------------------------
            sorted_rows = sorted(
                rows,
                key=lambda r: self._num(r.get("position_size")),
                reverse=True
            )
            top_tickers = sorted_rows[:5]

            # -------------------------------------------------
            # CURRENCY SUMMARY
            # -------------------------------------------------
            grouped = defaultdict(list)
            currency_summary = {}

            for r in rows:
                ccy = r.get("currency_pair")
                if ccy:
                    grouped[ccy].append(r)

            for currency, items in grouped.items():
                currency_summary[currency] = {
                    "total_exposure": sum(
                        self._num(r.get("position_size")) for r in items
                    ),
                    "total_fx_pnl": sum(
                        self._num(r.get("fx_pnl")) for r in items
                    ),
                    "worst_var_95": max(
                        (self._num(r.get("VaR_95_15m")) for r in items),
                        default=0
                    ),
                    "ticker_count": len(items),
                }

            # -------------------------------------------------
            # FINAL PAYLOAD (UNCHANGED SHAPE)
            # -------------------------------------------------
            payload = {
                "timestamp": timestamp,
                "totals": {
                    "total_exposure": total_exposure,
                    "total_fx_pnl": total_pnl,
                    "worst_var_95": worst_var,
                    "active_currency_pairs": active_pairs,
                },
                "top_tickers": top_tickers,
                "currency_summary": currency_summary,
            }

            await self.send(json.dumps(payload))

    async def disconnect(self, code):
        print("Overview WS disconnected")

        if hasattr(self, "stream_task"):
            self.stream_task.cancel()

        if hasattr(self, "pubsub"):
            await self.pubsub.close()

        if hasattr(self, "redis"):
            await self.redis.close()
