import json
import asyncio
from redis.asyncio import Redis
from channels.generic.websocket import AsyncWebsocketConsumer
import os

def safe_num(v):
    try:
        if v is None:
            return 0
        if isinstance(v, float) and v != v:  # NaN check
            return 0
        return v
    except Exception:
        return 0


class EquityTickerManagerConsumer(AsyncWebsocketConsumer):

    async def connect(self):

        self.ticker = self.scope["url_route"]["kwargs"]["ticker"]
        raw_manager = self.scope["url_route"]["kwargs"]["manager"]

        # Display
        self.manager = raw_manager.replace("-", " ").title()
        # Normalized
        self.normalized_manager = raw_manager.replace("-", " ").lower()

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

            try:
                rows = json.loads(message["data"])
            except Exception:
                continue

            if not rows:
                continue

            # ---------------------------------------------
            # Find exact row (ticker + manager)
            # ---------------------------------------------
            row = next(
                (
                    r for r in rows
                    if r.get("ticker") == self.ticker
                    and r.get("asset_manager", "")
                        .replace("-", " ")
                        .lower() == self.normalized_manager
                ),
                None,
            )

            if not row:
                continue

            # ---------------------------------------------
            # Alerts (NaN safe)
            # ---------------------------------------------
            alerts = []

            vol = row.get("vol_15m")
            ret1 = row.get("return_1m")

            if vol is not None and vol == vol and vol > 0.02:
                alerts.append({
                    "type": "Volatility Spike",
                    "severity": "medium",
                    "time": row.get("timestamp"),
                    "trigger": "vol_15m > 0.02",
                })

            if ret1 is not None and ret1 == ret1 and abs(ret1) > 0.008:
                alerts.append({
                    "type": "Return Shock",
                    "severity": "high" if abs(ret1) > 0.015 else "medium",
                    "time": row.get("timestamp"),
                    "trigger": "abs(return_1m) > 0.8%",
                })

            # ---------------------------------------------
            # Payload (UNCHANGED FIELDS)
            # ---------------------------------------------
            payload = {
                "timestamp": row.get("timestamp"),
                "ticker": row.get("ticker"),
                "manager": row.get("asset_manager"),

                "totals": {
                    "exposure": row.get("intraday_exposure"),
                    "pnl": row.get("portfolio_intraday_pnl"),
                    "shares": row.get("Shares"),
                    "weight": (
                        safe_num(row.get("intraday_exposure")) /
                        safe_num(row.get("portfolio_intraday_exposure") or 1)
                    ),
                    "price": row.get("close"),
                },

                "signals": {
                    "return_1m": row.get("return_1m"),
                    "return_5m": row.get("return_5m"),
                    "vol_15m": row.get("vol_15m"),
                    "range_pct_1m": row.get("range_pct_1m"),
                    "vwap_5m": row.get("rolling_vwap_5m"),
                    "close_diff": row.get("close_diff"),
                    "rolling_high": row.get("rolling_high_5m"),
                    "rolling_low": row.get("rolling_low_5m"),
                    "trend_slope_5m": row.get("trend_slope_5m"),
                    "breakout_strength": row.get("breakout_strength"),
                    "volume_burst": row.get("volume_burst"),
                },

                "alerts": alerts,
            }

            await self.send(json.dumps(payload))

    async def disconnect(self, code):
        print(f"WS ticker+manager disconnected: {self.ticker} / {self.manager}")

        if hasattr(self, "stream_task"):
            self.stream_task.cancel()

        if hasattr(self, "pubsub"):
            await self.pubsub.close()

        if hasattr(self, "redis"):
            await self.redis.close()
