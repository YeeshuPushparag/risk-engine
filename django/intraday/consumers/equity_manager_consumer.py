import json
import asyncio
from redis.asyncio import Redis
from channels.generic.websocket import AsyncWebsocketConsumer
import os

def safe_num(v):
    try:
        if v is None:
            return 0
        if isinstance(v, float) and v != v:  # NaN
            return 0
        return v
    except Exception:
        return 0


class EquityManagerConsumer(AsyncWebsocketConsumer):

    async def connect(self):
        raw_manager = self.scope["url_route"]["kwargs"]["manager"]

        # Display version (title case)
        self.manager = raw_manager.replace("-", " ").title()

        # Normalized version for filtering
        self.normalized_manager = raw_manager.replace("-", " ").lower()

        await self.accept()
        print(f"Manager WS connected: {self.manager}")

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
            # Filter rows for this manager
            # ---------------------------------------------
            filtered = [
                r for r in rows
                if r.get("asset_manager", "").replace("-", " ").lower()
                == self.normalized_manager
            ]

            if not filtered:
                continue

            # ---------------------------------------------
            # Aggregates
            # ---------------------------------------------
            exposure = sum(safe_num(r.get("intraday_exposure")) for r in filtered)
            pnl = sum(safe_num(r.get("portfolio_intraday_pnl")) for r in filtered)

            count = len(filtered)

            avg_return_1m = (
                sum(safe_num(r.get("return_1m")) for r in filtered) / count
                if count else 0
            )
            avg_return_5m = (
                sum(safe_num(r.get("return_5m")) for r in filtered) / count
                if count else 0
            )
            avg_vol_15m = (
                sum(safe_num(r.get("vol_15m")) for r in filtered) / count
                if count else 0
            )

            portfolio_total_exposure = sum(
                safe_num(r.get("intraday_exposure")) for r in rows
            )
            weight = (
                exposure / portfolio_total_exposure
                if portfolio_total_exposure else 0
            )

            holdings_count = count
            timestamp = rows[0].get("timestamp")

            # ---------------------------------------------
            # Holdings (top 50 by exposure)
            # ---------------------------------------------
            holdings = sorted(
                filtered,
                key=lambda r: safe_num(r.get("intraday_exposure")),
                reverse=True
            )[:50]

            # ---------------------------------------------
            # Alerts (ranked by exposure)
            # ---------------------------------------------
            alerts = []

            for r in filtered:
                vol = r.get("vol_15m")
                ret1 = r.get("return_1m")
                row_exposure = safe_num(r.get("intraday_exposure"))

                if vol is not None and vol == vol and vol > 0.02:
                    alerts.append({
                        "type": "Volatility Spike",
                        "ticker": r.get("ticker"),
                        "severity": "medium",
                        "time": r.get("timestamp"),
                        "trigger": "vol_15m > 0.02",
                        "_exposure": row_exposure,
                    })

                if ret1 is not None and ret1 == ret1 and abs(ret1) > 0.008:
                    alerts.append({
                        "type": "Return Shock",
                        "ticker": r.get("ticker"),
                        "severity": "high" if abs(ret1) > 0.015 else "medium",
                        "time": r.get("timestamp"),
                        "trigger": "abs(return_1m) > 0.8%",
                        "_exposure": row_exposure,
                    })

            alerts = sorted(
                alerts,
                key=lambda a: safe_num(a.get("_exposure")),
                reverse=True
            )[:10]

            for a in alerts:
                a.pop("_exposure", None)

            # ---------------------------------------------
            # Payload (UNCHANGED)
            # ---------------------------------------------
            payload = {
                "manager": self.manager,
                "timestamp": timestamp,
                "totals": {
                    "intraday_exposure": exposure,
                    "intraday_pnl": pnl,
                    "return_1m": avg_return_1m,
                    "return_5m": avg_return_5m,
                    "vol_15m": avg_vol_15m,
                    "alerts": len(alerts),
                    "weight": weight,
                    "holdings_count": holdings_count,
                },
                "holdings": holdings,
                "alerts": alerts,
            }

            await self.send(json.dumps(payload))

    async def disconnect(self, code):
        print(f"Manager WS disconnected: {self.manager}")

        if hasattr(self, "stream_task"):
            self.stream_task.cancel()

        if hasattr(self, "pubsub"):
            await self.pubsub.close()

        if hasattr(self, "redis"):
            await self.redis.close()
