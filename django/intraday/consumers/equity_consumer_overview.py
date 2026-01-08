import json
import asyncio
from redis.asyncio import Redis
from channels.generic.websocket import AsyncWebsocketConsumer
from collections import defaultdict
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


class EquityOverviewConsumer(AsyncWebsocketConsumer):

    async def connect(self):
        await self.accept()
        print("Equity Overview WS connected")

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

            timestamp = rows[0].get("timestamp")

            # ==========================================================
            # PORTFOLIO TOTALS (MANAGER LEVEL)
            # ==========================================================
            total_exposure = sum(
                safe_num(r.get("intraday_exposure")) for r in rows
            )

            # portfolio_intraday_pnl is already summed PER MANAGER,
            # so summing it across rows would double count.
            # Correct approach: sum UNIQUE managers.
            seen_managers = {}
            for r in rows:
                m = r.get("asset_manager")
                if m and m not in seen_managers:
                    seen_managers[m] = safe_num(r.get("portfolio_intraday_pnl"))

            intraday_pnl = sum(seen_managers.values())

            tickers_streaming = len({
                r.get("ticker") for r in rows if r.get("ticker")
            })

            active_managers = len(seen_managers)

            # ==========================================================
            # TOP MOVERS (POSITION LEVEL: TICKER × MANAGER)
            # ==========================================================
            # Movers = what actually moved today → intraday_pnl
            top_movers = sorted(
                rows,
                key=lambda r: abs(safe_num(r.get("intraday_pnl"))),
                reverse=True,
            )[:10]

            # ==========================================================
            # AGG: TICKERS (ACROSS MANAGERS)
            # ==========================================================
            ticker_agg = defaultdict(lambda: {
                "ticker": None,
                "total_exposure": 0,
                "total_pnl": 0,
            })

            for r in rows:
                t = r.get("ticker")
                if not t:
                    continue

                agg = ticker_agg[t]
                agg["ticker"] = t
                agg["total_exposure"] += safe_num(r.get("intraday_exposure"))
                agg["total_pnl"] += safe_num(r.get("intraday_pnl"))

            top_tickers_agg = sorted(
                ticker_agg.values(),
                key=lambda x: abs(safe_num(x.get("total_exposure"))),
                reverse=True,
            )[:5]

            # ==========================================================
            # AGG: MANAGERS
            # ==========================================================
            manager_agg = defaultdict(lambda: {
                "manager": None,
                "total_exposure": 0,
                "total_pnl": 0,
            })

            for r in rows:
                m = r.get("asset_manager")
                if not m:
                    continue

                agg = manager_agg[m]
                agg["manager"] = m
                agg["total_exposure"] += safe_num(r.get("intraday_exposure"))
                agg["total_pnl"] += safe_num(r.get("intraday_pnl"))

            top_managers_agg = sorted(
                manager_agg.values(),
                key=lambda x: abs(safe_num(x.get("total_exposure"))),
                reverse=True,
            )[:5]

            # ==========================================================
            # ALERTS (POSITION LEVEL, EXPOSURE AWARE)
            # ==========================================================
            alerts = []

            for r in rows:
                vol = safe_num(r.get("vol_15m"))
                ret1 = safe_num(r.get("return_1m"))
                exposure = abs(safe_num(r.get("intraday_exposure")))

                if vol > 0.02 and exposure > 0:
                    alerts.append({
                        "type": "Volatility Spike",
                        "ticker": r.get("ticker"),
                        "manager": r.get("asset_manager"),
                        "severity": "medium" if exposure < 1_000_000 else "high",
                        "time": r.get("timestamp"),
                        "trigger": "vol_15m > 0.02",
                        "_sort": exposure,
                    })

                if abs(ret1) > 0.008 and exposure > 0:
                    alerts.append({
                        "type": "Return Shock",
                        "ticker": r.get("ticker"),
                        "manager": r.get("asset_manager"),
                        "severity": "high" if abs(ret1) > 0.015 else "medium",
                        "time": r.get("timestamp"),
                        "trigger": "abs(return_1m) > 0.8%",
                        "_sort": exposure,
                    })

            alerts = sorted(
                alerts,
                key=lambda x: safe_num(x.get("_sort")),
                reverse=True
            )[:10]

            for a in alerts:
                a.pop("_sort", None)

            # ==========================================================
            # PAYLOAD
            # ==========================================================
            payload = {
                "timestamp": timestamp,
                "totals": {
                    "total_exposure": total_exposure,
                    "intraday_pnl": intraday_pnl,
                    "tickers_streaming": tickers_streaming,
                    "active_managers": active_managers,
                },
                "top_movers": top_movers,             
                "top_tickers_agg": top_tickers_agg,
                "top_managers_agg": top_managers_agg,
                "active_alerts": alerts,
            }

            await self.send(json.dumps(payload))

    async def disconnect(self, code):
        print("Equity Overview WS disconnected")

        if hasattr(self, "stream_task"):
            self.stream_task.cancel()

        if hasattr(self, "pubsub"):
            await self.pubsub.close()

        if hasattr(self, "redis"):
            await self.redis.close()
