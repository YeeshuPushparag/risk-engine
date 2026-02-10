import json
import asyncio
from redis.asyncio import Redis
from channels.generic.websocket import AsyncWebsocketConsumer
import yfinance as yf
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


class EquityTickerConsumer(AsyncWebsocketConsumer):

    async def connect(self):
        self.ticker = self.scope["url_route"]["kwargs"]["ticker"]

        await self.accept()
        print(f"Ticker WS connected: {self.ticker}")

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
            # Filter rows for this ticker
            # ---------------------------------------------
            filtered = [r for r in rows if r.get("ticker") == self.ticker]
            if not filtered:
                continue

            # ---------------------------------------------
            # Market cap refresh (safe)
            # ---------------------------------------------
            fetched_market_cap = None
            try:
                yf_ticker = yf.Ticker(self.ticker)
                info = yf_ticker.info or {}
                fetched_market_cap = info.get("marketCap")
            except Exception:
                pass

            for r in filtered:
                r["marketCap"] = fetched_market_cap or r.get("marketCap")

            # ---------------------------------------------
            # Portfolio aggregates (SAFE)
            # ---------------------------------------------
            total_exposure = sum(
                safe_num(r.get("intraday_exposure")) for r in filtered
            )
            total_pnl = sum(
                safe_num(r.get("portfolio_intraday_pnl")) for r in filtered
            )
            managers_count = len({
                r.get("asset_manager") for r in filtered if r.get("asset_manager")
            })

            portfolio_total_exposure = sum(
                safe_num(r.get("intraday_exposure")) for r in rows
            )

            portfolio_weight = (
                total_exposure / portfolio_total_exposure
                if portfolio_total_exposure else 0
            )

            # ---------------------------------------------
            # Market snapshot
            # ---------------------------------------------
            s = filtered[0]

            market = {
                "open": s.get("open"),
                "high": s.get("high"),
                "low": s.get("low"),
                "close": s.get("close"),
                "volume": s.get("volume"),

                "prev_open": s.get("prev_open"),
                "prev_high": s.get("prev_high"),
                "prev_low": s.get("prev_low"),
                "prev_close": s.get("prev_close"),
                "prev_volume": s.get("prev_volume"),

                "return_1m": s.get("return_1m"),
                "return_5m": s.get("return_5m"),
                "vol_15m": s.get("vol_15m"),
                "range_pct_1m": s.get("range_pct_1m"),

                "rolling_vwap_5m": s.get("rolling_vwap_5m"),
                "rolling_high_5m": s.get("rolling_high_5m"),
                "rolling_low_5m": s.get("rolling_low_5m"),
                "trend_slope_5m": s.get("trend_slope_5m"),
                "breakout_strength": s.get("breakout_strength"),
                "volume_burst": s.get("volume_burst"),

                "timestamp": s.get("timestamp"),
            }

            # ---------------------------------------------
            # Fundamentals (unchanged)
            # ---------------------------------------------
            fundamentals = {
                "issuer_name": s.get("issuer_name"),
                "class_name": s.get("class"),
                "cusip": s.get("CUSIP"),
                "sector": s.get("sector"),
                "industry": s.get("industry"),

                "marketCap": s.get("marketCap"),
                "totalAssets": s.get("totalAssets"),
                "totalDebt": s.get("totalDebt"),
                "revenue": s.get("revenue"),
                "ebitda": s.get("ebitda"),
                "net_income": s.get("netIncome"),

                "ebitda_margin": s.get("ebitda_margin"),
                "debt_to_assets": s.get("debt_to_assets"),
                "debt_to_ebitda": s.get("debt_to_ebitda"),
            }

            # ---------------------------------------------
            # Alerts (NaN safe)
            # ---------------------------------------------
            alerts = []

            vol = s.get("vol_15m")
            ret1 = s.get("return_1m")

            if vol is not None and vol == vol and vol > 0.02:
                alerts.append({
                    "type": "volatility_spike",
                    "severity": "medium",
                    "time": s.get("timestamp"),
                    "trigger": "vol_15m > 0.02",
                })

            if ret1 is not None and ret1 == ret1 and abs(ret1) > 0.008:
                alerts.append({
                    "type": "return_shock",
                    "severity": "high" if abs(ret1) > 0.015 else "medium",
                    "time": s.get("timestamp"),
                    "trigger": "abs(return_1m) > 0.8%",
                })

            # ---------------------------------------------
            # Manager breakdown (SAFE DIV)
            # ---------------------------------------------
            manager_breakdown = [
                {
                    "manager": r.get("asset_manager"),
                    "exposure": r.get("intraday_exposure"),
                    "pnl": r.get("portfolio_intraday_pnl"),
                    "weight": (
                        safe_num(r.get("intraday_exposure")) / total_exposure
                        if total_exposure else 0
                    ),
                }
                for r in filtered
            ]

            # ---------------------------------------------
            # Payload (UNCHANGED)
            # ---------------------------------------------
            payload = {
                "ticker": self.ticker,
                "totals": {
                    "intraday_exposure": total_exposure,
                    "intraday_pnl": total_pnl,
                    "portfolio_weight": portfolio_weight,
                    "managers_count": managers_count,
                },
                "market": market,
                "fundamentals": fundamentals,
                "alerts": alerts,
                "manager_breakdown": manager_breakdown,
            }

            await self.send(json.dumps(payload))

    async def disconnect(self, code):
        print(f"Ticker WS disconnected: {self.ticker}")

        if hasattr(self, "stream_task"):
            self.stream_task.cancel()

        if hasattr(self, "pubsub"):
            await self.pubsub.close()

        if hasattr(self, "redis"):
            await self.redis.close()
