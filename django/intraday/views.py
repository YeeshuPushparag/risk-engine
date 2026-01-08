from django.http import JsonResponse
import redis, json
from collections import defaultdict
import yfinance as yf
import math
import os

def safe_num(x):
    try:
        if x is None:
            return 0.0
        if isinstance(x, float) and math.isnan(x):
            return 0.0
        return float(x)
    except Exception:
        return 0.0





redis_client = redis.Redis(
    host=os.getenv("REDIS_HOST"), 
    port=int(os.getenv("REDIS_PORT", 6379)),   
    db=int(os.environ.get("REDIS_DB_STREAM", 1)),
    decode_responses=True
)


def fx_overview_initial(request):

    raw = redis_client.get("fx_latest_snapshot")

    if not raw:
        return JsonResponse({
            "timestamp": None,
            "totals": {
                "total_exposure": 0,
                "total_fx_pnl": 0,
                "worst_var_95": 0,
                "active_currency_pairs": 0,
            },
            "top_tickers": [],
            "currency_summary": {},
        })

    try:
        rows = json.loads(raw)
    except Exception:
        rows = []

    if not rows:
        return JsonResponse({
            "timestamp": None,
            "totals": {
                "total_exposure": 0,
                "total_fx_pnl": 0,
                "worst_var_95": 0,
                "active_currency_pairs": 0,
            },
            "top_tickers": [],
            "currency_summary": {},
        })

    timestamp = rows[0].get("timestamp")

    total_exposure = sum(safe_num(r.get("position_size")) for r in rows)
    total_pnl = sum(safe_num(r.get("fx_pnl")) for r in rows)

    worst_var = max(
        (safe_num(r.get("VaR_95_15m")) for r in rows),
        default=0
    )

    active_pairs = len({
        r.get("currency_pair")
        for r in rows
        if r.get("currency_pair")
    })

    top_tickers = sorted(
        rows,
        key=lambda r: safe_num(r.get("position_size")),
        reverse=True
    )[:5]

    grouped = defaultdict(list)
    currency_summary = {}

    for r in rows:
        currency = r.get("currency_pair")
        if currency:
            grouped[currency].append(r)

    for currency, items in grouped.items():
        currency_summary[currency] = {
            "total_exposure": sum(safe_num(r.get("position_size")) for r in items),
            "total_fx_pnl": sum(safe_num(r.get("fx_pnl")) for r in items),
            "worst_var_95": max(
                (safe_num(r.get("VaR_95_15m")) for r in items),
                default=0
            ),
            "ticker_count": len(items),
        }

    return JsonResponse({
        "timestamp": timestamp,
        "totals": {
            "total_exposure": total_exposure,
            "total_fx_pnl": total_pnl,
            "worst_var_95": worst_var,
            "active_currency_pairs": active_pairs,
        },
        "top_tickers": top_tickers,
        "currency_summary": currency_summary,
    })


def fx_currency_initial(request):

    currency = request.GET.get("currency")

    raw = redis_client.get("fx_latest_snapshot")
    if not raw:
        return JsonResponse({"error": "no snapshot available"}, status=404)

    try:
        rows = json.loads(raw)
    except Exception:
        return JsonResponse({"error": "invalid snapshot"}, status=500)

    filtered = [
        r for r in rows
        if r.get("currency_pair") == currency
    ]

    if not filtered:
        return JsonResponse({"error": "currency not found"}, status=404)

    total_exposure = sum(safe_num(r.get("position_size")) for r in filtered)
    total_pnl = sum(safe_num(r.get("fx_pnl")) for r in filtered)

    worst_var = max(
        (safe_num(r.get("VaR_95_15m")) for r in filtered),
        default=0
    )

    ticker_count = len(filtered)

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

    filtered.sort(
        key=lambda r: safe_num(r.get("position_size")),
        reverse=True
    )

    return JsonResponse({
        "currency": currency,
        "market": market,
        "totals": {
            "total_exposure": total_exposure,
            "total_fx_pnl": total_pnl,
            "worst_var_95": worst_var,
            "ticker_count": ticker_count,
        },
        "tickers": filtered[:5],
    })


def fx_ticker_initial(request):

    ticker = request.GET.get("ticker")

    raw = redis_client.get("fx_latest_snapshot")
    if not raw:
        return JsonResponse({"error": "no snapshot available"}, status=404)

    try:
        rows = json.loads(raw)
    except Exception:
        return JsonResponse({"error": "invalid snapshot"}, status=500)

    row = next(
        (r for r in rows if r.get("ticker") == ticker),
        None
    )

    if not row:
        return JsonResponse({"error": "ticker not found"}, status=404)

    position_size = safe_num(row.get("position_size"))
    fx_pnl = safe_num(row.get("fx_pnl"))
    var_95 = safe_num(row.get("VaR_95_15m"))

    payload = {
        "ticker": ticker,
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

    return JsonResponse(payload)


# ---------------------------
# 1. Equity Overview API
# ---------------------------
def equity_overview(request):

    raw = redis_client.get("equity_latest_snapshot")
    if not raw:
        return JsonResponse({"error": "no snapshot found"}, status=404)

    rows = json.loads(raw)
    if not rows:
        return JsonResponse({"error": "empty snapshot"}, status=404)

    timestamp = rows[0].get("timestamp")

    total_exposure = sum(safe_num(r.get("intraday_exposure")) for r in rows)
    intraday_pnl = sum(safe_num(r.get("portfolio_intraday_pnl")) for r in rows)

    tickers_count = len({r.get("ticker") for r in rows if r.get("ticker")})
    managers_count = len({r.get("asset_manager") for r in rows if r.get("asset_manager")})

    top_movers = sorted(
        rows,
        key=lambda r: abs(safe_num(r.get("intraday_pnl"))),
        reverse=True,
    )[:10]

    ticker_agg = {}
    for r in rows:
        t = r.get("ticker")
        if not t:
            continue
        ticker_agg.setdefault(t, {
            "ticker": t,
            "total_exposure": 0,
            "total_pnl": 0,
        })
        ticker_agg[t]["total_exposure"] += safe_num(r.get("intraday_exposure"))
        ticker_agg[t]["total_pnl"] += safe_num(r.get("portfolio_intraday_pnl"))

    top_tickers_agg = sorted(
        ticker_agg.values(),
        key=lambda x: abs(safe_num(x["total_exposure"])),
        reverse=True,
    )[:5]

    manager_agg = {}
    for r in rows:
        m = r.get("asset_manager")
        if not m:
            continue
        manager_agg.setdefault(m, {
            "manager": m,
            "total_exposure": 0,
            "total_pnl": 0,
            "tickers": set(),
        })
        manager_agg[m]["total_exposure"] += safe_num(r.get("intraday_exposure"))
        manager_agg[m]["total_pnl"] += safe_num(r.get("portfolio_intraday_pnl"))
        if r.get("ticker"):
            manager_agg[m]["tickers"].add(r["ticker"])

    top_managers_agg = [
        {
            "manager": m["manager"],
            "total_exposure": m["total_exposure"],
            "total_pnl": m["total_pnl"],
            "ticker_count": len(m["tickers"]),
        }
        for m in sorted(
            manager_agg.values(),
            key=lambda x: abs(safe_num(x["total_exposure"])),
            reverse=True,
        )[:5]
    ]

    alerts = []
    for r in rows:
        vol = r.get("vol_15m")
        ret1 = r.get("return_1m")
        exposure = safe_num(r.get("intraday_exposure"))

        if safe_num(vol) > 0.02:
            alerts.append({
                "type": "Volatility Spike",
                "ticker": r.get("ticker"),
                "manager": r.get("asset_manager"),
                "severity": "medium",
                "time": r.get("timestamp"),
                "trigger": "vol_15m > 0.02",
                "_exposure": exposure,
            })

        if abs(safe_num(ret1)) > 0.008:
            alerts.append({
                "type": "Return Shock",
                "ticker": r.get("ticker"),
                "manager": r.get("asset_manager"),
                "severity": "high" if abs(safe_num(ret1)) > 0.015 else "medium",
                "time": r.get("timestamp"),
                "trigger": "abs(return_1m) > 0.8%",
                "_exposure": exposure,
            })

    alerts = sorted(alerts, key=lambda x: x["_exposure"], reverse=True)[:10]
    for a in alerts:
        a.pop("_exposure", None)

    return JsonResponse({
        "timestamp": timestamp,
        "totals": {
            "total_exposure": total_exposure,
            "intraday_pnl": intraday_pnl,
            "tickers_streaming": tickers_count,
            "active_managers": managers_count,
        },
        "top_movers": top_movers,
        "top_tickers_agg": top_tickers_agg,
        "top_managers_agg": top_managers_agg,
        "active_alerts": alerts,
    })

# ---------------------------
# 2. Equity Manager API
# ---------------------------
def equity_manager(request):

    manager = request.GET.get("manager")
    if not manager:
        return JsonResponse({"error": "missing manager"}, status=400)

    raw = redis_client.get("equity_latest_snapshot")
    if not raw:
        return JsonResponse({"error": "no snapshot"}, status=404)

    rows = json.loads(raw)
    if not rows:
        return JsonResponse({"error": "empty snapshot"}, status=404)

    normalized_manager = manager.replace("-", " ").lower()

    filtered = [
        r for r in rows
        if r.get("asset_manager", "").replace("-", " ").lower() == normalized_manager
    ]

    if not filtered:
        return JsonResponse({"error": "manager not found"}, status=404)

    # ---------------- Aggregates ----------------
    exposure = sum(safe_num(r.get("intraday_exposure")) for r in filtered)
    pnl = sum(safe_num(r.get("portfolio_intraday_pnl")) for r in filtered)

    avg_r1m = sum(safe_num(r.get("return_1m")) for r in filtered) / len(filtered)
    avg_r5m = sum(safe_num(r.get("return_5m")) for r in filtered) / len(filtered)
    avg_vol = sum(safe_num(r.get("vol_15m")) for r in filtered) / len(filtered)

    portfolio_total_exposure = sum(
        safe_num(r.get("intraday_exposure")) for r in rows
    )
    weight = exposure / portfolio_total_exposure if portfolio_total_exposure else 0

    holdings = sorted(
        filtered,
        key=lambda r: safe_num(r.get("intraday_exposure")),
        reverse=True,
    )[:50]

    timestamp = rows[0].get("timestamp")

    # ---------------- Alerts ----------------
    alerts = []

    for r in filtered:
        vol = safe_num(r.get("vol_15m"))
        ret1 = safe_num(r.get("return_1m"))
        row_exposure = safe_num(r.get("intraday_exposure"))

        if vol > 0.02:
            alerts.append({
                "type": "Volatility Spike",
                "ticker": r.get("ticker"),
                "severity": "medium",
                "time": r.get("timestamp"),
                "trigger": "vol_15m > 0.02",
                "_exposure": row_exposure,
            })

        if abs(ret1) > 0.008:
            alerts.append({
                "type": "Return Shock",
                "ticker": r.get("ticker"),
                "severity": "high" if abs(ret1) > 0.015 else "medium",
                "time": r.get("timestamp"),
                "trigger": "abs(return_1m) > 0.8%",
                "_exposure": row_exposure,
            })

    alerts = sorted(alerts, key=lambda a: a["_exposure"], reverse=True)[:10]
    for a in alerts:
        a.pop("_exposure", None)

    return JsonResponse({
        "manager": manager,
        "timestamp": timestamp,
        "totals": {
            "intraday_exposure": exposure,
            "intraday_pnl": pnl,
            "return_1m": avg_r1m,
            "return_5m": avg_r5m,
            "vol_15m": avg_vol,
            "holdings_count": len(filtered),
            "weight": weight,
            "alerts": len(alerts),
        },
        "holdings": holdings,
        "alerts": alerts,
    })

# ---------------------------
# 3. Equity Ticker (fixed alerts)
# ---------------------------
def equity_ticker(request):
    ticker = request.GET.get("ticker")
    if not ticker:
        return JsonResponse({"error": "missing ticker"}, status=400)

    raw = redis_client.get("equity_latest_snapshot")
    if not raw:
        return JsonResponse({"error": "no snapshot"}, status=404)

    rows = json.loads(raw)
    if not rows:
        return JsonResponse({"error": "empty snapshot"}, status=404)

    # Filter rows for this ticker
    filtered = [r for r in rows if r.get("ticker") == ticker]
    if not filtered:
        return JsonResponse({"error": "ticker not found"}, status=404)

    # Portfolio aggregates
    total_exposure = sum(safe_num(r.get("intraday_exposure")) for r in filtered)
    total_pnl = sum(safe_num(r.get("portfolio_intraday_pnl")) for r in filtered)
    managers_count = len({r.get("asset_manager") for r in filtered if r.get("asset_manager")})
    portfolio_total_exposure = sum(safe_num(r.get("intraday_exposure")) for r in rows)
    portfolio_weight = total_exposure / portfolio_total_exposure if portfolio_total_exposure else 0

    fetched_market_cap = None
    try:
        yf_ticker = yf.Ticker(ticker)
        info = yf_ticker.info or {}
        fetched_market_cap = info.get("marketCap")
    except Exception:
        pass

    for r in filtered:
        r["marketCap"] = fetched_market_cap or r.get("marketCap")

    # Take first row for snapshot
    s = filtered[0]

    # Market snapshot
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

    # Fundamentals
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

    # Alerts
    alerts = []
    vol = safe_num(s.get("vol_15m"))
    ret1 = safe_num(s.get("return_1m"))

    if vol > 0.02:
        alerts.append({
            "type": "volatility_spike",
            "severity": "medium",
            "time": s.get("timestamp"),
            "trigger": "vol_15m > 0.02",
        })

    if abs(ret1) > 0.008:
        alerts.append({
            "type": "return_shock",
            "severity": "high" if abs(ret1) > 0.015 else "medium",
            "time": s.get("timestamp"),
            "trigger": "abs(return_1m) > 0.8%",
        })

    # Manager breakdown
    manager_breakdown = [
        {
            "manager": r.get("asset_manager"),
            "exposure": safe_num(r.get("intraday_exposure")),
            "pnl": safe_num(r.get("portfolio_intraday_pnl")),
            "weight": safe_num(r.get("intraday_exposure")) / total_exposure if total_exposure else 0,
        }
        for r in filtered
    ]

    # Final payload
    payload = {
        "ticker": ticker,
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

    return JsonResponse(payload)


def equity_ticker_manager(request):

    ticker = request.GET.get("ticker")
    manager = request.GET.get("manager")

    if not ticker or not manager:
        return JsonResponse({"error": "missing params"}, status=400)

    raw = redis_client.get("equity_latest_snapshot")
    if not raw:
        return JsonResponse({"error": "no snapshot"}, status=404)

    rows = json.loads(raw)
    if not rows:
        return JsonResponse({"error": "empty snapshot"}, status=404)

    normalized_manager = manager.replace("-", " ").lower()

    row = next(
        (
            r for r in rows
            if r.get("ticker") == ticker
            and r.get("asset_manager", "").replace("-", " ").lower() == normalized_manager
        ),
        None,
    )

    if not row:
        return JsonResponse({"error": "not found"}, status=404)

    alerts = []

    vol = safe_num(row.get("vol_15m"))
    ret1 = safe_num(row.get("return_1m"))

    if vol > 0.02:
        alerts.append({
            "type": "Volatility Spike",
            "severity": "medium",
            "time": row.get("timestamp"),
            "trigger": "vol_15m > 0.02",
        })

    if abs(ret1) > 0.008:
        alerts.append({
            "type": "Return Shock",
            "severity": "high" if abs(ret1) > 0.015 else "medium",
            "time": row.get("timestamp"),
            "trigger": "abs(return_1m) > 0.8%",
        })

    payload = {
        "timestamp": row.get("timestamp"),
        "ticker": row.get("ticker"),
        "manager": row.get("asset_manager"),
        "totals": {
            "exposure": safe_num(row.get("intraday_exposure")),
            "pnl": safe_num(row.get("portfolio_intraday_pnl")),
            "shares": safe_num(row.get("Shares")),
            "weight": (
                safe_num(row.get("intraday_exposure"))
                / (safe_num(row.get("portfolio_intraday_exposure")) or 1)
            ),
            "price": safe_num(row.get("close")),
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

    return JsonResponse(payload)
