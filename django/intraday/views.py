"""
views.py
========
REST views for the equity and FX dashboards.

Design principle (per product spec): portfolio valuation and market-data
freshness are two different concepts and must never be conflated.

    "What is my portfolio worth?"           -> valuation (all-tickers/all-pairs
                                                Redis store, stale included)
    "How complete/recent is the data?"      -> freshness (is_stale flag on
                                                that same store)

Redis stores, by role (see equity-consumer.py / fx-consumer.py for how
these are produced):

    EQUITY_ALL_KEY    "equity_latest_snapshot"          - every ticker ever
                                                           seen, own latest
                                                           row, tagged
                                                           is_stale. THE
                                                           valuation source.
    EQUITY_SYNCED_KEY "equity_latest_snapshot_synced"   - only tickers that
                                                           are new-this-cycle,
                                                           fresh, and floored
                                                           to the same
                                                           minute. Never
                                                           stale by
                                                           construction.

    FX_SYNCED_KEY     "fx_latest_snapshot"              - FX's synced store
                                                           (same semantics as
                                                           equity's synced,
                                                           pre-existing key
                                                           name).
    FX_ALL_KEY        "fx_latest_snapshot_all"           - FX's all-pairs
                                                           store (same
                                                           semantics as
                                                           equity's all-
                                                           tickers, tagged
                                                           is_stale).

Per-page Redis usage (confirmed with the user):
    equity_overview        : ALL + SYNCED
    equity_manager          : ALL + SYNCED
    equity_ticker            : ALL only
    equity_ticker_manager    : ALL only
    fx_overview_initial      : ALL + SYNCED
    fx_currency_initial      : ALL + SYNCED (assumed symmetric with
                                equity_manager; FX has no "manager" concept,
                                fx_currency is its aggregation-across-tickers
                                equivalent -- confirm/correct if wrong)
    fx_ticker_initial        : ALL only

One view per page. No new views added. Each view returns a single combined
JSON payload containing everything that page needs.

Open item (explicitly a stub, not implemented): the "Missing Market Data"
section's EOD fallback price. Nothing in the current pipeline stores EOD
data. Every missing-ticker entry below includes eod_price/eod_date/
eod_available placeholder fields so the response shape is ready, but they
are always null/false until a real EOD source is wired in.
"""

from django.http import JsonResponse
from django.core.cache import cache
import redis, json, math, os, io
import boto3
import pandas as pd
import pendulum
import yfinance as yf
from collections import defaultdict

# =============================================================
# REDIS
# =============================================================

redis_client = redis.Redis(
    host=os.getenv("REDIS_HOST"),
    port=int(os.getenv("REDIS_PORT", 6379)),
    db=int(os.environ.get("REDIS_DB_STREAM", 1)),
    decode_responses=True
)

EQUITY_ALL_KEY    = "equity_latest_snapshot"
EQUITY_SYNCED_KEY = "equity_latest_snapshot_synced"
FX_SYNCED_KEY     = "fx_latest_snapshot"
FX_ALL_KEY        = "fx_latest_snapshot_all"

# FX has no separate universe file -- the 7 currency pairs ARE the FX
# universe (must match fx-producer.py CONFIG["currency_pairs"] exactly).
# There is no ticker-level FX universe source, so per-ticker FX coverage
# (fx_currency page) stays self-referential, same as before.
FX_CURRENCY_PAIRS = [
    "USDEUR", "USDJPY", "USDCAD",
    "USDCHF", "USDGBP", "USDAUD", "USDCNY",
]

UNIVERSE_CACHE_TTL_S = 300  # static universes rarely change; refresh every 5 min


def _get_redis_rows(key: str) -> list:
    """Fetch and JSON-decode a Redis snapshot key. Never raises."""
    raw = redis_client.get(key)
    if not raw:
        return []
    try:
        rows = json.loads(raw)
        return rows if isinstance(rows, list) else []
    except Exception:
        return []


def safe_num(x):
    try:
        if x is None:
            return 0.0
        if isinstance(x, float) and math.isnan(x):
            return 0.0
        return float(x)
    except Exception:
        return 0.0


# =============================================================
# STATIC UNIVERSE  (never Redis -- per design doc Rule 1)
# =============================================================
# Same bucket/folder the equity producer already treats as source-of-
# truth (see equity-producer.py CONFIG["read_bucket"] /
# CONFIG["ticker_key"]) -- Django reads the identical S3 objects rather
# than keeping a separate baked-into-the-image copy that could silently
# drift from what the producer is actually streaming.

EQUITY_UNIVERSE_BUCKET     = "pushpa-equity-bucket"
EQUITY_TICKERS_KEY         = "historical-equity/tickers50.csv"
EQUITY_TICKER_MANAGER_KEY  = "historical-equity/tickers50_asset_manager.csv"


def _get_s3():
    return boto3.client("s3")


def _find_col(df: pd.DataFrame, candidates: list) -> str:
    """Case-insensitive column lookup -- CSV header casing isn't guaranteed."""
    cols_lower = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand.lower() in cols_lower:
            return cols_lower[cand.lower()]
    raise KeyError(f"None of {candidates} found in columns {list(df.columns)}")


def load_equity_universe_tickers():
    """
    Return the full equity ticker universe from S3 -- the same file
    (pushpa-equity-bucket / historical-equity/tickers50.csv) the
    producer already reads as its own source-of-truth ticker list.
    Cached; returns None on failure so callers can degrade gracefully
    instead of showing a wrong count.
    """
    cached = cache.get("equity_universe_tickers")
    if cached is not None:
        return cached
    try:
        obj = _get_s3().get_object(
            Bucket=EQUITY_UNIVERSE_BUCKET,
            Key=EQUITY_TICKERS_KEY,
        )
        body = obj["Body"].read().decode("utf-8")
        df = pd.read_csv(io.StringIO(body))
        ticker_col = _find_col(df, ["ticker", "symbol"])
        tickers = sorted(
            df[ticker_col].dropna().astype(str).str.strip().str.upper().unique().tolist()
        )
        cache.set("equity_universe_tickers", tickers, timeout=UNIVERSE_CACHE_TTL_S)
        return tickers
    except Exception:
        return None


def load_equity_ticker_manager_map():
    """
    Return {manager_name: [tickers this manager holds, per the static
    file]} from S3 (same bucket/folder as the ticker universe file
    above: pushpa-equity-bucket / historical-equity/
    tickers50_asset_manager.csv). Cached; returns None on failure.

    This gives equity_manager a REAL, independent universe per manager
    (previously it could only fall back to "whatever tickers already
    have a Redis row for this manager," which could never detect a
    ticker that had gone completely missing -- this fixes that gap).

    Column names are matched case-insensitively against a small set of
    likely headers; adjust the candidate lists in _find_col calls below
    if the actual CSV uses different column names.
    """
    cached = cache.get("equity_ticker_manager_map")
    if cached is not None:
        return cached
    try:
        obj = _get_s3().get_object(
            Bucket=EQUITY_UNIVERSE_BUCKET,
            Key=EQUITY_TICKER_MANAGER_KEY,
        )
        body = obj["Body"].read().decode("utf-8")
        df = pd.read_csv(io.StringIO(body))
        ticker_col  = _find_col(df, ["ticker", "symbol"])
        manager_col = _find_col(df, ["asset_manager", "manager", "assetmanager"])

        df[ticker_col]  = df[ticker_col].astype(str).str.strip().str.upper()
        df[manager_col] = df[manager_col].astype(str).str.strip()

        manager_to_tickers = defaultdict(set)
        for _, row in df.iterrows():
            manager_to_tickers[row[manager_col]].add(row[ticker_col])

        result = {m: sorted(ts) for m, ts in manager_to_tickers.items()}
        cache.set("equity_ticker_manager_map", result, timeout=UNIVERSE_CACHE_TTL_S)
        return result
    except Exception:
        return None


# =============================================================
# EOD FALLBACK -- STUB ONLY, no data source wired up yet
# =============================================================

def _eod_placeholder(ticker: str) -> dict:
    """
    No EOD data source exists in the current pipeline. This returns a
    fixed shape (all null/false) so the frontend can build against the
    final schema now; swap the body for a real lookup later without
    changing anything downstream.
    """
    return {
        "eod_price":     None,
        "eod_date":      None,
        "eod_available": False,
    }


# =============================================================
# COVERAGE / FRESHNESS  (shared by every "ALL" page)
# =============================================================

def _dedupe_by_ticker(rows: list, ticker_field: str = "ticker") -> dict:
    """
    Snapshot rows can be one-per-position (equity: ticker x manager) or
    one-per-fanned-ticker (FX: pair x exposed ticker) -- multiple rows
    can share the same ticker with identical market-data fields
    (price/timestamp/is_stale) but different position fields. Returns
    one representative row per ticker for market-data purposes only.
    """
    out = {}
    for r in rows:
        t = r.get(ticker_field)
        if t and t not in out:
            out[t] = r
    return out


def _coverage_and_freshness(rows: list, universe_tickers, ticker_field: str = "ticker") -> dict:
    """
    rows: the ALL-store rows (has is_stale) for this page's scope.
    universe_tickers: authoritative ticker list, or None if the static
        universe source was unavailable (falls back to "whatever is
        in Redis", flagged via universe_known=False).
    """
    by_ticker = _dedupe_by_ticker(rows, ticker_field)
    present = set(by_ticker.keys())

    universe_known = universe_tickers is not None
    universe = list(universe_tickers) if universe_known else sorted(present)
    universe_set = set(universe)

    valued_tickers  = sorted(universe_set & present)
    missing_tickers = sorted(universe_set - present)

    fresh_tickers = [t for t in valued_tickers if not by_ticker[t].get("is_stale", False)]
    stale_tickers = [t for t in valued_tickers if by_ticker[t].get("is_stale", False)]

    last_update = None
    timestamps = [by_ticker[t].get("timestamp") for t in valued_tickers if by_ticker[t].get("timestamp")]
    if timestamps:
        try:
            last_update = max(pendulum.parse(ts) for ts in timestamps).to_iso8601_string()
        except Exception:
            last_update = max(timestamps)

    return {
        "universe_total":  len(universe),
        "valued_count":    len(valued_tickers),
        "missing_tickers": missing_tickers,
        "fresh_count":     len(fresh_tickers),
        "stale_count":     len(stale_tickers),
        "last_market_update": last_update,
        "universe_known":  universe_known,
        "by_ticker":       by_ticker,
    }


def _basis_note(valued: int, total: int, label: str = "valued tickers") -> str:
    if valued == total:
        return None
    return f"Based on {valued}/{total} {label}"


def _missing_instruments(
    missing_items: list,
    field_name: str = "ticker",
    reason: str = "No latest market price available",
    include_eod: bool = True,
) -> list:
    """
    field_name="ticker" for equity/per-ticker FX pages.
    field_name="currency_pair" for the FX overview page (universe is
    the 7 currency pairs, not tickers) -- EOD is an equity concept with
    no FX-pair equivalent, so include_eod=False there.
    """
    out = []
    for item in missing_items:
        entry = {field_name: item, "reason": reason}
        if include_eod:
            entry.update(_eod_placeholder(item))
        out.append(entry)
    return out


# =============================================================
# EQUITY: OVERVIEW  (ALL + SYNCED)
# =============================================================

def build_equity_overview_payload(all_rows: list, synced_rows: list, universe_tickers) -> dict:
    """
    Pure function: rows in, payload dict out. No Django/HTTP, no Redis
    I/O. Shared by equity_overview() (REST) and EquityOverviewConsumer
    (WS) so both surfaces are always guaranteed to agree.
    """
    coverage = _coverage_and_freshness(all_rows, universe_tickers, ticker_field="ticker")

    # Sums are naturally scoped to whatever tickers are actually present
    # in `all_rows` -- a missing ticker contributes nothing, which is
    # the correct behavior (can't value what isn't there), not a bug.
    total_exposure = sum(safe_num(r.get("intraday_exposure")) for r in all_rows)

    # portfolio_intraday_pnl is already pre-summed PER MANAGER upstream;
    # summing it across every row would double count. Sum unique managers.
    seen_managers = {}
    for r in all_rows:
        m = r.get("asset_manager")
        if m and m not in seen_managers:
            seen_managers[m] = safe_num(r.get("portfolio_intraday_pnl"))
    intraday_pnl   = sum(seen_managers.values())
    active_managers = len(seen_managers)

    top_movers = sorted(
        all_rows, key=lambda r: abs(safe_num(r.get("intraday_pnl"))), reverse=True
    )[:10]

    ticker_agg = {}
    for r in all_rows:
        t = r.get("ticker")
        if not t:
            continue
        ticker_agg.setdefault(t, {"ticker": t, "total_exposure": 0, "total_pnl": 0})
        ticker_agg[t]["total_exposure"] += safe_num(r.get("intraday_exposure"))
        ticker_agg[t]["total_pnl"]      += safe_num(r.get("intraday_pnl"))
    top_tickers_agg = sorted(
        ticker_agg.values(), key=lambda x: abs(safe_num(x["total_exposure"])), reverse=True
    )[:5]

    manager_agg = {}
    for r in all_rows:
        m = r.get("asset_manager")
        if not m:
            continue
        manager_agg.setdefault(m, {"manager": m, "total_exposure": 0, "total_pnl": 0, "tickers": set()})
        manager_agg[m]["total_exposure"] += safe_num(r.get("intraday_exposure"))
        manager_agg[m]["total_pnl"]      += safe_num(r.get("intraday_pnl"))
        if r.get("ticker"):
            manager_agg[m]["tickers"].add(r["ticker"])
    top_managers_agg = [
        {
            "manager": m["manager"],
            "total_exposure": m["total_exposure"],
            "total_pnl": m["total_pnl"],
            "ticker_count": len(m["tickers"]),
        }
        for m in sorted(manager_agg.values(), key=lambda x: abs(safe_num(x["total_exposure"])), reverse=True)[:5]
    ]

    alerts = []
    for r in all_rows:
        vol      = safe_num(r.get("vol_15m"))
        ret1     = safe_num(r.get("return_1m"))
        exposure = safe_num(r.get("intraday_exposure"))
        if vol > 0.02:
            alerts.append({
                "type": "Volatility Spike", "ticker": r.get("ticker"), "manager": r.get("asset_manager"),
                "severity": "medium", "time": r.get("timestamp"), "trigger": "vol_15m > 0.02",
                "_exposure": exposure,
            })
        if abs(ret1) > 0.008:
            alerts.append({
                "type": "Return Shock", "ticker": r.get("ticker"), "manager": r.get("asset_manager"),
                "severity": "high" if abs(ret1) > 0.015 else "medium",
                "time": r.get("timestamp"), "trigger": "abs(return_1m) > 0.8%",
                "_exposure": exposure,
            })
    alerts = sorted(alerts, key=lambda x: x["_exposure"], reverse=True)[:10]
    for a in alerts:
        a.pop("_exposure", None)

    valued = coverage["valued_count"]
    total  = coverage["universe_total"]

    return {
        "timestamp": coverage["last_market_update"],
        "portfolio_overview": {
            "total_tickers":      total,
            "total_exposure":     total_exposure,
            "total_pnl":          intraday_pnl,
            "active_managers":    active_managers,
            "valuation_coverage": {"valued": valued, "total": total, "unit": "tickers"},
            "basis_note":         _basis_note(valued, total),
        },
        "market_data_health": {
            "fresh_updates":          {"fresh": coverage["fresh_count"], "total": total},
            "last_market_update":     coverage["last_market_update"],
            "delayed_tickers_count":  coverage["stale_count"],
            "universe_source_available": coverage["universe_known"],
        },
        "data_quality_issues": _missing_instruments(coverage["missing_tickers"]),
        "top_movers":          top_movers,
        "top_tickers_agg":     top_tickers_agg,
        "top_managers_agg":    top_managers_agg,
        "active_alerts":       alerts,
        "synced_snapshot":     synced_rows,
    }


def equity_overview(request):
    all_rows    = _get_redis_rows(EQUITY_ALL_KEY)
    synced_rows = _get_redis_rows(EQUITY_SYNCED_KEY)
    universe_tickers = load_equity_universe_tickers()
    return JsonResponse(build_equity_overview_payload(all_rows, synced_rows, universe_tickers))


# =============================================================
# EQUITY: MANAGER  (ALL + SYNCED)
# =============================================================

def build_equity_manager_payload(all_rows: list, synced_rows: list, manager: str):
    """
    Pure function. Returns None only if the manager is unknown entirely
    (not in Redis AND not in the static ticker<->manager mapping file).
    If the manager is known via the static file but currently has zero
    priced tickers, this still returns a payload showing 0/N coverage
    rather than treating it as "not found."
    """
    normalized_manager = manager.replace("-", " ").lower()

    filtered = [
        r for r in all_rows
        if r.get("asset_manager", "").replace("-", " ").lower() == normalized_manager
    ]

    # Real, independent universe for this manager from the static
    # ticker<->manager CSV -- not just "whatever already has a Redis
    # row," so a ticker that's gone completely missing for this
    # manager can actually be detected.
    manager_map = load_equity_ticker_manager_map()
    manager_universe = None
    if manager_map:
        for m_name, tickers in manager_map.items():
            if m_name.replace("-", " ").lower() == normalized_manager:
                manager_universe = tickers
                break

    if not filtered and manager_universe is None:
        return None

    filtered_tickers = sorted({r["ticker"] for r in filtered if r.get("ticker")})
    synced_filtered  = [r for r in synced_rows if r.get("ticker") in filtered_tickers]

    # Fall back to the self-referential set only if the static mapping
    # file is unavailable/doesn't know this manager -- same caveat as
    # before applies only in that degraded case.
    coverage_universe = manager_universe if manager_universe is not None else filtered_tickers
    coverage = _coverage_and_freshness(filtered, coverage_universe, ticker_field="ticker")

    exposure = sum(safe_num(r.get("intraday_exposure")) for r in filtered)
    # portfolio_intraday_pnl is already pre-summed for this manager
    # upstream -- every row in `filtered` shares the same value.
    pnl = safe_num(filtered[0].get("portfolio_intraday_pnl")) if filtered else 0.0

    count   = len(filtered)
    avg_r1m = sum(safe_num(r.get("return_1m")) for r in filtered) / count if count else 0
    avg_r5m = sum(safe_num(r.get("return_5m")) for r in filtered) / count if count else 0
    avg_vol = sum(safe_num(r.get("vol_15m"))   for r in filtered) / count if count else 0

    portfolio_total_exposure = sum(safe_num(r.get("intraday_exposure")) for r in all_rows)
    weight = exposure / portfolio_total_exposure if portfolio_total_exposure else 0

    holdings = sorted(filtered, key=lambda r: safe_num(r.get("intraday_exposure")), reverse=True)[:50]

    alerts = []
    for r in filtered:
        vol           = safe_num(r.get("vol_15m"))
        ret1          = safe_num(r.get("return_1m"))
        row_exposure  = safe_num(r.get("intraday_exposure"))
        if vol > 0.02:
            alerts.append({
                "type": "Volatility Spike", "ticker": r.get("ticker"), "severity": "medium",
                "time": r.get("timestamp"), "trigger": "vol_15m > 0.02", "_exposure": row_exposure,
            })
        if abs(ret1) > 0.008:
            alerts.append({
                "type": "Return Shock", "ticker": r.get("ticker"),
                "severity": "high" if abs(ret1) > 0.015 else "medium",
                "time": r.get("timestamp"), "trigger": "abs(return_1m) > 0.8%", "_exposure": row_exposure,
            })
    alerts = sorted(alerts, key=lambda a: a["_exposure"], reverse=True)[:10]
    for a in alerts:
        a.pop("_exposure", None)

    valued = coverage["valued_count"]
    total  = coverage["universe_total"]

    return {
        "manager":   manager,
        "timestamp": coverage["last_market_update"],
        "portfolio_overview": {
            "total_tickers":      total,
            "total_exposure":     exposure,
            "total_pnl":          pnl,
            "return_1m":          avg_r1m,
            "return_5m":          avg_r5m,
            "vol_15m":            avg_vol,
            "weight":             weight,
            "holdings_count":     count,
            "valuation_coverage": {"valued": valued, "total": total, "unit": "tickers"},
            "basis_note":         _basis_note(valued, total),
        },
        "market_data_health": {
            "fresh_updates":         {"fresh": coverage["fresh_count"], "total": total},
            "last_market_update":    coverage["last_market_update"],
            "delayed_tickers_count": coverage["stale_count"],
        },
        "data_quality_issues": _missing_instruments(coverage["missing_tickers"]),
        "holdings":            holdings,
        "alerts":              alerts,
        "synced_snapshot":     synced_filtered,
    }


def equity_manager(request):
    manager = request.GET.get("manager")
    if not manager:
        return JsonResponse({"error": "missing manager"}, status=400)

    all_rows    = _get_redis_rows(EQUITY_ALL_KEY)
    synced_rows = _get_redis_rows(EQUITY_SYNCED_KEY)

    payload = build_equity_manager_payload(all_rows, synced_rows, manager)
    if payload is None:
        return JsonResponse({"error": "manager not found"}, status=404)
    return JsonResponse(payload)


# =============================================================
# EQUITY: TICKER  (ALL only)
# =============================================================

def build_equity_ticker_payload(all_rows: list, ticker: str) -> dict:
    """
    Pure function. Returns a dict always; sets "_not_found": True when
    the ticker has no current row. Callers (REST view, WS consumer)
    decide what to do with that marker -- REST turns it into a 404 and
    strips the marker, WS sends the payload as-is (frontend can render
    the "missing market data" state live) after stripping it too.
    """
    filtered = [r for r in all_rows if r.get("ticker") == ticker]

    if not filtered:
        # Ticker exists in the universe conceptually but has no current
        # Redis row at all -- still respond with the documented shape
        # rather than nothing, so the frontend can render the
        # "missing market data" ticker page directly from this response.
        return {
            "ticker": ticker,
            "market_data_status": {
                "intraday_price_available": False,
                "valuation_status": "Excluded from portfolio totals",
                **_eod_placeholder(ticker),
            },
            "_not_found": True,
        }

    total_exposure = sum(safe_num(r.get("intraday_exposure")) for r in filtered)
    total_pnl      = sum(safe_num(r.get("portfolio_intraday_pnl")) for r in filtered)
    managers_count = len({r.get("asset_manager") for r in filtered if r.get("asset_manager")})
    portfolio_total_exposure = sum(safe_num(r.get("intraday_exposure")) for r in all_rows)
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

    s = filtered[0]

    market = {
        "open": s.get("open"), "high": s.get("high"), "low": s.get("low"), "close": s.get("close"),
        "volume": s.get("volume"),
        "prev_open": s.get("prev_open"), "prev_high": s.get("prev_high"),
        "prev_low": s.get("prev_low"), "prev_close": s.get("prev_close"),
        "prev_volume": s.get("prev_volume"),
        "return_1m": s.get("return_1m"), "return_5m": s.get("return_5m"), "vol_15m": s.get("vol_15m"),
        "range_pct_1m": s.get("range_pct_1m"),
        "rolling_vwap_5m": s.get("rolling_vwap_5m"), "rolling_high_5m": s.get("rolling_high_5m"),
        "rolling_low_5m": s.get("rolling_low_5m"), "trend_slope_5m": s.get("trend_slope_5m"),
        "breakout_strength": s.get("breakout_strength"), "volume_burst": s.get("volume_burst"),
        "timestamp": s.get("timestamp"),
    }

    fundamentals = {
        "issuer_name": s.get("issuer_name"), "class_name": s.get("class"), "cusip": s.get("CUSIP"),
        "sector": s.get("sector"), "industry": s.get("industry"),
        "marketCap": s.get("marketCap"), "totalAssets": s.get("totalAssets"), "totalDebt": s.get("totalDebt"),
        "revenue": s.get("revenue"), "ebitda": s.get("ebitda"), "net_income": s.get("netIncome"),
        "ebitda_margin": s.get("ebitda_margin"), "debt_to_assets": s.get("debt_to_assets"),
        "debt_to_ebitda": s.get("debt_to_ebitda"),
    }

    alerts = []
    vol  = safe_num(s.get("vol_15m"))
    ret1 = safe_num(s.get("return_1m"))
    if vol > 0.02:
        alerts.append({"type": "volatility_spike", "severity": "medium", "time": s.get("timestamp"), "trigger": "vol_15m > 0.02"})
    if abs(ret1) > 0.008:
        alerts.append({"type": "return_shock", "severity": "high" if abs(ret1) > 0.015 else "medium",
                        "time": s.get("timestamp"), "trigger": "abs(return_1m) > 0.8%"})

    manager_breakdown = [
        {
            "manager": r.get("asset_manager"),
            "exposure": safe_num(r.get("intraday_exposure")),
            "pnl": safe_num(r.get("portfolio_intraday_pnl")),
            "weight": safe_num(r.get("intraday_exposure")) / total_exposure if total_exposure else 0,
        }
        for r in filtered
    ]

    is_stale = bool(s.get("is_stale", False))

    return {
        "ticker": ticker,
        "totals": {
            "intraday_exposure": total_exposure,
            "intraday_pnl":      total_pnl,
            "portfolio_weight":  portfolio_weight,
            "managers_count":    managers_count,
        },
        "market":               market,
        "fundamentals":         fundamentals,
        "alerts":               alerts,
        "manager_breakdown":    manager_breakdown,
        "market_data_status": {
            "intraday_price_available": True,
            "is_stale":         is_stale,
            "last_update":      s.get("timestamp"),
            "valuation_status": "Included in portfolio totals",
            **_eod_placeholder(ticker),
        },
    }


def equity_ticker(request):
    ticker = request.GET.get("ticker")
    if not ticker:
        return JsonResponse({"error": "missing ticker"}, status=400)

    all_rows = _get_redis_rows(EQUITY_ALL_KEY)
    payload = build_equity_ticker_payload(all_rows, ticker)
    not_found = payload.pop("_not_found", False)
    return JsonResponse(payload, status=404 if not_found else 200)


# =============================================================
# EQUITY: TICKER + MANAGER  (ALL only)
# =============================================================

def build_equity_ticker_manager_payload(all_rows: list, ticker: str, manager: str) -> dict:
    normalized_manager = manager.replace("-", " ").lower()

    row = next(
        (
            r for r in all_rows
            if r.get("ticker") == ticker
            and r.get("asset_manager", "").replace("-", " ").lower() == normalized_manager
        ),
        None,
    )

    if not row:
        return {
            "ticker": ticker,
            "manager": manager,
            "market_data_status": {
                "intraday_price_available": False,
                "valuation_status": "Excluded from portfolio totals",
                **_eod_placeholder(ticker),
            },
            "_not_found": True,
        }

    alerts = []
    vol  = safe_num(row.get("vol_15m"))
    ret1 = safe_num(row.get("return_1m"))
    if vol > 0.02:
        alerts.append({"type": "Volatility Spike", "severity": "medium", "time": row.get("timestamp"), "trigger": "vol_15m > 0.02"})
    if abs(ret1) > 0.008:
        alerts.append({"type": "Return Shock", "severity": "high" if abs(ret1) > 0.015 else "medium",
                        "time": row.get("timestamp"), "trigger": "abs(return_1m) > 0.8%"})

    return {
        "timestamp": row.get("timestamp"),
        "ticker":    row.get("ticker"),
        "manager":   row.get("asset_manager"),
        "totals": {
            "exposure": safe_num(row.get("intraday_exposure")),
            "pnl":      safe_num(row.get("portfolio_intraday_pnl")),
            "shares":   safe_num(row.get("Shares")),
            "weight": (
                safe_num(row.get("intraday_exposure"))
                / (safe_num(row.get("portfolio_intraday_exposure")) or 1)
            ),
            "price": safe_num(row.get("close")),
        },
        "signals": {
            "return_1m": row.get("return_1m"), "return_5m": row.get("return_5m"),
            "vol_15m": row.get("vol_15m"), "range_pct_1m": row.get("range_pct_1m"),
            "vwap_5m": row.get("rolling_vwap_5m"), "close_diff": row.get("close_diff"),
            "rolling_high": row.get("rolling_high_5m"), "rolling_low": row.get("rolling_low_5m"),
            "trend_slope_5m": row.get("trend_slope_5m"), "breakout_strength": row.get("breakout_strength"),
            "volume_burst": row.get("volume_burst"),
        },
        "alerts": alerts,
        "market_data_status": {
            "intraday_price_available": True,
            "is_stale":         bool(row.get("is_stale", False)),
            "last_update":      row.get("timestamp"),
            "valuation_status": "Included in portfolio totals",
            **_eod_placeholder(ticker),
        },
    }


def equity_ticker_manager(request):
    ticker  = request.GET.get("ticker")
    manager = request.GET.get("manager")
    if not ticker or not manager:
        return JsonResponse({"error": "missing params"}, status=400)

    all_rows = _get_redis_rows(EQUITY_ALL_KEY)
    payload = build_equity_ticker_manager_payload(all_rows, ticker, manager)
    not_found = payload.pop("_not_found", False)
    return JsonResponse(payload, status=404 if not_found else 200)


# =============================================================
# EQUITY: TICKER LIST  (for search/autocomplete -- static universe)
# =============================================================

def tickers_list(request):
    """
    Full equity ticker universe, for the TickerSearch autocomplete on
    the equity dashboard. Reuses load_equity_universe_tickers(), which
    is already cached (5 min TTL) -- this endpoint doesn't add any new
    I/O cost of its own.
    """
    tickers = load_equity_universe_tickers()
    if tickers is None:
        return JsonResponse(
            {"error": "Unable to load ticker universe"},
            status=500,
        )
    return JsonResponse(tickers, safe=False)


# =============================================================
# FX: OVERVIEW  (ALL + SYNCED)
# =============================================================

def build_fx_overview_payload(all_rows: list, synced_rows: list, currency_pairs: list) -> dict:
    """
    currency_pairs: the static FX universe (FX_CURRENCY_PAIRS) -- the
    true priceable unit in FX is the pair, not the ~thousands of
    fanned-out exposure tickers, so coverage/freshness here is computed
    per pair. A single missing pair can affect hundreds of tickers at
    once; per-pair coverage reflects the actual root cause instead of
    a diffuse per-ticker count.
    """
    coverage = _coverage_and_freshness(all_rows, currency_pairs, ticker_field="currency_pair")

    total_exposure = sum(safe_num(r.get("position_size")) for r in all_rows)
    total_pnl      = sum(safe_num(r.get("fx_pnl")) for r in all_rows)
    worst_var      = max((safe_num(r.get("VaR_95_15m")) for r in all_rows), default=0)

    top_tickers = sorted(all_rows, key=lambda r: safe_num(r.get("position_size")), reverse=True)[:5]

    grouped = defaultdict(list)
    for r in all_rows:
        ccy = r.get("currency_pair")
        if ccy:
            grouped[ccy].append(r)

    currency_summary = {}
    for currency, items in grouped.items():
        currency_summary[currency] = {
            "total_exposure": sum(safe_num(r.get("position_size")) for r in items),
            "total_fx_pnl":   sum(safe_num(r.get("fx_pnl")) for r in items),
            "worst_var_95":   max((safe_num(r.get("VaR_95_15m")) for r in items), default=0),
            "ticker_count":   len(items),
        }

    valued = coverage["valued_count"]
    total  = coverage["universe_total"]

    return {
        "timestamp": coverage["last_market_update"],
        "portfolio_overview": {
            "total_currency_pairs":  total,
            "total_exposure":        total_exposure,
            "total_fx_pnl":          total_pnl,
            "worst_var_95":          worst_var,
            "valuation_coverage":    {"valued": valued, "total": total, "unit": "currency_pairs"},
            "basis_note":            _basis_note(valued, total, label="valued currency pairs"),
        },
        "market_data_health": {
            "fresh_updates":             {"fresh": coverage["fresh_count"], "total": total},
            "last_market_update":        coverage["last_market_update"],
            "delayed_pairs_count":       coverage["stale_count"],
            "universe_source_available": coverage["universe_known"],
        },
        "data_quality_issues": _missing_instruments(
            coverage["missing_tickers"],
            field_name="currency_pair",
            reason="No latest price available for this currency pair",
            include_eod=False,
        ),
        "top_tickers":         top_tickers,
        "currency_summary":    currency_summary,
        "synced_snapshot":     synced_rows,
    }


def fx_overview_initial(request):
    all_rows    = _get_redis_rows(FX_ALL_KEY)
    synced_rows = _get_redis_rows(FX_SYNCED_KEY)
    return JsonResponse(build_fx_overview_payload(all_rows, synced_rows, FX_CURRENCY_PAIRS))


# =============================================================
# FX: CURRENCY  (ALL + SYNCED -- assumed symmetric with equity_manager)
# =============================================================

def build_fx_currency_payload(all_rows: list, synced_rows: list, currency: str):
    """Pure function. Returns None if the currency pair has no rows present."""
    filtered = [r for r in all_rows if r.get("currency_pair") == currency]
    if not filtered:
        return None

    filtered_tickers = sorted({r["ticker"] for r in filtered if r.get("ticker")})
    synced_filtered  = [r for r in synced_rows if r.get("ticker") in filtered_tickers]

    # Same caveat as equity_manager: no separate "this pair's mandate"
    # universe file exists here, so coverage is computed against the
    # tickers already present for this pair, not an independent source.
    coverage = _coverage_and_freshness(filtered, filtered_tickers, ticker_field="ticker")

    total_exposure = sum(safe_num(r.get("position_size")) for r in filtered)
    total_pnl      = sum(safe_num(r.get("fx_pnl")) for r in filtered)
    worst_var      = max((safe_num(r.get("VaR_95_15m")) for r in filtered), default=0)
    ticker_count   = len(filtered)

    s = filtered[0]
    market = {
        "open": s.get("open"), "high": s.get("high"), "low": s.get("low"), "close": s.get("close"),
        "prev_open": s.get("prev_open"), "prev_high": s.get("prev_high"),
        "prev_low": s.get("prev_low"), "prev_close": s.get("prev_close"),
        "timestamp": s.get("timestamp"),
    }

    filtered_sorted = sorted(filtered, key=lambda r: safe_num(r.get("position_size")), reverse=True)

    valued = coverage["valued_count"]
    total  = coverage["universe_total"]

    return {
        "currency":  currency,
        "timestamp": coverage["last_market_update"],
        "market":    market,
        "portfolio_overview": {
            "total_tickers":      total,
            "total_exposure":     total_exposure,
            "total_fx_pnl":       total_pnl,
            "worst_var_95":       worst_var,
            "ticker_count":       ticker_count,
            "valuation_coverage": {"valued": valued, "total": total, "unit": "tickers"},
            "basis_note":         _basis_note(valued, total),
        },
        "market_data_health": {
            "fresh_updates":         {"fresh": coverage["fresh_count"], "total": total},
            "last_market_update":    coverage["last_market_update"],
            "delayed_tickers_count": coverage["stale_count"],
        },
        "data_quality_issues": _missing_instruments(coverage["missing_tickers"]),
        "tickers":             filtered_sorted[:5],
        "synced_snapshot":     synced_filtered,
    }


def fx_currency_initial(request):
    currency = request.GET.get("currency")
    if not currency:
        return JsonResponse({"error": "missing currency"}, status=400)

    all_rows    = _get_redis_rows(FX_ALL_KEY)
    synced_rows = _get_redis_rows(FX_SYNCED_KEY)

    payload = build_fx_currency_payload(all_rows, synced_rows, currency)
    if payload is None:
        return JsonResponse({"error": "currency not found"}, status=404)
    return JsonResponse(payload)


# =============================================================
# FX: TICKER  (ALL only)
# =============================================================

def build_fx_ticker_payload(all_rows: list, ticker: str) -> dict:
    row = next((r for r in all_rows if r.get("ticker") == ticker), None)

    if not row:
        return {
            "ticker": ticker,
            "market_data_status": {
                "intraday_price_available": False,
                "valuation_status": "Excluded from portfolio totals",
                **_eod_placeholder(ticker),
            },
            "_not_found": True,
        }

    position_size = safe_num(row.get("position_size"))
    fx_pnl        = safe_num(row.get("fx_pnl"))
    var_95        = safe_num(row.get("VaR_95_15m"))

    return {
        "ticker":        ticker,
        "currency_pair": row.get("currency_pair"),
        "timestamp":     row.get("timestamp"),
        "open": row.get("open"), "high": row.get("high"), "low": row.get("low"), "close": row.get("close"),
        "prev_close": row.get("prev_close"),
        "fx_return_1m": row.get("fx_return_1m"), "fx_return_5m": row.get("fx_return_5m"),
        "fx_vol_15m": row.get("fx_vol_15m"),
        "position_size": position_size, "fx_pnl": fx_pnl, "VaR_95_15m": var_95,
        "totals": {
            "total_exposure": position_size,
            "total_fx_pnl":   fx_pnl,
            "worst_var_95":   var_95,
        },
        "market_data_status": {
            "intraday_price_available": True,
            "is_stale":         bool(row.get("is_stale", False)),
            "last_update":      row.get("timestamp"),
            "valuation_status": "Included in portfolio totals",
            **_eod_placeholder(ticker),
        },
    }


def fx_ticker_initial(request):
    ticker = request.GET.get("ticker")
    if not ticker:
        return JsonResponse({"error": "missing ticker"}, status=400)

    all_rows = _get_redis_rows(FX_ALL_KEY)
    payload = build_fx_ticker_payload(all_rows, ticker)
    not_found = payload.pop("_not_found", False)
    return JsonResponse(payload, status=404 if not_found else 200)