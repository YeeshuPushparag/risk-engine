from django.http import JsonResponse
from django.db.models import Sum, Avg, Count, Q, F, FloatField, ExpressionWrapper, Max, Func, Value
from ..models import EquityData
from django.db.models.functions import NullIf, Coalesce
import yfinance as yf
import redis, json
import os

redis_client = redis.Redis(
    host=os.getenv("REDIS_HOST"),  
    port=int(os.getenv("REDIS_PORT", 6379)),   
    db=int(os.environ.get("REDIS_DB_STREAM", 1)),   
    decode_responses=True
)
# Global scenario dictionary
# ------------------------
SCENARIOS = {
    "baseline": {"ret_shift": 0.0, "vol_multiplier": 1.0, "var_multiplier": 1.0},
    "risk_on": {"ret_shift": 0.05, "vol_multiplier": 0.85, "var_multiplier": 0.80},
    "risk_off": {"ret_shift": -0.08, "vol_multiplier": 1.30, "var_multiplier": 1.40},
    "equity_rally": {"ret_shift": 0.10, "vol_multiplier": 0.75, "var_multiplier": 0.70},
    "equity_crash": {"ret_shift": -0.20, "vol_multiplier": 1.80, "var_multiplier": 1.60},
    "volatility_spike": {"ret_shift": -0.05, "vol_multiplier": 2.00, "var_multiplier": 1.80},
    "rates_up": {"ret_shift": -0.06, "vol_multiplier": 1.25, "var_multiplier": 1.30},
    "rates_down": {"ret_shift": 0.04, "vol_multiplier": 0.90, "var_multiplier": 0.85},
    "inflation_shock": {"ret_shift": -0.07, "vol_multiplier": 1.35, "var_multiplier": 1.40},
    "stagflation": {"ret_shift": -0.10, "vol_multiplier": 1.40, "var_multiplier": 1.30},
    "gfc_2008": {"ret_shift": -0.30, "vol_multiplier": 2.20, "var_multiplier": 2.00},
    "covid_2020": {"ret_shift": -0.25, "vol_multiplier": 2.00, "var_multiplier": 1.80},
}

def equity_latest_date(request):
    latest_date = (
        EquityData.objects
        .order_by("-date")
        .values_list("date", flat=True)
        .first()
    )

    return JsonResponse({
        "latest_date": latest_date
    })

def equity_status(request):
    latest_date = (
        EquityData.objects
        .order_by("-date")
        .values_list("date", flat=True)
        .first()
    )

    return JsonResponse({
        "latest_daily_date": latest_date,
        "intraday_enabled": False,  # later make dynamic
    })

def latest_asset_managers(request):
    latest_date = EquityData.objects.aggregate(
        latest=Max("date")
    )["latest"]

    managers = (
        EquityData.objects
        .filter(date=latest_date)
        .values_list("asset_manager", flat=True)
        .distinct()
        .order_by("asset_manager")
    )

    return JsonResponse(list(managers), safe=False)

def latest_tickers(request):
    # ---------------------------------
    # SOURCE OF TRUTH (ORDER MATTERS)
    # ---------------------------------
    req_type = request.GET.get("type")

    if not req_type:
        if "intraday" in request.path:
            req_type = "intraday"
        else:
            req_type = "daily"

    # -------------------------------
    # INTRADAY → REDIS
    # -------------------------------
    if req_type == "intraday":
        snapshot = redis_client.get("equity_latest_snapshot")

        if snapshot:
            try:
                data = json.loads(snapshot)
                tickers = sorted({
                    row.get("ticker")
                    for row in data
                    if row.get("ticker")
                })

                if tickers:
                    return JsonResponse(tickers, safe=False)

            except Exception:
                pass  # fallback to daily

    # -------------------------------
    # DAILY → DATABASE (FALLBACK)
    # -------------------------------
    latest_date = EquityData.objects.aggregate(
        latest=Max("date")
    )["latest"]

    tickers = (
        EquityData.objects
        .filter(date=latest_date)
        .values_list("ticker", flat=True)
        .distinct()
        .order_by("ticker")
    )

    return JsonResponse(list(tickers), safe=False)

   

# ------------------------------------------------
# SHARED STRESS PARSER (NO DUPLICATION)
# ------------------------------------------------
def _get_stress_params(request):
    scenario = request.GET.get("scenario")
    ret_p = request.GET.get("ret_shift")
    vol_p = request.GET.get("vol_multiplier")
    var_p = request.GET.get("var_multiplier")

    defaults = SCENARIOS.get(scenario, {})

    ret = float(ret_p) if ret_p is not None else defaults.get("ret_shift", 0.0)
    vol = float(vol_p) if vol_p is not None else defaults.get("vol_multiplier", 1.0)
    var = float(var_p) if var_p is not None else defaults.get("var_multiplier", 1.0)

    ret = max(min(ret, 0.30), -0.30)
    vol = max(min(vol, 3.0), 0.3)
    var = max(min(var, 5.0), 0.5)
    if scenario == "baseline":
        is_stressed = False
        return ret, vol, var, is_stressed, scenario
    
    else:
        is_stressed = bool(scenario or ret_p or vol_p or var_p)
        return ret, vol, var, is_stressed, scenario


# ------------------------------------------------
# 1️⃣ EQUITY OVERVIEW
# ------------------------------------------------
def equity_overview(request):
    date = request.GET.get("date")
    ret, vol, var, is_stressed, scenario = _get_stress_params(request)

    qs = EquityData.objects.filter(date=date)

    exposure = qs.aggregate(total=Sum("mtm_value"))["total"] or 0.0

    data = qs.aggregate(
        total_exposure=Sum("mtm_value"),
        daily_pnl=Sum("daily_pnl"),
        var_95=Sum(F("mtm_value") * F("daily_var_95")),
        var_99=Sum(F("mtm_value") * F("daily_var_99")),
        cvar_95=Sum(F("mtm_value") * F("daily_cvar_95")),
        cvar_99=Sum(F("mtm_value") * F("daily_cvar_99")),
        expected_return=Avg("daily_mu"),
        ex_ante_volatility=Avg("daily_sigma"),
        diversification_score=Avg("diversification_score"),
        hhi_sector=Avg("hhi_sector"),
    )

    if is_stressed:
        data["daily_pnl"] += exposure * ret
        data["expected_return"] += ret
        data["var_95"] *= var
        data["var_99"] *= var
        data["cvar_95"] *= var
        data["cvar_99"] *= var
        data["ex_ante_volatility"] *= vol

    data["manager_count"] = qs.values("asset_manager").distinct().count()
    data["ticker_count"] = qs.values("ticker").distinct().count()
    data["is_stressed"] = is_stressed

    return JsonResponse(data)


# ------------------------------------------------
# 2️⃣ MANAGERS
# ------------------------------------------------
def equity_managers(request):
    date = request.GET.get("date")
    ret, vol, var, is_stressed, scenario = _get_stress_params(request)

    total_exposure = (
        EquityData.objects
        .filter(date=date)
        .aggregate(total=Sum("mtm_value"))["total"]
        or 0.0
    )

    qs = (
        EquityData.objects
        .filter(date=date)
        .values("asset_manager")
        .annotate(
            exposure=Sum("mtm_value"),
            daily_pnl=Sum("daily_pnl"),
            book_pct=Sum("mtm_value") / total_exposure,
            ticker_count=Count("ticker", distinct=True),

            # ✅ VAR AS PERCENTAGE
            var_usage_pct=ExpressionWrapper(
                Sum(F("mtm_value") * F("daily_var_95")) /
                NullIf(Sum("mtm_value"), 0),
                output_field=FloatField()
            ),

            alerts=Count("id", filter=Q(portfolio_weight__gt=0.05)),
        )
        .order_by("-exposure")[:10]
    )

    data = []
    for row in qs:
        if is_stressed:
            row["daily_pnl"] += row["exposure"] * ret
            row["var_usage_pct"] *= var
        row["is_stressed"] = is_stressed
        data.append(row)

    return JsonResponse(data, safe=False)

# ------------------------------------------------
# 3️⃣ DAILY RISK DRIVERS
# ------------------------------------------------
def equity_daily_risk_drivers(request):
    date = request.GET.get("date")
    rtype = request.GET.get("type")
    ret, vol, var, is_stressed, scenario = _get_stress_params(request)

    qs = EquityData.objects.filter(date=date, mtm_value__gt=0)

    base = qs.values("ticker", "sector").annotate(
        aggregatedExposureUsd=Sum("mtm_value"),
        aggregatedDailyPnlUsd=Sum("daily_pnl"),
        varContributionUsd=Sum(F("mtm_value") * F("daily_var_95")),
        volContributionUsd=Sum(F("mtm_value") * F("volatility_21d")),
    )

    if rtype == "var":
        base = base.order_by("-varContributionUsd")[:5]
    elif rtype == "loss":
        base = base.order_by("aggregatedDailyPnlUsd")[:5]
    elif rtype == "exposure":
        base = base.order_by("-aggregatedExposureUsd")[:5]
    else:
        return JsonResponse({"error": "invalid type"}, status=400)

    data = list(base)
    for row in data:
        if is_stressed:
            row["aggregatedDailyPnlUsd"] += (
                row["aggregatedExposureUsd"] or 0.0
            ) * ret
            row["varContributionUsd"] *= var
            row["volContributionUsd"] *= vol

        row["is_stressed"] = is_stressed

    return JsonResponse(data, safe=False)


# ------------------------------------------------
# 4️⃣ ALERTS
# ------------------------------------------------
def equity_alerts(request):
    date = request.GET.get("date")
    ret, vol, var, is_stressed, scenario = _get_stress_params(request)

    alerts = []

    conc = EquityData.objects.filter(
        date=date, portfolio_weight__gt=0.05
    ).values("ticker", "asset_manager")[:5]

    for r in conc:
        alerts.append({
            "alert": "Concentration Limit Breach",
            "ticker": r["ticker"],
            "manager": r["asset_manager"],
            "severity": "High",
            "date": date,
            "is_stressed": is_stressed,
        })

    liq_threshold = 0.5 / vol if is_stressed else 0.5

    liq = EquityData.objects.filter(
        date=date, amihud_illiquidity__gt=liq_threshold
    ).values("ticker", "asset_manager")[:5]

    for r in liq:
        alerts.append({
            "alert": "Liquidity Risk Spike",
            "ticker": r["ticker"],
            "manager": r["asset_manager"],
            "severity": "Medium",
            "date": date,
            "is_stressed": is_stressed,
        })

    return JsonResponse(alerts, safe=False)

# ------------------------------------------------
# MANAGER OVERVIEW
# ------------------------------------------------
def equity_manager_overview(request):
    date = request.GET.get("date")
    manager = request.GET.get("manager")
    manager_normalized = manager.replace("-", " ") if manager else None

    if not date or not manager:
        return JsonResponse({"error": "date and manager required"}, status=400)

    ret, vol, var, is_stressed, scenario = _get_stress_params(request)

    total_exposure = EquityData.objects.filter(date=date).aggregate(
        total=Sum("mtm_value")
    )["total"] or 0.0

    qs = EquityData.objects.filter(
        date=date, asset_manager__iexact=manager_normalized
    )

    if not qs.exists():
        return JsonResponse({
            "exposure": 0,
            "portfolio_daily_pnl": 0,
            "portfolio_weight": 0,
            "daily_portfolio_var_95": 0,
            "daily_portfolio_var_99": 0,
            "daily_portfolio_cvar_95": 0,
            "daily_portfolio_cvar_99": 0,
            "tickers_count": 0,
            "alerts": 0,
            "is_stressed": is_stressed,
        })

    data = qs.aggregate(
        exposure=Sum("mtm_value"),
        portfolio_daily_pnl=Sum("daily_pnl"),
        portfolio_weight=Sum("mtm_value") / total_exposure,
        daily_portfolio_var_95=Sum(F("mtm_value") * F("daily_var_95")),
        daily_portfolio_var_99=Sum(F("mtm_value") * F("daily_var_99")),
        daily_portfolio_cvar_95=Sum(F("mtm_value") * F("daily_cvar_95")),
        daily_portfolio_cvar_99=Sum(F("mtm_value") * F("daily_cvar_99")),
    )

    if is_stressed:
        exposure = data["exposure"] or 0.0
        data["portfolio_daily_pnl"] += exposure * ret
        data["daily_portfolio_var_95"] *= var
        data["daily_portfolio_var_99"] *= var
        data["daily_portfolio_cvar_95"] *= var
        data["daily_portfolio_cvar_99"] *= var

    data["alerts"] = qs.filter(portfolio_weight__gt=0.05).count()
    data["tickers_count"] = qs.values("ticker").distinct().count()
    data["is_stressed"] = is_stressed

    return JsonResponse(data)


# ------------------------------------------------
# MANAGER HOLDINGS
# ------------------------------------------------
def equity_manager_holdings(request):
    date = request.GET.get("date")
    manager = request.GET.get("manager")
    manager_normalized = manager.replace("-", " ") if manager else None

    ret, vol, var, is_stressed, scenario = _get_stress_params(request)

    qs = (
        EquityData.objects
        .filter(date=date, asset_manager__iexact=manager_normalized)
        .values("ticker", "cusip", "sector")
        .annotate(
            exposure=Sum("mtm_value"),
            weight=Sum("portfolio_weight"),
            daily_pnl=Sum("daily_pnl"),
            var_usd=Sum(F("mtm_value") * F("daily_var_95")),
            turnover_ratio=Avg("turnover_ratio"),
        )
        .order_by("-exposure")[:20]
    )

    data = []
    for row in qs:
        exposure = row["exposure"] or 0.0

        if is_stressed:
            row["daily_pnl"] += exposure * ret
            row["var_usd"] *= var

        row["var_pct"] = (
            row["var_usd"] / exposure if exposure else 0.0
        )
        row["turnover_ratio"] = round(row["turnover_ratio"], 3)
        row["is_stressed"] = is_stressed

        data.append(row)

    return JsonResponse(data, safe=False)


# ------------------------------------------------
# MANAGER RISK CONTRIBUTORS
# ------------------------------------------------
def equity_manager_risk_contributors(request):
    date = request.GET.get("date")
    manager = request.GET.get("manager")
    manager_normalized = manager.replace("-", " ") if manager else None

    ret, vol, var, is_stressed, scenario = _get_stress_params(request)

    qs = (
        EquityData.objects
        .filter(
            date=date,
            asset_manager__iexact=manager_normalized,
            daily_var_95__isnull=False,
        )
        .values("ticker")
        .annotate(
            exposure=Sum("mtm_value"),
            var_usd=Sum(F("mtm_value") * F("daily_var_95")),
        )
        .order_by("-var_usd")[:5]
    )

    data = []
    for row in qs:
        if is_stressed:
            row["var_usd"] *= var
        row["is_stressed"] = is_stressed
        data.append(row)

    return JsonResponse(data, safe=False)


# ------------------------------------------------
# MANAGER ALERTS
# ------------------------------------------------
def equity_manager_alerts(request):
    date = request.GET.get("date")
    manager = request.GET.get("manager")
    manager_normalized = manager.replace("-", " ") if manager else None

    if not date or not manager:
        return JsonResponse({"error": "date and manager required"}, status=400)

    ret, vol, var, is_stressed, scenario = _get_stress_params(request)

    qs = (
        EquityData.objects
        .filter(
            date=date,
            asset_manager__iexact=manager_normalized,
            portfolio_weight__gt=0.05,
        )
        .values("ticker")
        .annotate(exposure=Sum("mtm_value"))
        .order_by("-exposure")[:10]
    )

    alerts = []
    for row in qs:
        alerts.append({
            "alert": "Concentration limit breach",
            "severity": "High",
            "ticker": row["ticker"],
            "exposure": row["exposure"],
            "date": date,
            "is_stressed": is_stressed,
        })

    return JsonResponse(alerts, safe=False)

# ------------------------------------------------
# 1️⃣ TICKER MARKET (NO PRICE STRESS)
# ------------------------------------------------
def equity_ticker_market(request):
    date = request.GET.get("date")
    ticker = request.GET.get("ticker")
    _, _, _, is_stressed, _ = _get_stress_params(request)

    row = (
        EquityData.objects
        .filter(date=date, ticker=ticker)
        .values(
            "date", "open", "high", "low", "close", "volume",
            "daily_return", "log_return",
            "high_low_spread", "close_open_diff",
        )
        .first()
    )

    prev_row = (
        EquityData.objects
        .filter(ticker=ticker, date__lt=date)
        .order_by("-date")
        .values(
            "date", "open", "high", "low", "close", "volume"
        )
        .first()
    )

    if row:
        row["is_stressed"] = is_stressed
        if prev_row:
            row.update({
                "prev_date": prev_row["date"],
                "prev_open": prev_row["open"],
                "prev_high": prev_row["high"],
                "prev_low": prev_row["low"],
                "prev_close": prev_row["close"],
                "prev_volume": prev_row["volume"],
            })
        else:
            row.update({
                "prev_date": None,
                "prev_open": None,
                "prev_high": None,
                "prev_low": None,
                "prev_close": None,
                "prev_volume": None,
            })

    return JsonResponse(row or {})


# ------------------------------------------------
# 2️⃣ TICKER RISK
# ------------------------------------------------
def equity_ticker_risk(request):
    date = request.GET.get("date")
    ticker = request.GET.get("ticker")
    ret, _, var, is_stressed, _ = _get_stress_params(request)

    data = (
        EquityData.objects
        .filter(date=date, ticker=ticker)
        .aggregate(
            beta=Max("beta"),
            daily_sigma=Max("daily_sigma"),
            daily_mu=Max("daily_mu"),
            daily_var_95=Sum(F("mtm_value") * F("daily_var_95")),
            daily_var_99=Sum(F("mtm_value") * F("daily_var_99")),
            daily_cvar_95=Sum(F("mtm_value") * F("daily_cvar_95")),
            daily_cvar_99=Sum(F("mtm_value") * F("daily_cvar_99")),
        )
    )

    if is_stressed:
        data["daily_mu"] = (data["daily_mu"] or 0.0) + ret
        data["daily_var_95"] *= var
        data["daily_var_99"] *= var
        data["daily_cvar_95"] *= var
        data["daily_cvar_99"] *= var

    data["is_stressed"] = is_stressed
    return JsonResponse(data)


# ------------------------------------------------
# 3️⃣ TICKER VOLATILITY
# ------------------------------------------------
def equity_ticker_volatility(request):
    date = request.GET.get("date")
    ticker = request.GET.get("ticker")
    _, vol, _, is_stressed, _ = _get_stress_params(request)

    row = (
        EquityData.objects
        .filter(date=date, ticker=ticker)
        .values(
            "vol_5d", "vol_20d", "volatility_21d",
            "ma_5", "ma_20",
            "momentum_5d", "momentum_20d",
            "avg_volume_10d", "vol_change",
            "downside_risk", "sharpe_ratio", "sortino_ratio",
        )
        .first()
    )

    if row and is_stressed:
        row["volatility_21d"] *= vol
        row["vol_5d"] *= vol
        row["vol_20d"] *= vol

    if row:
        row["is_stressed"] = is_stressed

    return JsonResponse(row or {})


# ------------------------------------------------
# 4️⃣ TICKER LIQUIDITY
# ------------------------------------------------
def equity_ticker_liquidity(request):
    date = request.GET.get("date")
    ticker = request.GET.get("ticker")
    _, vol, _, is_stressed, _ = _get_stress_params(request)

    row = (
        EquityData.objects
        .filter(date=date, ticker=ticker)
        .values(
            "amihud_illiquidity",
            "turnover_ratio",
            "liquidity_risk_score",
        )
        .first()
    )

    if row and is_stressed:
        row["liquidity_risk_score"] *= vol

    if row:
        row["is_stressed"] = is_stressed

    return JsonResponse(row or {})


# ------------------------------------------------
# 5️⃣ TICKER FUNDAMENTALS (NO STRESS)
# ------------------------------------------------
def equity_ticker_fundamentals(request):
    ticker = request.GET.get("ticker")
    _, _, _, is_stressed, _ = _get_stress_params(request)

    row = (
        EquityData.objects
        .filter(ticker=ticker)
        .values(
            "issuer_name", "class_name", "cusip",
            "sector", "industry",
            "market_cap",
            "total_assets", "total_debt",
            "revenue", "ebitda", "net_income",
            "ebitda_margin", "debt_to_assets", "debt_to_ebitda",
            "interest_coverage_proxy", "altman_z",
            "free_cash_flow", "cash_and_equivalents",
            "credit_rating",
        )
        .first()
    )

    fetched_market_cap = None
    try:
        yf_ticker = yf.Ticker(ticker)
        fetched_market_cap = yf_ticker.info.get("marketCap")
    except Exception:
        pass

    if row:
        row["market_cap"] = fetched_market_cap or row.get("market_cap")
        row["is_stressed"] = is_stressed

    return JsonResponse(row or {})


# ------------------------------------------------
# 6️⃣ TICKER AGGREGATE
# ------------------------------------------------
def equity_ticker_aggregate(request):
    date = request.GET.get("date")
    ticker = request.GET.get("ticker")
    ret, _, var, is_stressed, _ = _get_stress_params(request)

    qs = EquityData.objects.filter(date=date, ticker=ticker)

    total_exposure = EquityData.objects.filter(date=date).aggregate(
        total=Sum("mtm_value")
    )["total"] or 0.0

    data = qs.aggregate(
        exposureUsd=Sum("mtm_value"),
        dailyPnlUsd=Sum("daily_pnl"),
        portfolio_weight_pct=Sum("mtm_value") / total_exposure,
        beta_weighted=Sum("beta_weighted"),
        sector_beta_weighted=Sum("sector_beta_weighted"),
    )

    if is_stressed:
        data["dailyPnlUsd"] += (data["exposureUsd"] or 0.0) * ret
        data["beta_weighted"] *= var
        data["sector_beta_weighted"] *= var

    data["is_stressed"] = is_stressed
    return JsonResponse(data)

# ------------------------------------------------
# 7️⃣ TICKER → MANAGERS
# ------------------------------------------------
def equity_ticker_managers(request):
    date = request.GET.get("date")
    ticker = request.GET.get("ticker")
    ret, _, var, is_stressed, _ = _get_stress_params(request)

    rows = (
        EquityData.objects
        .filter(date=date, ticker=ticker)
        .values("asset_manager")
        .annotate(
            exposureUsd=Sum("mtm_value"),
            dailyPnlUsd=Sum("daily_pnl"),
            weightPct=Sum("portfolio_weight"),
        )
        .order_by("-exposureUsd")
    )

    data = []
    for r in rows:
        exposure = r["exposureUsd"] or 0.0

        if is_stressed:
            r["dailyPnlUsd"] += exposure * ret
            # ❗ weights are NOT stressed
        r["is_stressed"] = is_stressed
        data.append(r)

    return JsonResponse(data, safe=False)


# ------------------------------------------------
# POSITION SUMMARY
# ------------------------------------------------
def equity_position_summary(request):
    ticker = request.GET.get("ticker")
    manager = request.GET.get("manager")
    date = request.GET.get("date")

    ret, _, _, is_stressed, _ = _get_stress_params(request)

    qs = EquityData.objects.filter(
        ticker=ticker,
        asset_manager__iexact=manager,
    )
    if date:
        qs = qs.filter(date=date)

    if not qs.exists():
        return JsonResponse({})

    data = qs.aggregate(
        exposure_usd=Sum("mtm_value"),
        shares=Sum("shares"),
        daily_pnl_usd=Sum("daily_pnl"),
        portfolio_weight=Max("portfolio_weight"),
        current_price=Max("close"),
    )

    if is_stressed:
        data["daily_pnl_usd"] += (data["exposure_usd"] or 0.0) * ret

    data.update({
        "ticker": ticker,
        "manager": manager,
        "is_stressed": is_stressed,
    })

    return JsonResponse(data)


# ------------------------------------------------
# POSITION INTELLIGENCE
# ------------------------------------------------
def equity_position_intelligence(request):
    ticker = request.GET.get("ticker")
    manager = request.GET.get("manager")
    date = request.GET.get("date")

    ret, vol, var, is_stressed, _ = _get_stress_params(request)

    qs = EquityData.objects.filter(
        ticker=ticker,
        asset_manager__iexact=manager,
    )
    if date:
        qs = qs.filter(date=date)

    row = qs.aggregate(
        pred_ret_1d=Max("pred_ret_1d"),
        pred_ret_5d=Max("pred_ret_5d"),
        pred_ret_21d=Max("pred_ret_21d"),
        pred_vol_21d=Max("pred_vol_21d"),
        pred_downside_21d=Max("pred_downside_21d"),
        pred_var_21d=Max("pred_var_21d"),
        pred_factor_21d=Max("pred_factor_21d"),

        pred_port_ret_1d=Max("pred_port_ret_1d"),
        pred_port_ret_5d=Max("pred_port_ret_5d"),
        pred_port_ret_21d=Max("pred_port_ret_21d"),
        pred_port_var_1d=Max("pred_port_var_1d"),
        pred_port_var_5d=Max("pred_port_var_5d"),
        pred_port_var_21d=Max("pred_port_var_21d"),

        sector_exposure_pct=Max("sector_exposure_pct"),
        beta_weighted=Max("beta_weighted"),
        sector_beta_weighted=Max("sector_beta_weighted"),
        pred_sector_rotation=Max("pred_sector_rotation"),
        pred_macro_regime=Max("pred_macro_regime"),

        exposure_usd=Max("mtm_value"),
    )

    exposure = row["exposure_usd"]
    if not exposure:
        return JsonResponse({})

    if is_stressed:
        row["pred_ret_1d"] += ret
        row["pred_ret_5d"] += ret
        row["pred_ret_21d"] += ret
        row["pred_vol_21d"] *= vol
        row["pred_var_21d"] *= var

    row["expected_pnl_1d"] = exposure * (row["pred_ret_1d"] or 0.0)
    row["expected_pnl_5d"] = exposure * (row["pred_ret_5d"] or 0.0)
    row["expected_pnl_21d"] = exposure * (row["pred_ret_21d"] or 0.0)

    row["expected_var_contribution_21d"] = row["pred_var_21d"]
    row["var_contribution_pct"] = (
        row["pred_var_21d"] / exposure if exposure else None
    )

    row["is_stressed"] = is_stressed
    return JsonResponse(row)


# ------------------------------------------------
# POSITION ALERTS
# ------------------------------------------------
def equity_position_alerts(request):
    ticker = request.GET.get("ticker")
    manager = request.GET.get("manager")
    date = request.GET.get("date")

    _, vol, var, is_stressed, _ = _get_stress_params(request)

    qs = EquityData.objects.filter(
        ticker=ticker,
        asset_manager__iexact=manager,
    )
    if date:
        qs = qs.filter(date=date)

    if not qs.exists():
        return JsonResponse([], safe=False)

    row = qs.aggregate(
        exposure=Max("mtm_value"),
        portfolio_weight=Max("portfolio_weight"),
        pred_var_21d=Max("pred_var_21d"),
        pred_factor_21d=Max("pred_factor_21d"),
        pred_sector_rotation=Max("pred_sector_rotation"),
        pred_macro_regime=Max("pred_macro_regime"),
        amihud=Max("amihud_illiquidity"),
    )

    alerts = []

    if row["portfolio_weight"] and row["portfolio_weight"] > 0.05:
        alerts.append({
            "type": "Concentration",
            "severity": "High",
            "message": "Position exceeds 5% of portfolio",
        })

    stressed_var = (row["pred_var_21d"] or 0.0) * (var if is_stressed else 1.0)
    if stressed_var > 0.10 * (row["exposure"] or 0.0):
        alerts.append({
            "type": "Risk",
            "severity": "High",
            "message": "VaR contribution exceeds 10% of exposure",
        })

    if row["pred_factor_21d"] is not None and row["pred_factor_21d"] < -0.05:
        alerts.append({
            "type": "Model",
            "severity": "Medium",
            "message": "Negative factor signal",
        })

    if row["pred_sector_rotation"] is not None and row["pred_sector_rotation"] < -0.02:
        alerts.append({
            "type": "Sector",
            "severity": "Medium",
            "message": "Sector rotating out",
        })

    if row["pred_macro_regime"] == 0:
        alerts.append({
            "type": "Macro",
            "severity": "Medium",
            "message": "Risk-Off macro regime",
        })

    for a in alerts:
        a["is_stressed"] = is_stressed

    return JsonResponse(alerts, safe=False)
