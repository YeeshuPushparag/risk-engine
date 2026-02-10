from django.http import JsonResponse
from django.db.models import Sum, Min, Avg, Max
from django.db.models.functions import Coalesce

from ..models import FXData





def fx_overview(request):
    TOP_TICKERS = 10
    TOP_CURRENCIES = 7
    TOP_ROWS = 10
    """
    FX — Firm Risk Overview (latest date, limited payload)
    """

    latest_date = (
        FXData.objects
        .order_by("-date")
        .values_list("date", flat=True)
        .first()
    )

    qs = FXData.objects.filter(date=latest_date)

    # ---------------- SUMMARY ----------------
    summary = qs.aggregate(
        total_exposure=Coalesce(Sum("exposure_amount"), 0.0),
        total_pnl=Coalesce(Sum("total_pnl"), 0.0),
        worst_var_95=Min("VaR_95"),
        worst_var_99=Min("VaR_99"),
        avg_volatility=Avg("fx_volatility"),
    )

    summary["ticker_count"] = qs.values("ticker").distinct().count()
    summary["currency_count"] = qs.values("currency_pair").distinct().count()

    # ---------------- RAW ROWS (LIMITED) ----------------
    rows = list(
        qs.order_by("-exposure_amount")[:TOP_ROWS]
        .values(
            "ticker",
            "currency_pair",
            "exposure_amount",
            "total_pnl",
            "VaR_95",
            "VaR_99",
        )
    )

    # ---------------- BY TICKER (TOP N) ----------------
    ticker_qs = (
        qs.values("ticker")
        .annotate(
            exposure=Coalesce(Sum("exposure_amount"), 0.0),
            pnl=Coalesce(Sum("total_pnl"), 0.0),
            VaR95=Min("VaR_95"),
            VaR99=Min("VaR_99"),
        )
        .order_by("-exposure")[:TOP_TICKERS]
    )

    ticker_currency_map = {}
    for r in qs.values("ticker", "currency_pair"):
        ticker_currency_map.setdefault(r["ticker"], set()).add(r["currency_pair"])

    by_ticker = [
        {
            **t,
            "currency_count": len(ticker_currency_map.get(t["ticker"], [])),
        }
        for t in ticker_qs
    ]

    # ---------------- BY CURRENCY (TOP N) ----------------
    currency_qs = (
        qs.values("currency_pair")
        .annotate(
            exposure=Coalesce(Sum("exposure_amount"), 0.0),
            pnl=Coalesce(Sum("total_pnl"), 0.0),
            VaR95=Min("VaR_95"),
            VaR99=Min("VaR_99"),
        )
        .order_by("-exposure")[:TOP_CURRENCIES]
    )

    currency_ticker_map = {}
    for r in qs.values("currency_pair", "ticker"):
        currency_ticker_map.setdefault(r["currency_pair"], set()).add(r["ticker"])

    by_currency = [
        {
            **c,
            "ticker_count": len(currency_ticker_map.get(c["currency_pair"], [])),
        }
        for c in currency_qs
    ]

    return JsonResponse(
        {
            "date": latest_date,
            "summary": summary,
            "rows": rows,
            "by_ticker": by_ticker,
            "by_currency": by_currency,
        }
    )




def fx_ticker_detail(request):
    """
    FX — Ticker Detail (latest date)
    One row per currency_pair
    VaR is USD
    """
    ticker = request.GET.get("ticker")
    if not ticker:
        return JsonResponse(
            {"error": "Missing ticker"},
            status=400
        )
    ticker = ticker.upper()


    latest_date = (
        FXData.objects
        .order_by("-date")
        .values_list("date", flat=True)
        .first()
    )

    qs = FXData.objects.filter(
        date=latest_date,
        ticker=ticker,
        is_warmup=False,
    )

    if not qs.exists():
        return JsonResponse(
            {"error": f"No FX data found for ticker {ticker}"},
            status=404,
        )

    # ---------------- ROWS (PER CURRENCY) ----------------
    rows = list(
        qs.values(
            "ticker",
            "sector",
            "industry",
            "currency_pair",
            "foreign_revenue_ratio",
            "fx_rate",
            "fx_return",
            "fx_volatility",
            "fx_volatility_20d",
            "fx_volatility_30d",
            "predicted_volatility_21d",
            "interest_diff",
            "carry_daily",
            "return_carry_adj",
            "position_size",
            "hedge_ratio",
            "exposure_amount",
            "fx_pnl",
            "carry_pnl",
            "total_pnl",
            "expected_pnl",
            "VaR_95",
            "VaR_99",
            "sharpe_like_ratio",
            "is_warmup",
            "gdp",
            "unrate",
            "fedfunds",
            "cpi",
        )
    )

    # ---------------- SUMMARY ----------------
    summary = qs.aggregate(
        total_exposure=Coalesce(Sum("exposure_amount"), 0.0),
        total_pnl=Coalesce(Sum("total_pnl"), 0.0),

        # VaR stored as USD → use MAX for worst loss
        worst_var_95=Coalesce(Max("VaR_95"), 0.0),
        worst_var_99=Coalesce(Max("VaR_99"), 0.0),
    )

    # ---------------- INSTRUMENT PROFILE ----------------
    profile = {
        "ticker": ticker,
        "sector": rows[0]["sector"],
        "industry": rows[0]["industry"],
        "foreign_revenue_ratio": rows[0]["foreign_revenue_ratio"],
    }

    return JsonResponse(
        {
            "date": latest_date,
            "ticker": ticker,
            "profile": profile,
            "summary": summary,
            "rows": rows,
        }
    )





def fx_currency_detail(request):
    """
    FX — Currency Pair Detail (latest date)
    One row per ticker
    VaR in USD
    """

    currency = request.GET.get("currency")
 
    if not currency:
        return JsonResponse(
            {"error": "Missing currency parameter"},
            status=400,
        )

    currency = currency.upper()

    latest_date = (
        FXData.objects
        .order_by("-date")
        .values_list("date", flat=True)
        .first()
    )

    qs = FXData.objects.filter(
        date=latest_date,
        currency_pair=currency,
        is_warmup=False,
    )

    if not qs.exists():
        return JsonResponse(
            {"error": f"No FX data found for currency {currency}"},
            status=404,
        )

    # ---------------- ROWS (PER TICKER) ----------------
    rows = list(
        qs.values(
            "ticker",
            "sector",
            "industry",
            "currency_pair",
            "foreign_revenue_ratio",
            "fx_rate",
            "fx_return",
            "volume",
            "fx_volatility",
            "fx_volatility_20d",
            "fx_volatility_30d",
            "predicted_volatility_21d",
            "interest_diff",
            "carry_daily",
            "return_carry_adj",
            "position_size",
            "hedge_ratio",
            "exposure_amount",
            "fx_pnl",
            "carry_pnl",
            "total_pnl",
            "expected_pnl",
            "VaR_95",
            "VaR_99",
            "value_at_risk",
            "sharpe_like_ratio",
            "is_warmup",
            "gdp",
            "unrate",
            "fedfunds",
            "cpi",
            "date",
        )
    )

    # ---------------- SUMMARY ----------------
    summary = qs.aggregate(
        total_exposure=Coalesce(Sum("exposure_amount"), 0.0),
        total_pnl=Coalesce(Sum("total_pnl"), 0.0),
        worst_var_95=Coalesce(Max("VaR_95"), 0.0),
        worst_var_99=Coalesce(Max("VaR_99"), 0.0),
    )

    return JsonResponse(
        {
            "date": latest_date,
            "currency_pair": currency,
            "summary": summary,
            "rows": rows,
        }
    )




def fx_currency_detail(request):
    """
    FX — Currency Pair Detail (latest date)
    One row per ticker
    Top 20 tickers by exposure
    VaR in USD
    """

    TOP_TICKERS = 20
    currency = request.GET.get("currency")

    if not currency:
        return JsonResponse(
            {"error": "Missing currency parameter"},
            status=400,
        )

    currency = currency.upper()

    latest_date = (
        FXData.objects
        .order_by("-date")
        .values_list("date", flat=True)
        .first()
    )

    qs = FXData.objects.filter(
        date=latest_date,
        currency_pair=currency,
        is_warmup=False,
    )

    if not qs.exists():
        return JsonResponse(
            {"error": f"No FX data found for currency {currency}"},
            status=404,
        )

    # ---------------- ROWS (TOP 20 BY EXPOSURE) ----------------
    rows = list(
        qs.order_by("-exposure_amount")[:TOP_TICKERS]
        .values(
            "ticker",
            "sector",
            "industry",
            "currency_pair",
            "foreign_revenue_ratio",
            "fx_rate",
            "fx_return",
            "volume",
            "fx_volatility",
            "fx_volatility_20d",
            "fx_volatility_30d",
            "predicted_volatility_21d",
            "interest_diff",
            "carry_daily",
            "return_carry_adj",
            "position_size",
            "hedge_ratio",
            "exposure_amount",
            "fx_pnl",
            "carry_pnl",
            "total_pnl",
            "expected_pnl",
            "VaR_95",
            "VaR_99",
            "value_at_risk",
            "sharpe_like_ratio",
            "is_warmup",
            "gdp",
            "unrate",
            "fedfunds",
            "cpi",
            "date",
        )
    )

    # ---------------- SUMMARY (FULL UNIVERSE) ----------------
    summary = qs.aggregate(
        total_exposure=Coalesce(Sum("exposure_amount"), 0.0),
        total_pnl=Coalesce(Sum("total_pnl"), 0.0),
        worst_var_95=Coalesce(Max("VaR_95"), 0.0),
        worst_var_99=Coalesce(Max("VaR_99"), 0.0),
    )

    return JsonResponse(
        {
            "date": latest_date,
            "currency_pair": currency,
            "summary": summary,
            "rows": rows,
            "meta": {
                "limit": TOP_TICKERS,
                "sorted_by": "exposure_amount_desc",
            },
        }
    )
