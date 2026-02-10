from django.http import JsonResponse
from django.db.models import Avg, Count
from django.db.models.functions import Coalesce

from ..models import BondData


def bond_overview(request):
    """
    Bond — Firm Overview (latest date)
    """

    latest_date = (
        BondData.objects
        .order_by("-date")
        .values_list("date", flat=True)
        .first()
    )

    qs = BondData.objects.filter(date=latest_date)

    # ---------------- SUMMARY ----------------
    summary = qs.aggregate(
        bond_count=Count("bond_id", distinct=True),
        ticker_count=Count("ticker", distinct=True),
        weighted_credit_spread=Avg("credit_spread"),
        weighted_ytm=Avg("yield_to_maturity"),
        weighted_implied_pd=Avg("implied_pd_annual"),
    )

    # ---------------- TOP 5 — DEFAULT RISK ----------------
    top_default_risk = list(
        qs.order_by("-implied_pd_annual")[:5]
        .values(
            "ticker",
            "bond_id",
            "credit_rating",
            "implied_pd_annual",
            "bond_price",
        )
    )

    # ---------------- TOP 5 — WIDEST SPREADS ----------------
    top_spreads = list(
        qs.order_by("-credit_spread")[:5]
        .values(
            "ticker",
            "bond_id",
            "credit_rating",
            "credit_spread",
            "yield_to_maturity",
        )
    )

    # ---------------- TOP 5 — LONGEST MATURITY ----------------
    top_maturity = list(
        qs.order_by("-maturity_years")[:5]
        .values(
            "ticker",
            "bond_id",
            "credit_rating",
            "maturity_years",
            "bond_price",
        )
    )

    return JsonResponse(
        {
            "date": latest_date,
            "summary": summary,
            "top_default_risk": top_default_risk,
            "top_spreads": top_spreads,
            "top_maturity": top_maturity,
        }
    )




def bond_ticker_detail(request):
    """
    Bond — Ticker Detail (latest date)
    Assumes one bond per ticker
    """

    ticker = request.GET.get("ticker")
    if not ticker:
        return JsonResponse(
            {"error": "Missing ticker parameter"},
            status=400,
        )

    ticker = ticker.upper()

    latest_date = (
        BondData.objects
        .order_by("-date")
        .values_list("date", flat=True)
        .first()
    )

    qs = BondData.objects.filter(
        date=latest_date,
        ticker=ticker,
    )

    if not qs.exists():
        return JsonResponse(
            {"error": f"No bond data found for ticker {ticker}"},
            status=404,
        )

    bond = qs.values(
        "bond_id",
        "ticker",
        "sector",
        "industry",
        "credit_rating",
        "coupon_rate",
        "issue_date",
        "maturity_date",
        "maturity_years",
        "bond_price",
        "yield_to_maturity",
        "benchmark_yield",
        "credit_spread",
        "implied_hazard",
        "implied_pd_annual",
        "implied_pd_multi_year",
        "pred_pd_21d",
        "DGS10",
        "DGS10_ma",
        "dgs10_anom",
        "gdp",
        "cpi",
        "unrate",
        "fedfunds",
        "pred_spread_5d",
        "market_synthetic_score",
        "implied_rating",
    ).first()

    return JsonResponse(
        {
            "date": latest_date,
            "bond": bond,
        }
    )
