from django.http import JsonResponse
from django.db.models import Sum, Avg, Count, F
from django.db.models.functions import Coalesce

from ..models import DerivativeData


def derivatives_overview(request):
    """
    Firm-Level Derivatives Risk — latest date only
    Aggregates across ALL trades
    """

    latest_date = (
        DerivativeData.objects
        .order_by("-date")
        .values_list("date", flat=True)
        .first()
    )

    qs = DerivativeData.objects.filter(date=latest_date)

    if not qs.exists():
        return JsonResponse({"error": "No derivative data found"}, status=404)

    summary = qs.aggregate(
        total_notional=Coalesce(Sum("notional"), 0.0),
        total_net_exposure=Coalesce(Sum("net_exposure"), 0.0),
        delta_equivalent=Coalesce(Sum("delta_equivalent_exposure"), 0.0),
        margin_call_amount=Coalesce(Sum("margin_call_amount"), 0.0),
        total_pnl=Coalesce(Sum("pnl"), 0.0),
        margin_call_count=Count("trade_id", filter=F("margin_call_flag")),
    )

    # firm margin stress flag
    summary["margin_stress"] = summary["margin_call_count"] > 0

    top_tickers = (
        qs.values("ticker")
        .annotate(
            notional=Coalesce(Sum("notional"), 0.0),
            net_exposure=Coalesce(Sum("net_exposure"), 0.0),
            delta_eq=Coalesce(Sum("delta_equivalent_exposure"), 0.0),
            pnl=Coalesce(Sum("pnl"), 0.0),
            margin_calls=Count("trade_id", filter=F("margin_call_flag")),
        )
        .order_by("-net_exposure")[:5]
    )

    by_asset_class = (
        qs.values("asset_class")
        .annotate(
            notional=Coalesce(Sum("notional"), 0.0),
            net_exposure=Coalesce(Sum("net_exposure"), 0.0),
            pnl=Coalesce(Sum("pnl"), 0.0),
        )
        .order_by("-notional")
    )

    return JsonResponse(
        {
            "date": latest_date,
            "summary": summary,
            "top_tickers": list(top_tickers),
            "by_asset_class": list(by_asset_class),
        }
    )


def derivatives_ticker_detail(request):
    ticker = request.GET.get("ticker")

    if not ticker:
        return JsonResponse({"error": "Missing ticker"}, status=400)

    ticker = ticker.upper()

    latest_date = (
        DerivativeData.objects
        .order_by("-date")
        .values_list("date", flat=True)
        .first()
    )

    qs = DerivativeData.objects.filter(date=latest_date, ticker=ticker)

    if not qs.exists():
        return JsonResponse(
            {"error": f"No derivative data for ticker {ticker}"},
            status=404,
        )

    summary = qs.aggregate(
        total_notional=Coalesce(Sum("notional"), 0.0),
        total_net_exposure=Coalesce(Sum("net_exposure"), 0.0),
        delta_equivalent=Coalesce(Sum("delta_equivalent_exposure"), 0.0),
        margin_call_amount=Coalesce(Sum("margin_call_amount"), 0.0),
        total_pnl=Coalesce(Sum("pnl"), 0.0),
        margin_call_count=Count("trade_id", filter=F("margin_call_flag")),
    )

    summary["margin_stress"] = summary["margin_call_count"] > 0

    asset_class_summary = (
        qs.values("asset_class")
        .annotate(
            notional=Coalesce(Sum("notional"), 0.0),
            net_exposure=Coalesce(Sum("net_exposure"), 0.0),
            delta_eq=Coalesce(Sum("delta_equivalent_exposure"), 0.0),
            margin_stress=Count("trade_id", filter=F("margin_call_flag")),
        )
        .order_by("-notional")
    )

    return JsonResponse(
        {
            "date": latest_date,
            "ticker": ticker,
            "summary": summary,
            "asset_class_summary": list(asset_class_summary),
        }
    )





def derivatives_ticker_asset_class(request):
    ticker = request.GET.get("ticker")
    asset_class = request.GET.get("asset_class")

    if not ticker:
        return JsonResponse({"error": "Missing ticker"}, status=400)

    if not asset_class:
        return JsonResponse({"error": "Missing asset_class"}, status=400)

    ticker = ticker.upper()

    latest_date = (
        DerivativeData.objects
        .order_by("-date")
        .values_list("date", flat=True)
        .first()
    )

    qs = DerivativeData.objects.filter(
        date=latest_date,
        ticker=ticker,
        asset_class=asset_class
    )

    if not qs.exists():
        return JsonResponse(
            {"error": f"No derivative data for {ticker} in {asset_class}"},
            status=404,
        )

    # -------- SUMMARY AT TICKER x ASSET CLASS LEVEL --------
    summary = qs.aggregate(
        total_notional=Coalesce(Sum("notional"), 0.0),
        total_net_exposure=Coalesce(Sum("net_exposure"), 0.0),
        delta_equivalent=Coalesce(Sum("delta_equivalent_exposure"), 0.0),
        margin_call_amount=Coalesce(Sum("margin_call_amount"), 0.0),
        total_pnl=Coalesce(Sum("pnl"), 0.0),
        margin_call_count=Count("trade_id", filter=F("margin_call_flag")),
    )

    summary["margin_stress"] = summary["margin_call_count"] > 0

    # -------- TRADE LEVEL INVENTORY --------
    trades = list(
        qs.values(
            "trade_id",
            "derivative_type",
            "notional",
            "net_exposure",
            "delta",
            "gamma",
            "vega",
            "delta_equivalent_exposure",
            "tenor_years",
            "margin_call_flag",
            "margin_call_amount",
            "pnl",
        )
        .order_by("-net_exposure")
    )

    return JsonResponse(
        {
            "date": latest_date,
            "ticker": ticker,
            "asset_class": asset_class,
            "summary": summary,
            "trades": trades,
        }
    )
