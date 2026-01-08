from django.db.models import Sum, Avg, Count, Q, F
from django.db.models.functions import Coalesce
from django.http import JsonResponse
from ..models import CollateralModelData, CollateralData




def collateral_overview(request):
    """
    Firm-level collateral snapshot
    Uses CollateralModelData (aggregated model exposure)
    """

    latest_date = (
        CollateralModelData.objects
        .order_by("-date")
        .values_list("date", flat=True)
        .first()
    )

    qs = CollateralModelData.objects.filter(date=latest_date)

    # ----------- SUMMARY -----------
    summary = qs.aggregate(
        total_exposure=Coalesce(Sum("exposure_before_collateral"), 0.0),
        total_net_exposure=Coalesce(Sum("net_exposure"), 0.0),
        total_required=Coalesce(Sum("required_collateral"), 0.0),
        total_effective=Coalesce(Sum("effective_collateral"), 0.0),
        total_margin_call=Coalesce(Sum("margin_call_amount"), 0.0),
        margin_call_count=Count("id", filter=Q(margin_call_flag=True)),
    )

    summary["margin_stress"] = summary["margin_call_count"] > 0

    # ----------- ASSET CLASS SUMMARY -----------
    asset_class_summary = list(
        qs.values("asset_class")
        .annotate(
            exposure=Coalesce(Sum("exposure_before_collateral"), 0.0),
            required=Coalesce(Sum("required_collateral"), 0.0),
            effective=Coalesce(Sum("effective_collateral"), 0.0),
            net_exposure=Coalesce(Sum("net_exposure"), 0.0),
            avg_ratio=Coalesce(Avg("collateral_ratio"), 0.0),
            margin_stress=Count("id", filter=Q(margin_call_flag=True)),
        )
        .order_by("-exposure")
    )

    # ----------- TICKER / COUNTERPARTY RISK -----------
    ticker_counterparty = list(
        qs.values("ticker", "counterparty")
        .annotate(
            exposure=Coalesce(Sum("exposure_before_collateral"), 0.0),
            net_exposure=Coalesce(Sum("net_exposure"), 0.0),
            margin_call=Coalesce(Sum("margin_call_amount"), 0.0),
            stress=Count("id", filter=Q(margin_call_flag=True)),
        )
        .order_by("-net_exposure")[:10]
    )

    return JsonResponse({
        "date": latest_date,
        "summary": summary,
        "asset_class_summary": asset_class_summary,
        "ticker_counterparty": ticker_counterparty,
    })







def collateral_ticker_detail(request):
    ticker = request.GET.get("ticker")

    if not ticker:
        return JsonResponse({"error": "Missing ticker"}, status=400)

    ticker = ticker.upper()

    latest_date = (
        CollateralData.objects
        .order_by("-date")
        .values_list("date", flat=True)
        .first()
    )

    qs = CollateralData.objects.filter(date=latest_date, ticker=ticker)

    if not qs.exists():
        return JsonResponse(
            {"error": f"No collateral data for {ticker}"},
            status=404,
        )

    # -------- SUMMARY --------
    summary = qs.aggregate(
        total_exposure=Coalesce(Sum("exposure_before_collateral"), 0.0),
        total_required=Coalesce(Sum("required_collateral"), 0.0),
        total_effective=Coalesce(Sum("effective_collateral"), 0.0),
        total_net_exposure=Coalesce(Sum("net_exposure"), 0.0),
        total_margin_call=Coalesce(Sum("margin_call_amount"), 0.0),
        asset_class_count=Count("asset_class", distinct=True),
        margin_call_count=Count("id", filter=Q(margin_call_flag=True)),
    )

    summary["margin_stress"] = summary["margin_call_count"] > 0

    # -------- PER ASSET CLASS BREAKDOWN --------
    asset_breakdown = list(
        qs.values("asset_class")
        .annotate(
            exposure=Coalesce(Sum("exposure_before_collateral"), 0.0),
            required=Coalesce(Sum("required_collateral"), 0.0),
            effective=Coalesce(Sum("effective_collateral"), 0.0),
            net_exposure=Coalesce(Sum("net_exposure"), 0.0),
            collateral_ratio=Coalesce(Avg("collateral_ratio"), 0.0),
            margin_stress=Count("id", filter=Q(margin_call_flag=True)),
        )
        .order_by("-exposure")
    )

    # -------- Macro Context (take last or avg) --------
    macro = qs.aggregate(
        gdp=Avg("gdp"),
        unrate=Avg("unrate"),
        cpi=Avg("cpi"),
        fedfunds=Avg("fedfunds"),
    )

    return JsonResponse({
        "date": latest_date,
        "ticker": ticker,
        "summary": summary,
        "asset_class_breakdown": asset_breakdown,
        "macro": macro,
    })



def collateral_ticker_asset_class(request):
    ticker = request.GET.get("ticker")
    asset_class = request.GET.get("asset_class")

    if not ticker or not asset_class:
        return JsonResponse(
            {"error": "ticker and asset_class are required"},
            status=400
        )

    ticker = ticker.upper()

    latest_date = (
        CollateralData.objects
        .order_by("-date")
        .values_list("date", flat=True)
        .first()
    )

    qs = CollateralData.objects.filter(
        date=latest_date,
        ticker=ticker,
        asset_class=asset_class
    )

    if not qs.exists():
        return JsonResponse(
            {"error": f"No collateral data for {ticker} in {asset_class}"},
            status=404
        )

    # ---------- SUMMARY ----------
    summary = qs.aggregate(
        exposure=Coalesce(Sum("exposure_before_collateral"), 0.0),
        required=Coalesce(Sum("required_collateral"), 0.0),
        effective=Coalesce(Sum("effective_collateral"), 0.0),
        net_exposure=Coalesce(Sum("net_exposure"), 0.0),
        margin_call_amount=Coalesce(Sum("margin_call_amount"), 0.0),
        margin_call_count=Count("id", filter=Q(margin_call_flag=True)),
        avg_ratio=Coalesce(Avg("collateral_ratio"), 0.0),
    )

    summary["margin_stress"] = summary["margin_call_count"] > 0

    # ---------- STRESS EVENTS ----------
    stress_events = list(
        qs.filter(margin_call_flag=True)
        .values(
            "trade_id",
            "counterparty",
            "jurisdiction",
            "agreement_type",
            "required_collateral",
            "collateral_value",
            "effective_collateral",
            "margin_call_amount",
            "collateral_ratio",
        )
        .order_by("-margin_call_amount")
    )

    return JsonResponse({
        "date": latest_date,
        "ticker": ticker,
        "asset_class": asset_class,
        "summary": summary,
        "stress_events": stress_events,
    })
