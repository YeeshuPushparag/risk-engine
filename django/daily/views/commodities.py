from django.db.models import Sum, Count, Min, F, Avg
from django.db.models.functions import Coalesce
from django.http import JsonResponse
from ..models import CommodityData

# =========================
# Commodity Symbol Mapping
# =========================
COMMODITY_MAP = {
    "GC": "Gold",
    "SI": "Silver",
    "CL": "Crude Oil",
    "NG": "Natural Gas",
    "ZC": "Corn",
    "ZS": "Soybeans",
    "ZW": "Wheat",
    "HG": "Copper",
    "PA": "Palladium",
    "PL": "Platinum",
}

def clean_symbol(db_symbol: str) -> str:
    return db_symbol.replace("=F", "")

def db_commodity(symbol: str) -> str:
    return f"{symbol}=F"

# =========================
# OVERVIEW
# =========================
def commodities_overview(request):
    latest_date = CommodityData.objects.order_by("-date").values_list("date", flat=True).first()
    qs = CommodityData.objects.filter(date=latest_date)

    summary = qs.aggregate(
        total_exposure=Coalesce(Sum("exposure_amount"), 0.0),
        total_pnl=Coalesce(Sum("commodity_pnl"), 0.0),
        tickers=Count("ticker", distinct=True),
        managers=Count("asset_manager", distinct=True),
        commodities=Count("commodity", distinct=True),
        worst_var95=Min(F("VaR_95") * F("exposure_amount")),
        worst_var99=Min(F("VaR_99") * F("exposure_amount")),
    )

    manager_summary = (
        qs.values("asset_manager")
        .annotate(
            exposure=Coalesce(Sum("exposure_amount"), 0.0),
            pnl=Coalesce(Sum("commodity_pnl"), 0.0),
            var95=Min(F("VaR_95") * F("exposure_amount")),
            var99=Min(F("VaR_99") * F("exposure_amount")),
            commodities=Count("commodity", distinct=True),
        )
        .order_by("-exposure")[:5]
    )

    commodity_summary = (
        qs.values("commodity")
        .annotate(
            exposure=Coalesce(Sum("exposure_amount"), 0.0),
            pnl=Coalesce(Sum("commodity_pnl"), 0.0),
            var95=Min(F("VaR_95") * F("exposure_amount")),
            var99=Min(F("VaR_99") * F("exposure_amount")),
            managers=Count("asset_manager", distinct=True),
        )
        .order_by("-exposure")
    )

    for item in commodity_summary:
        symbol = clean_symbol(item["commodity"])
        item["commodity"] = symbol
        item["commodity_name"] = COMMODITY_MAP.get(symbol, symbol)

    ticker_summary = (
        qs.values("ticker")
        .annotate(
            exposure=Coalesce(Sum("exposure_amount"), 0.0),
            pnl=Coalesce(Sum("commodity_pnl"), 0.0),
            var95=Min(F("VaR_95") * F("exposure_amount")),
            var99=Min(F("VaR_99") * F("exposure_amount")),
            commodities=Count("commodity", distinct=True),
        )
        .order_by("-exposure")[:5]
    )

    return JsonResponse({
        "date": latest_date,
        "summary": summary,
        "managers": list(manager_summary),
        "commodities": list(commodity_summary),
        "tickers": list(ticker_summary),
    })

# =========================
# TICKER VIEW
# =========================
def commodities_ticker(request):
    ticker = request.GET.get("ticker")
    if not ticker:
        return JsonResponse({"error": "Missing ticker"}, status=400)

    ticker = ticker.upper()
    latest_date = CommodityData.objects.order_by("-date").values_list("date", flat=True).first()
    qs = CommodityData.objects.filter(date=latest_date, ticker=ticker)

    if not qs.exists():
        return JsonResponse({"error": f"No data for {ticker}"}, status=404)

    summary = qs.aggregate(
        total_exposure=Coalesce(Sum("exposure_amount"), 0.0),
        total_pnl=Coalesce(Sum("commodity_pnl"), 0.0),
        worst_var95=Min(F("VaR_95") * F("exposure_amount")),
        worst_var99=Min(F("VaR_99") * F("exposure_amount")),
        commodities=Count("commodity", distinct=True),
        managers=Count("asset_manager", distinct=True),
    )

    by_commodity = (
        qs.values("commodity")
        .annotate(
            exposure=Coalesce(Sum("exposure_amount"), 0.0),
            pnl=Coalesce(Sum("commodity_pnl"), 0.0),
            var95=Min(F("VaR_95") * F("exposure_amount")),
            var99=Min(F("VaR_99") * F("exposure_amount")),
            avg_hedge=Coalesce(Avg("hedge_ratio"), 0.0),
            avg_sensitivity=Coalesce(Avg("sensitivity"), 0.0),
            avg_pred_vol21=Avg("pred_vol21"),
            managers=Count("asset_manager", distinct=True),
        )
        .order_by("-exposure")
    )

    for item in by_commodity:
        symbol = clean_symbol(item["commodity"])
        item["commodity"] = symbol
        item["commodity_name"] = COMMODITY_MAP.get(symbol, symbol)

    by_manager = (
        qs.values("asset_manager")
        .annotate(
            exposure=Coalesce(Sum("exposure_amount"), 0.0),
            pnl=Coalesce(Sum("commodity_pnl"), 0.0),
            var95=Min(F("VaR_95") * F("exposure_amount")),
            var99=Min(F("VaR_99") * F("exposure_amount")),
            commodities=Count("commodity", distinct=True),
        )
        .order_by("-exposure")
    )

    return JsonResponse({
        "date": latest_date,
        "ticker": ticker,
        "summary": summary,
        "by_commodity": list(by_commodity),
        "by_manager": list(by_manager),
    })

# =========================
# MANAGER VIEW
# =========================
def commodities_manager(request):
    manager = request.GET.get("manager")
    if not manager:
        return JsonResponse({"error": "Missing manager"}, status=400)

    latest_date = CommodityData.objects.order_by("-date").values_list("date", flat=True).first()
    qs = CommodityData.objects.filter(date=latest_date, asset_manager=manager)

    if not qs.exists():
        return JsonResponse({"error": f"No data for {manager}"}, status=404)

    summary = qs.aggregate(
        total_exposure=Coalesce(Sum("exposure_amount"), 0.0),
        total_pnl=Coalesce(Sum("commodity_pnl"), 0.0),
        worst_var95=Min(F("VaR_95") * F("exposure_amount")),
        worst_var99=Min(F("VaR_99") * F("exposure_amount")),
        commodities=Count("commodity", distinct=True),
    )

    breakdown = (
        qs.values("commodity")
        .annotate(
            exposure=Coalesce(Sum("exposure_amount"), 0.0),
            pnl=Coalesce(Sum("commodity_pnl"), 0.0),
            var95=Min(F("VaR_95") * F("exposure_amount")),
            var99=Min(F("VaR_99") * F("exposure_amount")),
            tickers=Count("ticker", distinct=True),
        )
        .order_by("-exposure")
    )

    for item in breakdown:
        symbol = clean_symbol(item["commodity"])
        item["commodity"] = symbol
        item["commodity_name"] = COMMODITY_MAP.get(symbol, symbol)

    return JsonResponse({
        "date": latest_date,
        "manager": manager,
        "summary": summary,
        "commodities": list(breakdown),
    })

# =========================
# COMMODITY VIEW (SYMBOL API)
# =========================
def commodities_commodity(request):
    symbol = request.GET.get("commodity")
    if not symbol:
        return JsonResponse({"error": "Missing commodity"}, status=400)

    symbol = symbol.upper()
    commodity_db = db_commodity(symbol)

    latest_date = CommodityData.objects.order_by("-date").values_list("date", flat=True).first()
    qs = CommodityData.objects.filter(date=latest_date, commodity=commodity_db)

    if not qs.exists():
        return JsonResponse({"error": f"No data for {symbol}"}, status=404)

    commodity_name = COMMODITY_MAP.get(symbol, symbol)

    summary = qs.aggregate(
        total_exposure=Coalesce(Sum("exposure_amount"), 0.0),
        total_pnl=Coalesce(Sum("commodity_pnl"), 0.0),
        worst_var95=Min(F("VaR_95") * F("exposure_amount")),
        worst_var99=Min(F("VaR_99") * F("exposure_amount")),
    )

    tickers = (
        qs.values("ticker")
        .annotate(
            exposure=Coalesce(Sum("exposure_amount"), 0.0),
            pnl=Coalesce(Sum("commodity_pnl"), 0.0),
            avg_hedge=Coalesce(Avg("hedge_ratio"), 0.0),
            avg_sensitivity=Coalesce(Avg("sensitivity"), 0.0),
            managers=Count("asset_manager", distinct=True),
        )
        .order_by("-exposure")[:20]
    )

    return JsonResponse({
        "date": latest_date,
        "commodity": symbol,
        "commodity_name": commodity_name,
        "summary": summary,
        "tickers": list(tickers),
    })
from django.db.models import Sum, Count, Min, F, Avg
from django.db.models.functions import Coalesce
from django.http import JsonResponse
from ..models import CommodityData

# =========================
# Commodity Symbol Mapping
# =========================
COMMODITY_MAP = {
    "GC": "Gold",
    "SI": "Silver",
    "CL": "Crude Oil",
    "NG": "Natural Gas",
    "ZC": "Corn",
    "ZS": "Soybeans",
    "ZW": "Wheat",
    "HG": "Copper",
    "PA": "Palladium",
    "PL": "Platinum",
}

def clean_symbol(db_symbol: str) -> str:
    return db_symbol.replace("=F", "")

def db_commodity(symbol: str) -> str:
    return f"{symbol}=F"

# =========================
# OVERVIEW
# =========================
def commodities_overview(request):
    latest_date = CommodityData.objects.order_by("-date").values_list("date", flat=True).first()
    qs = CommodityData.objects.filter(date=latest_date)

    summary = qs.aggregate(
        total_exposure=Coalesce(Sum("exposure_amount"), 0.0),
        total_pnl=Coalesce(Sum("commodity_pnl"), 0.0),
        tickers=Count("ticker", distinct=True),
        managers=Count("asset_manager", distinct=True),
        commodities=Count("commodity", distinct=True),
        worst_var95=Min(F("VaR_95") * F("exposure_amount")),
        worst_var99=Min(F("VaR_99") * F("exposure_amount")),
    )

    manager_summary = (
        qs.values("asset_manager")
        .annotate(
            exposure=Coalesce(Sum("exposure_amount"), 0.0),
            pnl=Coalesce(Sum("commodity_pnl"), 0.0),
            var95=Min(F("VaR_95") * F("exposure_amount")),
            var99=Min(F("VaR_99") * F("exposure_amount")),
            commodities=Count("commodity", distinct=True),
        )
        .order_by("-exposure")[:5]
    )

    commodity_summary = (
        qs.values("commodity")
        .annotate(
            exposure=Coalesce(Sum("exposure_amount"), 0.0),
            pnl=Coalesce(Sum("commodity_pnl"), 0.0),
            var95=Min(F("VaR_95") * F("exposure_amount")),
            var99=Min(F("VaR_99") * F("exposure_amount")),
            managers=Count("asset_manager", distinct=True),
        )
        .order_by("-exposure")
    )

    for item in commodity_summary:
        symbol = clean_symbol(item["commodity"])
        item["commodity"] = symbol
        item["commodity_name"] = COMMODITY_MAP.get(symbol, symbol)

    ticker_summary = (
        qs.values("ticker")
        .annotate(
            exposure=Coalesce(Sum("exposure_amount"), 0.0),
            pnl=Coalesce(Sum("commodity_pnl"), 0.0),
            var95=Min(F("VaR_95") * F("exposure_amount")),
            var99=Min(F("VaR_99") * F("exposure_amount")),
            commodities=Count("commodity", distinct=True),
        )
        .order_by("-exposure")[:5]
    )

    return JsonResponse({
        "date": latest_date,
        "summary": summary,
        "managers": list(manager_summary),
        "commodities": list(commodity_summary),
        "tickers": list(ticker_summary),
    })

# =========================
# TICKER VIEW
# =========================
def commodities_ticker(request):
    ticker = request.GET.get("ticker")
    if not ticker:
        return JsonResponse({"error": "Missing ticker"}, status=400)

    ticker = ticker.upper()
    latest_date = CommodityData.objects.order_by("-date").values_list("date", flat=True).first()
    qs = CommodityData.objects.filter(date=latest_date, ticker=ticker)

    if not qs.exists():
        return JsonResponse({"error": f"No data for {ticker}"}, status=404)

    summary = qs.aggregate(
        total_exposure=Coalesce(Sum("exposure_amount"), 0.0),
        total_pnl=Coalesce(Sum("commodity_pnl"), 0.0),
        worst_var95=Min(F("VaR_95") * F("exposure_amount")),
        worst_var99=Min(F("VaR_99") * F("exposure_amount")),
        commodities=Count("commodity", distinct=True),
        managers=Count("asset_manager", distinct=True),
    )

    by_commodity = (
        qs.values("commodity")
        .annotate(
            exposure=Coalesce(Sum("exposure_amount"), 0.0),
            pnl=Coalesce(Sum("commodity_pnl"), 0.0),
            var95=Min(F("VaR_95") * F("exposure_amount")),
            var99=Min(F("VaR_99") * F("exposure_amount")),
            avg_hedge=Coalesce(Avg("hedge_ratio"), 0.0),
            avg_sensitivity=Coalesce(Avg("sensitivity"), 0.0),
            avg_pred_vol21=Avg("pred_vol21"),
            managers=Count("asset_manager", distinct=True),
        )
        .order_by("-exposure")
    )

    for item in by_commodity:
        symbol = clean_symbol(item["commodity"])
        item["commodity"] = symbol
        item["commodity_name"] = COMMODITY_MAP.get(symbol, symbol)

    by_manager = (
        qs.values("asset_manager")
        .annotate(
            exposure=Coalesce(Sum("exposure_amount"), 0.0),
            pnl=Coalesce(Sum("commodity_pnl"), 0.0),
            var95=Min(F("VaR_95") * F("exposure_amount")),
            var99=Min(F("VaR_99") * F("exposure_amount")),
            commodities=Count("commodity", distinct=True),
        )
        .order_by("-exposure")
    )

    return JsonResponse({
        "date": latest_date,
        "ticker": ticker,
        "summary": summary,
        "by_commodity": list(by_commodity),
        "by_manager": list(by_manager),
    })

# =========================
# MANAGER VIEW
# =========================
def commodities_manager(request):
    manager = request.GET.get("manager")
    if not manager:
        return JsonResponse({"error": "Missing manager"}, status=400)

    latest_date = CommodityData.objects.order_by("-date").values_list("date", flat=True).first()
    qs = CommodityData.objects.filter(date=latest_date, asset_manager=manager)

    if not qs.exists():
        return JsonResponse({"error": f"No data for {manager}"}, status=404)

    summary = qs.aggregate(
        total_exposure=Coalesce(Sum("exposure_amount"), 0.0),
        total_pnl=Coalesce(Sum("commodity_pnl"), 0.0),
        worst_var95=Min(F("VaR_95") * F("exposure_amount")),
        worst_var99=Min(F("VaR_99") * F("exposure_amount")),
        commodities=Count("commodity", distinct=True),
    )

    breakdown = (
        qs.values("commodity")
        .annotate(
            exposure=Coalesce(Sum("exposure_amount"), 0.0),
            pnl=Coalesce(Sum("commodity_pnl"), 0.0),
            var95=Min(F("VaR_95") * F("exposure_amount")),
            var99=Min(F("VaR_99") * F("exposure_amount")),
            tickers=Count("ticker", distinct=True),
        )
        .order_by("-exposure")
    )

    for item in breakdown:
        symbol = clean_symbol(item["commodity"])
        item["commodity"] = symbol
        item["commodity_name"] = COMMODITY_MAP.get(symbol, symbol)

    return JsonResponse({
        "date": latest_date,
        "manager": manager,
        "summary": summary,
        "commodities": list(breakdown),
    })

# =========================
# COMMODITY VIEW (SYMBOL API)
# =========================
def commodities_commodity(request):
    symbol = request.GET.get("commodity")
    if not symbol:
        return JsonResponse({"error": "Missing commodity"}, status=400)

    symbol = symbol.upper()
    commodity_db = db_commodity(symbol)

    latest_date = CommodityData.objects.order_by("-date").values_list("date", flat=True).first()
    qs = CommodityData.objects.filter(date=latest_date, commodity=commodity_db)

    if not qs.exists():
        return JsonResponse({"error": f"No data for {symbol}"}, status=404)

    commodity_name = COMMODITY_MAP.get(symbol, symbol)

    summary = qs.aggregate(
        total_exposure=Coalesce(Sum("exposure_amount"), 0.0),
        total_pnl=Coalesce(Sum("commodity_pnl"), 0.0),
        worst_var95=Min(F("VaR_95") * F("exposure_amount")),
        worst_var99=Min(F("VaR_99") * F("exposure_amount")),
    )

    tickers = (
        qs.values("ticker")
        .annotate(
            exposure=Coalesce(Sum("exposure_amount"), 0.0),
            pnl=Coalesce(Sum("commodity_pnl"), 0.0),
            avg_hedge=Coalesce(Avg("hedge_ratio"), 0.0),
            avg_sensitivity=Coalesce(Avg("sensitivity"), 0.0),
            managers=Count("asset_manager", distinct=True),
        )
        .order_by("-exposure")[:20]
    )

    return JsonResponse({
        "date": latest_date,
        "commodity": symbol,
        "commodity_name": commodity_name,
        "summary": summary,
        "tickers": list(tickers),
    })
