from django.http import JsonResponse
from django.db.models import Sum, Count, Avg
from django.db.models.functions import Coalesce

from ..models import LoanData


def loans_overview(request):
    """
    Firm-level Loan Credit Risk Overview
    Uses latest month snapshot
    """

    latest_month = (
        LoanData.objects
        .order_by("-month")
        .values_list("month", flat=True)
        .first()
    )

    qs = LoanData.objects.filter(month=latest_month)

    # ---------------- SUMMARY ----------------
    summary = qs.aggregate(
        ticker_count=Count("ticker", distinct=True),
        loan_count=Count("loan_id", distinct=True),
        total_notional=Coalesce(Sum("notional_usd"), 0.0),
        total_expected_loss=Coalesce(Sum("Expected_Loss"), 0.0),
        total_pnl=Coalesce(Sum("total_pnl"), 0.0),
    )

    # ---------------- TOP CREDIT RISK ----------------
    top_risk = list(
        qs.values("ticker")
        .annotate(
            total_notional=Coalesce(Sum("notional_usd"), 0.0),
            expected_loss=Coalesce(Sum("Expected_Loss"), 0.0),
            total_pnl=Coalesce(Sum("total_pnl"), 0.0),
            loan_count=Count("loan_id"),
        )
        .order_by("-expected_loss")[:5]
    )

    # ---------------- TOP NOTIONAL ----------------
    top_notional = list(
        qs.values("ticker")
        .annotate(
            total_notional=Coalesce(Sum("notional_usd"), 0.0),
            expected_loss=Coalesce(Sum("Expected_Loss"), 0.0),
            loan_count=Count("loan_id"),
        )
        .order_by("-total_notional")[:5]
    )

    return JsonResponse(
        {
            "month": latest_month,
            "summary": summary,
            "top_credit_risk": top_risk,
            "top_notional": top_notional,
            "update_frequency": "Monthly",
        }
    )





def loans_ticker_detail(request):
    ticker = request.GET.get("ticker")

    if not ticker:
        return JsonResponse({"error": "Missing ticker"}, status=400)

    ticker = ticker.upper()

    latest_month = (
        LoanData.objects
        .order_by("-month")
        .values_list("month", flat=True)
        .first()
    )

    qs = LoanData.objects.filter(month=latest_month, ticker=ticker)

    if not qs.exists():
        return JsonResponse(
            {"error": f"No loans found for ticker {ticker}"},
            status=404,
        )

    summary = qs.aggregate(
        loan_count=Count("loan_id"),
        total_notional=Coalesce(Sum("notional_usd"), 0.0),
        total_expected_loss=Coalesce(Sum("Expected_Loss"), 0.0),
        total_pnl=Coalesce(Sum("total_pnl"), 0.0),
        avg_stage=Avg("stage"),
    )

    # IFRS stage assessment
    stages = list(qs.values_list("stage", flat=True))
    if all(s == 1 for s in stages):
        ifrs_status = "All Loans Performing (Stage 1)"
    elif any(s == 3 for s in stages):
        ifrs_status = "Default Loans Present (Stage 3)"
    else:
        ifrs_status = "Mixed Credit Quality"

    loans = list(
        qs.values(
            "loan_id",
            "rate_type",
            "notional_usd",
            "Expected_Loss",
            "stage",
            "time_to_maturity_months",
        )
        .order_by("-notional_usd")
    )

    pred_spread = qs.aggregate(pred=Avg("pred_credit_spread"))["pred"]

    return JsonResponse(
        {
            "month": latest_month,
            "ticker": ticker,
            "summary": summary,
            "ifrs_status": ifrs_status,
            "predicted_spread_21d": pred_spread,
            "loans": loans,
        }
    )



def loan_detail(request):
    loan_id = request.GET.get("loan_id")

    if not loan_id:
        return JsonResponse({"error": "Missing loan_id"}, status=400)

    latest_month = (
        LoanData.objects
        .order_by("-month")
        .values_list("month", flat=True)
        .first()
    )

    loan = (
        LoanData.objects
        .filter(month=latest_month, loan_id=loan_id)
        .values(
            "loan_id",
            "ticker",
            "currency",
            "rate_type",
            "notional_usd",
            "credit_rating",
            "stage",
            "time_to_maturity_months",
            "PD",
            "LGD",
            "EAD",
            "Expected_Loss",
            "total_pnl",
            "liquidity_score",
            "macro_stress_score",
        )
        .first()
    )

    if not loan:
        return JsonResponse({"error": "Loan not found"}, status=404)

    return JsonResponse({"month": latest_month, "loan": loan})
