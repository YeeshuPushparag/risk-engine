from django.http import JsonResponse
from django.db.models import Sum, Avg, Count, Q, Max, F, Value, CharField, IntegerField, Case, When
from django.db.models.functions import Coalesce
from ..models import (
    EquityData, FXData, BondData, CommodityData, 
    LoanData, DerivativeData, CollateralData, CollateralModelData
)


from django.views.decorators.cache import cache_page


@cache_page(60 * 5)  # cache 5 minutes (safe for EOD dashboard)
def global_risk_dashboard(request):

    # -----------------------------
    # 1. SYNC TIMESTAMPS (FAST)
    # -----------------------------
    latest_daily = EquityData.objects.aggregate(
        d=Max("date")
    )["d"]

    latest_monthly = LoanData.objects.aggregate(
        m=Max("month")
    )["m"]

    # -----------------------------
    # 2. PRE-FILTERED QUERYSETS
    # -----------------------------
    eq_qs = EquityData.objects.filter(date=latest_daily)
    fx_qs = FXData.objects.filter(date=latest_daily)
    cm_qs = CommodityData.objects.filter(date=latest_daily)
    bd_qs = BondData.objects.filter(date=latest_daily)
    dr_qs = DerivativeData.objects.filter(date=latest_daily)
    cl_qs = CollateralData.objects.filter(date=latest_daily)
    ln_qs = LoanData.objects.filter(month=latest_monthly)

    # -----------------------------
    # 3. AGGREGATES (OPTIMIZED)
    # -----------------------------

    eq = eq_qs.aggregate(
        exp=Sum("mtm_value"),
        pnl=Sum("daily_pnl"),
        risk_val=Avg("volatility_21d"),
        alerts=Sum(
            Case(
                When(Q(daily_return__lt=-0.04) | Q(altman_z__lt=1.8), then=1),
                default=0,
                output_field=IntegerField()
            )
        ),
    )

    fx = fx_qs.aggregate(
        exp=Sum("exposure_amount"),
        pnl=Sum("total_pnl"),
        risk_val=Avg("fx_volatility"),
        alerts=Sum(
            Case(
                When(value_at_risk__gt=1_000_000, then=1),
                default=0,
                output_field=IntegerField()
            )
        ),
    )

    cm = cm_qs.aggregate(
        exp=Sum("exposure_amount"),
        pnl=Sum("commodity_pnl"),
        risk_val=Sum("VaR_95"),
        alerts=Sum(
            Case(
                When(daily_return__lt=-0.05, then=1),
                default=0,
                output_field=IntegerField()
            )
        ),
    )

    bd = bd_qs.aggregate(
        exp=Sum("bond_price"),
        pnl=Avg("credit_spread"),
        risk_val=Avg("yield_to_maturity"),
        alerts=Sum(
            Case(
                When(
                    Q(credit_spread__gt=450) |
                    Q(implied_pd_annual__gt=0.05),
                    then=1
                ),
                default=0,
                output_field=IntegerField()
            )
        ),
    )

    dr = dr_qs.aggregate(
        exp=Sum("notional"),
        pnl=Sum("pnl"),
        risk_val=Sum("delta"),
        alerts=Sum(
            Case(
                When(margin_call_flag=True, then=1),
                default=0,
                output_field=IntegerField()
            )
        ),
    )

    cl = cl_qs.aggregate(
        exp=Sum("collateral_value"),
        pnl=Sum("net_exposure"),
        risk_val=Avg("haircut"),
        alerts=Sum(
            Case(
                When(
                    Q(margin_call_flag=True) |
                    Q(collateral_ratio__lt=1.0),
                    then=1
                ),
                default=0,
                output_field=IntegerField()
            )
        ),
    )

    ln = ln_qs.aggregate(
        exp=Sum("notional_usd"),
        pnl=Sum("total_pnl"),
        risk_val=Avg("utilization_ratio"),
        alerts=Sum(
            Case(
                When(
                    Q(utilization_ratio__gt=0.85) |
                    Q(macro_stress_score__gt=0.7),
                    then=1
                ),
                default=0,
                output_field=IntegerField()
            )
        ),
    )

    # -----------------------------
    # 4. RISK COLOR LOGIC (UNCHANGED)
    # -----------------------------
    def get_risk_status(pnl, alerts):
        if alerts and alerts > 2:
            return "red"
        if alerts and alerts > 0 or (pnl and pnl < 0):
            return "yellow"
        return "green"

    # -----------------------------
    # 5. SEGMENTS (SCHEMA IDENTICAL)
    # -----------------------------
    segments = [
        {"id": "eq", "name": "Equity", "link": "equity",
         "exposure": eq["exp"] or 0, "pnl": eq["pnl"] or 0,
         "riskColor": get_risk_status(eq["pnl"], eq["alerts"]),
         "alerts": eq["alerts"] or 0,
         "metric": f"Vol: {eq['risk_val']:.2%}" if eq["risk_val"] else "N/A"},

        {"id": "fx", "name": "FX", "link": "fx",
         "exposure": fx["exp"] or 0, "pnl": fx["pnl"] or 0,
         "riskColor": get_risk_status(fx["pnl"], fx["alerts"]),
         "alerts": fx["alerts"] or 0,
         "metric": f"Vol: {fx['risk_val']:.2%}" if fx["risk_val"] else "N/A"},

        {"id": "cm", "name": "Commodities", "link": "commodities",
         "exposure": cm["exp"] or 0, "pnl": cm["pnl"] or 0,
         "riskColor": get_risk_status(cm["pnl"], cm["alerts"]),
         "alerts": cm["alerts"] or 0,
         "metric": f"VaR: {cm['risk_val']:.1f}" if cm["risk_val"] else "N/A"},

        {"id": "bd", "name": "Bonds", "link": "bonds",
         "exposure": bd["exp"] or 0, "pnl": bd["pnl"] or 0,
         "riskColor": get_risk_status(0, bd["alerts"]),
         "alerts": bd["alerts"] or 0,
         "metric": f"YTM: {bd['risk_val']:.2%}" if bd["risk_val"] else "N/A"},

        {"id": "dr", "name": "Derivatives", "link": "derivatives",
         "exposure": dr["exp"] or 0, "pnl": dr["pnl"] or 0,
         "riskColor": get_risk_status(dr["pnl"], dr["alerts"]),
         "alerts": dr["alerts"] or 0,
         "metric": f"Delta: {dr['risk_val']:.2f}" if dr["risk_val"] else "N/A"},

        {"id": "cl", "name": "Collateral", "link": "collateral",
         "exposure": cl["exp"] or 0, "pnl": cl["pnl"] or 0,
         "riskColor": get_risk_status(cl["pnl"], cl["alerts"]),
         "alerts": cl["alerts"] or 0,
         "metric": f"Haircut: {cl['risk_val']:.2%}" if cl["risk_val"] else "N/A"},

        {"id": "ln", "name": "Loans", "link": "loans",
         "exposure": ln["exp"] or 0, "pnl": ln["pnl"] or 0,
         "riskColor": get_risk_status(ln["pnl"], ln["alerts"]),
         "alerts": ln["alerts"] or 0,
         "metric": f"Util: {ln['risk_val']:.1%}" if ln["risk_val"] else "N/A"},
    ]

    total_exposure = sum(s["exposure"] for s in segments)

    # -----------------------------
    # 6. RESPONSE (UNCHANGED)
    # -----------------------------
    return JsonResponse({
        "total_gross_exposure": total_exposure,
        "metadata": {
            "market_date": latest_daily.isoformat() if latest_daily else None,
            "loan_month": latest_monthly.isoformat() if latest_monthly else None,
        },
        "segments": segments,
    })







@cache_page(60 * 5)
def compliance_overview(request):
    # 1. Determine the mode based on the archive variable
    is_archive_request = request.GET.get('archive', 'false').lower() == 'true'

    response = {
        "date": None,
        "view_mode": "Live" if not is_archive_request else "Archive",
        "governance": {
            "framework": "Internal Models Approved (IMA)",
            "regime_text": None,
            "regime_color": None,
        },
        "summary": {
            "critical_breaches": 0,
            "warnings": 0,
            "compliance_score": 100.0
        },
        "limits": [],
        "violations": []
    }

    # Helper function to get the correct date for each segment
    def get_target_date(model_class, date_field="date"):
        dates = list(model_class.objects.order_by(f"-{date_field}").values_list(date_field, flat=True).distinct())
        if not dates:
            return None
        if is_archive_request and len(dates) > 1:
            return dates[1]  # Return previous data point
        return dates[0]      # Return latest data point

    # ======================================================
    # 1️⃣ FIRM RISK STATE (Anchored to Equity Timeline)
    # ======================================================
    main_date = get_target_date(EquityData)
    response["date"] = main_date
    portfolio_var = 0

    if main_date:
        latest_eq = EquityData.objects.filter(date=main_date).values("daily_portfolio_var_99").first()
        if latest_eq:
            portfolio_var = latest_eq["daily_portfolio_var_99"] or 0
            if portfolio_var > 0.025:
                regime_text = "Crisis"
                regime_color = "red"
            elif portfolio_var > 0.015:
                regime_text = "Stress"
                regime_color = "amber"
            else:
                regime_text = "Normal"
                regime_color = "green"

            response["governance"]["regime_mode"] = regime_text
            response["governance"]["regime_color"] = regime_color

    # ======================================================
    # 2️⃣ FIRM LIMITS
    # ======================================================
    status = "Hard Breach" if portfolio_var > 0.02 else "Warning" if portfolio_var > 0.015 else "Compliant"
    if status == "Hard Breach":
        response["summary"]["critical_breaches"] += 1
        response["violations"].append({"message": "Firm Trading Book VaR breached risk appetite", "segment": "Firm"})
    elif status == "Warning":
        response["summary"]["warnings"] += 1

    response["limits"].append({
        "segment": "Firm", "type": "Portfolio VaR 99%", "limit": 0.02, "actual": portfolio_var, "status": status
    })

    # ======================================================
    # 3️⃣ EQUITIES
    # ======================================================
    try:
        t_date = get_target_date(EquityData)
        if t_date:
            qs = EquityData.objects.filter(date=t_date)
            eq_var = qs.aggregate(var=Coalesce(Avg("daily_var_99"), 0.0))["var"]
            status = "Hard Breach" if eq_var > 0.02 else "Warning" if eq_var > 0.015 else "Compliant"
            if status == "Hard Breach":
                response["summary"]["critical_breaches"] += 1
                response["violations"].append({"message": "Equity book VaR outside limits", "segment": "Equities"})
            elif status == "Warning":
                response["summary"]["warnings"] += 1
            response["limits"].append({"segment": "Equities", "type": "Market VaR 99%", "limit": 0.02, "actual": eq_var, "status": status})
    except: pass

    # ======================================================
    # 4️⃣ FX
    # ======================================================
    try:
        t_date = get_target_date(FXData)
        if t_date:
            qs = FXData.objects.filter(date=t_date)
            fx_var99 = qs.aggregate(v=Coalesce(Avg("VaR_99"), 0.0))["v"]
            status = "Hard Breach" if fx_var99 > 1500000 else "Warning" if fx_var99 > 1000000 else "Compliant"
            if status == "Hard Breach":
                response["summary"]["critical_breaches"] += 1
                response["violations"].append({"message": "FX VaR stress beyond threshold", "segment": "FX"})
            elif status == "Warning":
                response["summary"]["warnings"] += 1
            response["limits"].append({"segment": "FX", "type": "Market VaR 99%", "limit": 1000000, "actual": fx_var99, "status": status})
    except: pass

    # ======================================================
    # 5️⃣ BONDS
    # ======================================================
    try:
        t_date = get_target_date(BondData)
        if t_date:
            qs = BondData.objects.filter(date=t_date)
            spread = qs.aggregate(s=Coalesce(Avg("credit_spread"), 0.0))["s"]
            status = "Hard Breach" if spread > 300 else "Warning" if spread > 200 else "Compliant"
            if status == "Hard Breach":
                response["summary"]["critical_breaches"] += 1
                response["violations"].append({"message": "Corporate bond spread stress detected", "segment": "Bonds"})
            elif status == "Warning":
                response["summary"]["warnings"] += 1
            response["limits"].append({"segment": "Bonds", "type": "Spread Risk (bps)", "limit": 200, "actual": spread, "status": status})
    except: pass

    # ======================================================
    # 6️⃣ COMMODITIES
    # ======================================================
    try:
        t_date = get_target_date(CommodityData)
        if t_date:
            qs = CommodityData.objects.filter(date=t_date)
            var99 = qs.aggregate(v=Coalesce(Avg("VaR_99"), 0.0))["v"]
            status = "Hard Breach" if var99 > 2000000 else "Warning" if var99 > 1300000 else "Compliant"
            if status == "Hard Breach":
                response["summary"]["critical_breaches"] += 1
                response["violations"].append({"message": "Commodity VaR shock", "segment": "Commodities"})
            elif status == "Warning": response["summary"]["warnings"] += 1
            response["limits"].append({"segment": "Commodities", "type": "Market VaR 99%", "limit": 1300000, "actual": var99, "status": status})
    except: pass

    # ======================================================
    # 7️⃣ DERIVATIVES
    # ======================================================
    try:
        t_date = get_target_date(DerivativeData)
        if t_date:
            qs = DerivativeData.objects.filter(date=t_date)
            agg = qs.aggregate(total_mc=Coalesce(Sum("margin_call_amount"), 0.0), mc_count=Count("trade_id", filter=Q(margin_call_flag=True)))
            mc = agg["total_mc"]
            status = "Hard Breach" if (agg["mc_count"] > 10 or mc > 2000000) else "Warning" if mc > 1000000 else "Compliant"
            if status == "Hard Breach":
                response["summary"]["critical_breaches"] += 1
                response["violations"].append({"message": "Derivatives margin stress event", "segment": "Derivatives"})
            elif status == "Warning": response["summary"]["warnings"] += 1
            response["limits"].append({"segment": "Derivatives", "type": "Margin Stress", "limit": 2000000, "actual": mc, "status": status})
    except: pass

    # ======================================================
    # 8️⃣ COLLATERAL
    # ======================================================
    try:
        t_date = get_target_date(CollateralModelData)
        if t_date:
            qs = CollateralModelData.objects.filter(date=t_date)
            agg = qs.aggregate(net=Coalesce(Sum("net_exposure"), 0.0), stress=Count("id", filter=Q(margin_call_flag=True)))
            status = "Hard Breach" if (agg["stress"] > 8 or agg["net"] > 3000000) else "Warning" if agg["net"] > 1500000 else "Compliant"
            if status == "Hard Breach":
                response["summary"]["critical_breaches"] += 1
                response["violations"].append({"message": "Collateral deficiency detected", "segment": "Collateral"})
            elif status == "Warning": response["summary"]["warnings"] += 1
            response["limits"].append({"segment": "Collateral", "type": "Collateral Sufficiency", "limit": 1500000, "actual": agg["net"], "status": status})
    except: pass

    # ======================================================
    # 9️⃣ LOANS (Monthly Data)
    # ======================================================
    try:
        t_month = get_target_date(LoanData, "month")
        if t_month:
            qs = LoanData.objects.filter(month=t_month)
            el = qs.aggregate(el=Coalesce(Sum("Expected_Loss"), 0.0))["el"]
            status = "Hard Breach" if el > 5000000 else "Warning" if el > 3500000 else "Compliant"
            if status == "Hard Breach":
                response["summary"]["critical_breaches"] += 1
                response["violations"].append({"message": "Loan Expected Loss exceeds risk appetite", "segment": "Loans"})
            elif status == "Warning": response["summary"]["warnings"] += 1
            response["limits"].append({"segment": "Loans", "type": "Expected Loss", "limit": 5000000, "actual": el, "status": status})
    except: pass

    # ======================================================
    # 🔟 COMPLIANCE SCORE
    # ======================================================
    total_breaches = response["summary"]["critical_breaches"] + response["summary"]["warnings"]
    score = 100.0 if total_breaches == 0 else max(50.0, 100.0 - total_breaches * 8)
    response["summary"]["compliance_score"] = round(score, 2)

    return JsonResponse(response)