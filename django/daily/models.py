from django.db import models


class EquityData(models.Model):
    # Identifiers
    date = models.DateField()
    ticker = models.CharField(max_length=16)
    asset_manager = models.CharField(max_length=128)

    # Price / Volume
    open = models.FloatField(null=True, blank=True)
    high = models.FloatField(null=True, blank=True)
    low = models.FloatField(null=True, blank=True)
    close = models.FloatField(null=True, blank=True)
    volume = models.BigIntegerField(null=True, blank=True)

    # Returns & Technicals
    daily_return = models.FloatField(null=True, blank=True)
    log_return = models.FloatField(null=True, blank=True)
    high_low_spread = models.FloatField(null=True, blank=True)
    close_open_diff = models.FloatField(null=True, blank=True)
    vol_5d = models.FloatField(null=True, blank=True)
    vol_20d = models.FloatField(null=True, blank=True)
    volatility_21d = models.FloatField(null=True, blank=True)
    ma_5 = models.FloatField(null=True, blank=True)
    ma_20 = models.FloatField(null=True, blank=True)
    momentum_5d = models.FloatField(null=True, blank=True)
    momentum_20d = models.FloatField(null=True, blank=True)
    avg_volume_10d = models.FloatField(null=True, blank=True)
    vol_change = models.FloatField(null=True, blank=True)

    # Risk / Performance
    excess_return = models.FloatField(null=True, blank=True)
    downside_risk = models.FloatField(null=True, blank=True)
    sharpe_ratio = models.FloatField(null=True, blank=True)
    sortino_ratio = models.FloatField(null=True, blank=True)

    # Ownership / Holdings
    issuer_name = models.CharField(max_length=128, null=True, blank=True)
    class_name = models.CharField(max_length=32, null=True, blank=True)
    cusip = models.CharField(max_length=128, null=True, blank=True)

    total_value = models.FloatField(null=True, blank=True)
    total_percent = models.FloatField(null=True, blank=True)
    shares = models.FloatField(null=True, blank=True)
    option_type = models.CharField(max_length=32, null=True, blank=True)

    discretion = models.CharField(max_length=32, null=True, blank=True)
    other_manager = models.BigIntegerField(null=True, blank=True)
    sole = models.BigIntegerField(null=True, blank=True)
    shared = models.BigIntegerField(null=True, blank=True)
    none_ownership = models.BigIntegerField(null=True, blank=True)


    # Balance Sheet
    total_debt = models.FloatField(null=True, blank=True)
    short_term_debt = models.FloatField(null=True, blank=True)
    long_term_debt = models.FloatField(null=True, blank=True)
    total_assets = models.FloatField(null=True, blank=True)
    ebitda = models.FloatField(null=True, blank=True)
    revenue = models.FloatField(null=True, blank=True)
    net_income = models.FloatField(null=True, blank=True)

    debt_to_assets = models.FloatField(null=True, blank=True)
    debt_to_ebitda = models.FloatField(null=True, blank=True)
    ebitda_margin = models.FloatField(null=True, blank=True)

    # Market Data
    market_cap = models.FloatField(null=True, blank=True)
    beta = models.FloatField(null=True, blank=True)
    dividend_yield = models.FloatField(null=True, blank=True)
    sector = models.CharField(max_length=128, null=True, blank=True)
    industry = models.CharField(max_length=128, null=True, blank=True)

    # Macro
    gdp = models.FloatField(null=True, blank=True)
    unrate = models.FloatField(null=True, blank=True)
    cpi = models.FloatField(null=True, blank=True)
    fedfunds = models.FloatField(null=True, blank=True)

    # Credit / Fundamental Risk
    interest_coverage_proxy = models.FloatField(null=True, blank=True)
    maturity_proxy = models.FloatField(null=True, blank=True)
    altman_z = models.FloatField(null=True, blank=True)
    free_cash_flow = models.FloatField(null=True, blank=True)
    cash_and_equivalents = models.FloatField(null=True, blank=True)
    credit_rating = models.CharField(max_length=16, null=True, blank=True)

    # PnL / Portfolio
    mtm_value = models.FloatField(null=True, blank=True)
    daily_pnl = models.FloatField(null=True, blank=True)
    portfolio_weight = models.FloatField(null=True, blank=True)

    sector_exposure = models.FloatField(null=True, blank=True)
    industry_exposure = models.FloatField(null=True, blank=True)
    beta_weighted = models.FloatField(null=True, blank=True)
    sector_beta_weighted = models.FloatField(null=True, blank=True)

    daily_sigma = models.FloatField(null=True, blank=True)
    daily_mu = models.FloatField(null=True, blank=True)
    weighted_mu = models.FloatField(null=True, blank=True)
    w_sigma2 = models.FloatField(null=True, blank=True)

    # VaR / CVaR
    daily_var_95 = models.FloatField(null=True, blank=True)
    daily_var_99 = models.FloatField(null=True, blank=True)
    daily_cvar_95 = models.FloatField(null=True, blank=True)
    daily_cvar_99 = models.FloatField(null=True, blank=True)

    daily_portfolio_mu = models.FloatField(null=True, blank=True)
    daily_portfolio_ex_ante_volatility = models.FloatField(null=True, blank=True)
    daily_portfolio_var_95 = models.FloatField(null=True, blank=True)
    daily_portfolio_var_99 = models.FloatField(null=True, blank=True)
    daily_portfolio_cvar_95 = models.FloatField(null=True, blank=True)
    daily_portfolio_cvar_99 = models.FloatField(null=True, blank=True)

    # Concentration / Liquidity
    top_5_exposure = models.FloatField(null=True, blank=True)
    sector_sum = models.FloatField(null=True, blank=True)
    sector_sum_sq = models.FloatField(null=True, blank=True)
    hhi_sector = models.FloatField(null=True, blank=True)
    diversification_score = models.FloatField(null=True, blank=True)
    amihud_illiquidity = models.FloatField(null=True, blank=True)
    turnover_ratio = models.FloatField(null=True, blank=True)
    liquidity_risk_score = models.FloatField(null=True, blank=True)
    sector_exposure_pct = models.FloatField(null=True, blank=True)

    portfolio_daily_pnl = models.FloatField(null=True, blank=True)

    # Predictions
    pred_ret_1d = models.FloatField(null=True, blank=True)
    pred_ret_5d = models.FloatField(null=True, blank=True)
    pred_ret_21d = models.FloatField(null=True, blank=True)
    pred_vol_21d = models.FloatField(null=True, blank=True)
    pred_downside_21d = models.FloatField(null=True, blank=True)
    pred_var_21d = models.FloatField(null=True, blank=True)
    pred_factor_21d = models.FloatField(null=True, blank=True)
    pred_sector_rotation = models.FloatField(null=True, blank=True)
    pred_macro_regime = models.IntegerField(null=True, blank=True)

    pred_port_ret_1d = models.FloatField(null=True, blank=True)
    pred_port_ret_5d = models.FloatField(null=True, blank=True)
    pred_port_ret_21d = models.FloatField(null=True, blank=True)
    pred_port_var_1d = models.FloatField(null=True, blank=True)
    pred_port_var_5d = models.FloatField(null=True, blank=True)
    pred_port_var_21d = models.FloatField(null=True, blank=True)

    class Meta:
        db_table = "equity_data"
        constraints = [
            models.UniqueConstraint(
                fields=["date", "ticker", "asset_manager"],
                name="uniq_equity_date_ticker_manager",
            )
        ]
        indexes = [
            models.Index(fields=["date"]),
            models.Index(fields=["ticker"]),
            models.Index(fields=["asset_manager"]),
        ]



class FXData(models.Model):

    # Identifiers
    date = models.DateField()
    ticker = models.CharField(max_length=16)                 # instrument issuer etc.
    sector = models.CharField(max_length=128, null=True, blank=True)
    industry = models.CharField(max_length=128, null=True, blank=True)

    currency_pair = models.CharField(max_length=16)          # e.g. USD/AUD
    foreign_revenue_ratio = models.FloatField(null=True, blank=True)

    # FX Prices / Returns
    fx_rate = models.FloatField(null=True, blank=True)
    fx_return = models.FloatField(null=True, blank=True)
    
    fx_volatility_20d = models.FloatField(null=True, blank=True)
    fx_volatility_30d = models.FloatField(null=True, blank=True)
    fx_volatility = models.FloatField(null=True, blank=True)

    # Interest, Carry, and Risk
    interest_diff = models.FloatField(null=True, blank=True)
    carry_daily = models.FloatField(null=True, blank=True)
    return_carry_adj = models.FloatField(null=True, blank=True)

    # Position / Hedging
    position_size = models.FloatField(null=True, blank=True)
    hedge_ratio = models.FloatField(null=True, blank=True)
    exposure_amount = models.FloatField(null=True, blank=True)

    # PnL
    fx_pnl = models.FloatField(null=True, blank=True)
    carry_pnl = models.FloatField(null=True, blank=True)
    total_pnl = models.FloatField(null=True, blank=True)
    expected_pnl = models.FloatField(null=True, blank=True)

    # VaR
    VaR_95 = models.FloatField(null=True, blank=True)
    VaR_99 = models.FloatField(null=True, blank=True)
    value_at_risk = models.FloatField(null=True, blank=True)

    # Market / Volume
    volume = models.BigIntegerField(null=True, blank=True)
    sharpe_like_ratio = models.FloatField(null=True, blank=True)

    is_warmup = models.BooleanField(null=True, blank=True)

    # Macro
    gdp = models.FloatField(null=True, blank=True)
    unrate = models.FloatField(null=True, blank=True)
    fedfunds = models.FloatField(null=True, blank=True)
    cpi = models.FloatField(null=True, blank=True)

    # Model prediction
    predicted_volatility_21d = models.FloatField(null=True, blank=True)

    class Meta:
        db_table = "fx_data"
        constraints = [
            models.UniqueConstraint(
                fields=["date", "ticker", "currency_pair"],
                name="uniq_fx_date_ticker_pair"
            )
        ]
        indexes = [
            models.Index(fields=["date"]),
            models.Index(fields=["ticker"]),
            models.Index(fields=["currency_pair"])
        ]





class BondData(models.Model):

    # Identifiers
    bond_id = models.CharField(max_length=64)
    ticker = models.CharField(max_length=16)
    sector = models.CharField(max_length=128, null=True, blank=True)
    industry = models.CharField(max_length=128, null=True, blank=True)
    credit_rating = models.CharField(max_length=32, null=True, blank=True)

    # Bond specifics
    coupon_rate = models.FloatField(null=True, blank=True)
    issue_date = models.DateField(null=True, blank=True)
    maturity_date = models.DateField(null=True, blank=True)
    maturity_years = models.FloatField(null=True, blank=True)

    # Market + Calculation
    date = models.DateField()
    benchmark_yield = models.FloatField(null=True, blank=True)
    corporate_yield = models.FloatField(null=True, blank=True)
    credit_spread = models.FloatField(null=True, blank=True)
    bond_price = models.FloatField(null=True, blank=True)
    yield_to_maturity = models.FloatField(null=True, blank=True)
    
    implied_hazard = models.FloatField(null=True, blank=True)
    implied_pd_annual = models.FloatField(null=True, blank=True)
    implied_pd_multi_year = models.FloatField(null=True, blank=True)
    implied_rating = models.CharField(max_length=32, null=True, blank=True)

    market_synthetic_score = models.FloatField(null=True, blank=True)

    # Macro
    DGS10 = models.FloatField(null=True, blank=True)
    DGS10_ma = models.FloatField(null=True, blank=True)
    dgs10_anom = models.FloatField(null=True, blank=True)
    gdp = models.FloatField(null=True, blank=True)
    unrate = models.FloatField(null=True, blank=True)
    cpi = models.FloatField(null=True, blank=True)
    fedfunds = models.FloatField(null=True, blank=True)

    # Predictions
    pred_spread_5d = models.FloatField(null=True, blank=True)
    pred_pd_21d = models.FloatField(null=True, blank=True)

    class Meta:
        db_table = "bond_data"
        constraints = [
            models.UniqueConstraint(
                fields=["date", "bond_id"],
                name="uniq_bond_date_id",
            )
        ]
        indexes = [
            models.Index(fields=["date"]),
            models.Index(fields=["bond_id"]),
            models.Index(fields=["ticker"]),
        ]

    def __str__(self):
        return f"{self.bond_id} - {self.date}"



class CommodityData(models.Model):

    # Identifiers
    date = models.DateField()
    ticker = models.CharField(max_length=16)
    asset_manager = models.CharField(max_length=128, null=True, blank=True)
    sector = models.CharField(max_length=128, null=True, blank=True)
    industry = models.CharField(max_length=128, null=True, blank=True)
    commodity = models.CharField(max_length=64)

    # Price / volume
    open = models.FloatField(null=True, blank=True)
    high = models.FloatField(null=True, blank=True)
    low = models.FloatField(null=True, blank=True)
    close = models.FloatField(null=True, blank=True)
    volume = models.BigIntegerField(null=True, blank=True)

    daily_return = models.FloatField(null=True, blank=True)
    log_return = models.FloatField(null=True, blank=True)
    vol_20d = models.FloatField(null=True, blank=True)

    sensitivity = models.FloatField(null=True, blank=True)
    hedge_ratio = models.FloatField(null=True, blank=True)

    mtm_value = models.FloatField(null=True, blank=True)
    exposure_amount = models.FloatField(null=True, blank=True)
    commodity_pnl = models.FloatField(null=True, blank=True)

    VaR_95 = models.FloatField(null=True, blank=True)
    VaR_99 = models.FloatField(null=True, blank=True)

    # Macro data
    gdp = models.FloatField(null=True, blank=True)
    unrate = models.FloatField(null=True, blank=True)
    cpi = models.FloatField(null=True, blank=True)
    fedfunds = models.FloatField(null=True, blank=True)

    pred_vol21 = models.FloatField(null=True, blank=True)

    class Meta:
        db_table = "commodity_data"
        constraints = [
            models.UniqueConstraint(
                fields=["date", "ticker", "commodity", "asset_manager"],
                name="uniq_commodity_date_ticker_mgr",
            )
        ]
        indexes = [
            models.Index(fields=["date"]),
            models.Index(fields=["ticker"]),
            models.Index(fields=["commodity"]),
        ]

    def __str__(self):
        return f"{self.ticker} - {self.date}"




class LoanData(models.Model):

    # Identifiers
    loan_id = models.CharField(max_length=64)
    ticker = models.CharField(max_length=16)

    sector = models.CharField(max_length=128, null=True, blank=True)
    industry = models.CharField(max_length=128, null=True, blank=True)

    currency = models.CharField(max_length=16)

    # Monthly timestamp
    month = models.DateField()    # this replaces raw "date", always first day of month

    issue_date = models.DateField(null=True, blank=True)
    maturity_date = models.DateField(null=True, blank=True)

    rate_type = models.CharField(max_length=32, null=True, blank=True)
    coupon_rate = models.FloatField(null=True, blank=True)

    spread_bps = models.FloatField(null=True, blank=True)
    spread_rate = models.FloatField(null=True, blank=True)

    notional_usd = models.FloatField(null=True, blank=True)

    credit_rating = models.CharField(max_length=32, null=True, blank=True)
    credit_spread = models.FloatField(null=True, blank=True)
    yield_to_maturity = models.FloatField(null=True, blank=True)

    fx_rate = models.FloatField(null=True, blank=True)
    fx_volatility = models.FloatField(null=True, blank=True)
    carry_daily = models.FloatField(null=True, blank=True)

    close = models.FloatField(null=True, blank=True)
    vol_20d = models.FloatField(null=True, blank=True)

    gdp = models.FloatField(null=True, blank=True)
    unrate = models.FloatField(null=True, blank=True)
    cpi = models.FloatField(null=True, blank=True)
    fedfunds = models.FloatField(null=True, blank=True)

    loan_age_months = models.IntegerField(null=True, blank=True)
    time_to_maturity_months = models.IntegerField(null=True, blank=True)

    interest_income = models.FloatField(null=True, blank=True)

    exposure_pct_collateralized = models.FloatField(null=True, blank=True)
    macro_stress_score = models.FloatField(null=True, blank=True)
    volatility_index = models.FloatField(null=True, blank=True)

    credit_spread_ratio = models.FloatField(null=True, blank=True)
    profitability_ratio = models.FloatField(null=True, blank=True)
    utilization_ratio = models.FloatField(null=True, blank=True)

    counterparty = models.CharField(max_length=128, null=True, blank=True)

    funding_cost = models.FloatField(null=True, blank=True)
    liquidity_score = models.FloatField(null=True, blank=True)

    pred_credit_spread = models.FloatField(null=True, blank=True)
    PD = models.FloatField(null=True, blank=True)
    LGD = models.FloatField(null=True, blank=True)
    EAD = models.FloatField(null=True, blank=True)
    Expected_Loss = models.FloatField(null=True, blank=True)

    carry_pnl_current = models.FloatField(null=True, blank=True)
    carry_pnl_cumulative = models.FloatField(null=True, blank=True)
    spread_pnl = models.FloatField(null=True, blank=True)
    total_pnl = models.FloatField(null=True, blank=True)

    RAROC = models.FloatField(null=True, blank=True)

    stage = models.IntegerField(null=True, blank=True)

    class Meta:
        db_table = "loan_data"

        constraints = [
            models.UniqueConstraint(
                fields=["loan_id", "month"],
                name="uniq_loan_month"
            )
        ]

        indexes = [
            models.Index(fields=["month"]),
            models.Index(fields=["loan_id"]),
            models.Index(fields=["ticker"]),
        ]

    def __str__(self):
        return f"{self.loan_id} - {self.month}"




class DerivativeData(models.Model):

    # Identifiers
    trade_id = models.CharField(max_length=128, unique=False)
    date = models.DateField()
    counterparty = models.CharField(max_length=128, null=True, blank=True)

    asset_class = models.CharField(max_length=64)
    derivative_type = models.CharField(max_length=64)

    ticker = models.CharField(max_length=32, null=True, blank=True)
    sector = models.CharField(max_length=128, null=True, blank=True)
    industry = models.CharField(max_length=128, null=True, blank=True)

    # Trade financials
    underlying_price = models.FloatField(null=True, blank=True)
    notional = models.FloatField(null=True, blank=True)

    delta = models.FloatField(null=True, blank=True)
    gamma = models.FloatField(null=True, blank=True)
    vega = models.FloatField(null=True, blank=True)

    exposure_before_collateral = models.FloatField(null=True, blank=True)
    initial_margin_rate = models.FloatField(null=True, blank=True)

    collateral_type = models.CharField(max_length=32, null=True, blank=True)
    haircut = models.FloatField(null=True, blank=True)

    required_collateral = models.FloatField(null=True, blank=True)
    collateral_value = models.FloatField(null=True, blank=True)
    effective_collateral = models.FloatField(null=True, blank=True)

    net_exposure = models.FloatField(null=True, blank=True)
    collateral_ratio = models.FloatField(null=True, blank=True)

    margin_call_flag = models.BooleanField(null=True, blank=True)
    margin_call_amount = models.FloatField(null=True, blank=True)

    delta_equivalent_exposure = models.FloatField(null=True, blank=True)

    maturity_date = models.DateField(null=True, blank=True)
    tenor_years = models.FloatField(null=True, blank=True)

    pnl = models.FloatField(null=True, blank=True)

    # Macro
    gdp = models.FloatField(null=True, blank=True)
    unrate = models.FloatField(null=True, blank=True)
    cpi = models.FloatField(null=True, blank=True)
    fedfunds = models.FloatField(null=True, blank=True)

    class Meta:
        db_table = "derivative_data"

        constraints = [
            models.UniqueConstraint(
                fields=["trade_id", "date"],
                name="uniq_trade_date",
            )
        ]

        indexes = [
            models.Index(fields=["date"]),
            models.Index(fields=["ticker"]),
            models.Index(fields=["counterparty"]),
        ]

    def __str__(self):
        return f"{self.trade_id} - {self.date}"



class CollateralData(models.Model):

    # Identifiers
    date = models.DateField()
    asset_class = models.CharField(max_length=64)
    ticker = models.CharField(max_length=32, null=True, blank=True)
    sector = models.CharField(max_length=128, null=True, blank=True)
    industry = models.CharField(max_length=128, null=True, blank=True)
    counterparty = models.CharField(max_length=128, null=True, blank=True)

    # Agreement + jurisdiction
    agreement_type = models.CharField(max_length=64, null=True, blank=True)
    jurisdiction = models.CharField(max_length=64, null=True, blank=True)

    collateral_type = models.CharField(max_length=64, null=True, blank=True)

    haircut = models.FloatField(null=True, blank=True)
    initial_margin_rate = models.FloatField(null=True, blank=True)

    # Values + exposures
    exposure_before_collateral = models.FloatField(null=True, blank=True)
    required_collateral = models.FloatField(null=True, blank=True)
    collateral_value = models.FloatField(null=True, blank=True)
    effective_collateral = models.FloatField(null=True, blank=True)
    net_exposure = models.FloatField(null=True, blank=True)

    collateral_ratio = models.FloatField(null=True, blank=True)

    margin_call_flag = models.BooleanField(null=True, blank=True)
    margin_call_amount = models.FloatField(null=True, blank=True)

    reuse_flag = models.BooleanField(null=True, blank=True)
    reused_value = models.FloatField(null=True, blank=True)

    funding_cost = models.FloatField(null=True, blank=True)
    liquidity_score = models.FloatField(null=True, blank=True)

    trade_id = models.CharField(max_length=128, null=True, blank=True)
    collateral_id = models.CharField(max_length=128, null=True, blank=True)

    # Macro
    gdp = models.FloatField(null=True, blank=True)
    unrate = models.FloatField(null=True, blank=True)
    cpi = models.FloatField(null=True, blank=True)
    fedfunds = models.FloatField(null=True, blank=True)

    class Meta:
        db_table = "collateral_data"

        constraints = [
            models.UniqueConstraint(
                fields=["date","trade_id","collateral_id"],
                name="uniq_collateral_date_trade_collateral",
            )
        ]

        indexes = [
            models.Index(fields=["date"]),
            models.Index(fields=["trade_id"]),
            models.Index(fields=["counterparty"]),
        ]



class CollateralModelData(models.Model):

    date = models.DateField()

    asset_class = models.CharField(max_length=64)
    ticker = models.CharField(max_length=32, null=True, blank=True)
    counterparty = models.CharField(max_length=128, null=True, blank=True)

    exposure_before_collateral = models.FloatField(null=True, blank=True)
    required_collateral = models.FloatField(null=True, blank=True)
    collateral_value = models.FloatField(null=True, blank=True)
    effective_collateral = models.FloatField(null=True, blank=True)
    net_exposure = models.FloatField(null=True, blank=True)

    margin_call_amount = models.FloatField(null=True, blank=True)

    funding_cost = models.FloatField(null=True, blank=True)
    liquidity_score = models.FloatField(null=True, blank=True)
    collateral_ratio = models.FloatField(null=True, blank=True)

    margin_call_flag = models.BooleanField(null=True, blank=True)

    class Meta:
        db_table = "collateral_model_data"

        constraints = [
            models.UniqueConstraint(
                fields=["date","ticker","counterparty", "asset_class"],
                name="uniq_collateral_model_date_ticker_counterparty_asset_class",
            ),
        ]

        indexes = [
            models.Index(fields=["date"]),
            models.Index(fields=["ticker"]),
            models.Index(fields=["counterparty"]),
            models.Index(fields=["asset_class"]),
        ]
