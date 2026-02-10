import os
import pandas as pd
import numpy as np
from scipy.stats import norm
from datetime import datetime
import xgboost as xgb
import joblib
import boto3
from io import BytesIO, StringIO
from connections.snowflake_conn import get_snowflake_conn
from connections.postgre_conn import get_postgre_conn
from snowflake.connector.pandas_tools import write_pandas



# =========================
# CONSTANTS — UNCHANGED
# =========================
Z_95 = norm.ppf(0.05)
Z_99 = norm.ppf(0.01)
ALPHA_95 = 0.95
ALPHA_99 = 0.99

MIN_VOLATILITY = 0.01
MIN_VaR_95 = 0.005
MIN_VaR_99 = 0.01
MIN_CVaR_95 = 0.007
MIN_CVaR_99 = 0.015

S3_BUCKET = "daily-equity-portfolio"
s3 = boto3.client("s3")


# =========================
# FINANCIAL ENRICHMENT
# =========================
def financial_enrichment(df):
    td = df["totalDebt"]
    ta = df["totalAssets"]

    df["interest_coverage_proxy"] = df["ebitda"] / (0.05 * td)
    df.loc[(td <= 0) | td.isna(), "interest_coverage_proxy"] = np.nan

    df["maturity_proxy"] = df["longTermDebt"] / td
    df.loc[(td <= 0) | td.isna(), "maturity_proxy"] = np.nan

    valid = (ta > 0) & (td > 0)

    wc_ta = (ta - td) / ta
    re_ta = df["netIncome"] / ta
    ebit_ta = (df["ebitda"] - 0.05 * td) / ta
    mve_tl = df["marketCap"] / td
    s_ta = df["revenue"] / ta

    df["Altman_Z"] = (
        1.2 * wc_ta +
        1.4 * re_ta +
        3.3 * ebit_ta +
        0.6 * mve_tl +
        1.0 * s_ta
    ).where(valid, np.nan)

    df["free_cash_flow"] = df["ebitda"] - 0.10 * df["revenue"]
    df["cash_and_equivalents"] = df["totalAssets"] - df["totalDebt"] - 0.5 * df["revenue"]

    bins = [-np.inf, 0.5, 0.8, 1.23, 1.8, 3.0, 4.2, 5.85, 7.0, 8.15, np.inf]
    labels = ["D", "C", "CC", "CCC", "B", "BB", "BBB", "A", "AA", "AAA"]
    df["credit_rating"] = pd.cut(df["Altman_Z"], bins=bins, labels=labels)

    df["mtm_value"] = df["close"] * df["Shares"]

    return df


# =========================
# RISK ENRICHMENT
# =========================
def risk_enrichment(df):
    numeric_cols = [
        "total_value","total_percent","Shares","volume","mtm_value",
        "daily_return","vol_5d","vol_20d","volatility_21d","downside_risk",
        "sharpe_ratio","sortino_ratio","beta",
        "avg_volume_10d",
        "gdp","unrate","cpi","fedfunds",
        "totalDebt","shortTermDebt","longTermDebt","totalAssets",
        "ebitda","revenue","netIncome","marketCap","dividendYield",
        "Altman_Z","free_cash_flow","cash_and_equivalents",
    ]

    for c in numeric_cols:
        if c not in df.columns:
            df[c] = np.nan
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df["daily_pnl"] = df["mtm_value"] * df["daily_return"]

    total_mtm = df.groupby(["asset_manager", "date"])["mtm_value"].transform("sum")
    df["portfolio_weight"] = df["mtm_value"] / total_mtm
    df["portfolio_weight"] = df["portfolio_weight"].fillna(0)

    df["sector_exposure"] = df.groupby(["asset_manager","date","sector"])["portfolio_weight"].transform("sum")
    df["industry_exposure"] = df.groupby(["asset_manager","date","industry"])["portfolio_weight"].transform("sum")

    df["beta"] = df["beta"].fillna(0)
    df["beta_weighted"] = df["portfolio_weight"] * df["beta"]
    df["sector_beta_weighted"] = df.groupby(["asset_manager","date","sector"])["beta_weighted"].transform("sum")

    df["daily_sigma"] = (
        df["volatility_21d"]
        .fillna(df["vol_20d"])
        .fillna(df["vol_5d"])
        .fillna(MIN_VOLATILITY)
    ).clip(lower=MIN_VOLATILITY)

    df["daily_mu"] = df["daily_return"].fillna(0)

    df["daily_VaR_95"] = (-(df["daily_mu"] + Z_95 * df["daily_sigma"])).clip(lower=MIN_VaR_95)
    df["daily_VaR_99"] = (-(df["daily_mu"] + Z_99 * df["daily_sigma"])).clip(lower=MIN_VaR_99)

    z95 = norm.ppf(1 - ALPHA_95)
    z99 = norm.ppf(1 - ALPHA_99)

    df["daily_CVaR_95"] = (
        -df["daily_mu"] + df["daily_sigma"] * (norm.pdf(z95) / ALPHA_95)
    ).clip(lower=MIN_CVaR_95)

    df["daily_CVaR_99"] = (
        -df["daily_mu"] + df["daily_sigma"] * (norm.pdf(z99) / ALPHA_99)
    ).clip(lower=MIN_CVaR_99)

    g = df.groupby(["asset_manager","date"])
    df["weighted_mu"] = df["portfolio_weight"] * df["daily_mu"]
    port_mu = g["weighted_mu"].transform("sum")
    df["daily_portfolio_mu"] = port_mu

    df["w_sigma2"] = (df["portfolio_weight"] * df["daily_sigma"]) ** 2
    port_vol = g["w_sigma2"].transform("sum").apply(np.sqrt).clip(lower=MIN_VOLATILITY)

    df["daily_portfolio_ex_ante_volatility"] = port_vol

    df["daily_portfolio_VaR_95"] = (-(port_mu + Z_95 * port_vol)).clip(lower=MIN_VaR_95)
    df["daily_portfolio_VaR_99"] = (-(port_mu + Z_99 * port_vol)).clip(lower=MIN_VaR_99)

    df["daily_portfolio_CVaR_95"] = (
        -port_mu + port_vol * (norm.pdf(z95) / ALPHA_95)
    ).clip(lower=MIN_CVaR_95)

    df["daily_portfolio_CVaR_99"] = (
        -port_mu + port_vol * (norm.pdf(z99) / ALPHA_99)
    ).clip(lower=MIN_CVaR_99)

    df["top_5_exposure"] = g["portfolio_weight"].transform(lambda s: s.nlargest(5).sum())

    df["sector_sum"] = df.groupby(["asset_manager","date","sector"])["portfolio_weight"].transform("sum")
    df["sector_sum_sq"] = df["sector_sum"] ** 2
    df["HHI_sector"] = g["sector_sum_sq"].transform("sum")

    df["diversification_score"] = 1 / df["HHI_sector"].replace(0, np.nan)

    # Liquidity
    df["avg_volume_10d"] = df["avg_volume_10d"].fillna(0)
    df["Amihud_illiquidity"] = df["daily_return"].abs() / df["avg_volume_10d"].replace(0, np.nan)

    df["turnover_ratio"] = df["volume"] / df["Shares"].replace(0, np.nan)

    # Liquidity risk score (minmax)
    df["liquidity_risk_score"] = df.groupby(["asset_manager","date"])["Amihud_illiquidity"] \
        .transform(lambda x: (x - x.min()) / (x.max() - x.min()) if x.max() != x.min() else 0)
    df["Amihud_illiquidity"] = df["Amihud_illiquidity"].fillna(0)
    df["liquidity_risk_score"] = df["liquidity_risk_score"].fillna(0)
    df["turnover_ratio"] = df["turnover_ratio"].fillna(0)
    df["sector_exposure_pct"] = df["sector_exposure"]

    # Portfolio daily pnl
    df["portfolio_daily_pnl"] = g["daily_pnl"].transform("sum")

    return df


# =========================
# RUN ALL MODELS — SAME LOGIC
# =========================
def run_all_equity_predictions(df):
    model_map = {
        "ret_1d": ("models/equity_model_1d_xgb.json", "models/equity_features_1d.pkl", "pred_ret_1d"),
        "ret_5d": ("models/equity_model_5d_xgb.json", "models/equity_features_5d.pkl", "pred_ret_5d"),
        "ret_21d": ("models/equity_model_21d_xgb.json", "models/equity_features_21d.pkl", "pred_ret_21d"),
        "vol_21d": ("models/equity_model_vol21_xgb.json", "models/equity_features_vol21.pkl", "pred_vol_21d"),
        "down_21d": ("models/equity_model_down21_xgb.json", "models/equity_features_down21.pkl", "pred_downside_21d"),
        "var_21d": ("models/equity_model_var21_xgb.json", "models/equity_features_var21.pkl", "pred_var_21d"),
        "factor_21d": ("models/equity_model_factor21_xgb.json", "models/equity_features_factor21.pkl", "pred_factor_21d"),
        "sector_rotation": ("models/equity_model_sector_rotation_xgb.json", "models/equity_features_sector_rotation.pkl", "pred_sector_rotation"),
        "macro_regime": ("models/equity_model_macro_regime_xgb.json", "models/equity_features_macro_regime.pkl", "pred_macro_regime"),
        "port_ret_1d": ("models/equity_model_portfolio1d_xgb.json", "models/equity_features_portfolio1d.pkl", "pred_port_ret_1d"),
        "port_ret_5d": ("models/equity_model_portfolio5d_xgb.json", "models/equity_features_portfolio5d.pkl", "pred_port_ret_5d"),
        "port_ret_21d": ("models/equity_model_portfolio21d_xgb.json", "models/equity_features_portfolio21d.pkl", "pred_port_ret_21d"),
        "port_var_1d": ("models/equity_model_portfolio_var_1d_xgb.json", "models/equity_features_portfolio_var_1d.pkl", "pred_port_var_1d"),
        "port_var_5d": ("models/equity_model_portfolio_var_5d_xgb.json", "models/equity_features_portfolio_var_5d.pkl", "pred_port_var_5d"),
        "port_var_21d": ("models/equity_model_portfolio_var_21d_xgb.json", "models/equity_features_portfolio_var_21d.pkl", "pred_port_var_21d")
    }

    df.replace([np.inf, -np.inf], np.nan, inplace=True)

    for name, (model_s3_path, feat_path, out_col) in model_map.items():
        # ---- load feature list (training order) ----
        try:
            feat_obj = s3.get_object(Bucket=S3_BUCKET, Key=feat_path)
            features = joblib.load(BytesIO(feat_obj["Body"].read()))
        except Exception:
            df[out_col] = np.nan
            continue

        # ---- CRITICAL CHANGE: force correct feature order ----
        try:
            X = df.reindex(columns=features).fillna(0)
        except Exception:
            df[out_col] = np.nan
            continue

        # ---- load model ----
        try:
            model_obj = s3.get_object(Bucket=S3_BUCKET, Key=model_s3_path)
            booster = xgb.Booster()
            booster.load_model(bytearray(model_obj["Body"].read()))
        except Exception:
            df[out_col] = np.nan
            continue

        dtest = xgb.DMatrix(X)
        raw_pred = booster.predict(dtest)

        if name == "macro_regime":
            df[out_col] = raw_pred.argmax(axis=1)
        else:
            df[out_col] = raw_pred

    return df


# =========================
# FINAL EXECUTION FOR DAG
# =========================
def run_equity_risk_pipeline():
    today = pd.Timestamp.today().normalize()


    # Get last date from Snowflake EQUITY table
    with get_snowflake_conn() as ctx:
        with ctx.cursor() as cs:
            cs.execute('SELECT MAX("date") FROM "EQUITY"')
            last_date = cs.fetchone()[0]

    if last_date is not None:
        last_date = pd.Timestamp(last_date).normalize()
    else:
        last_date = pd.Timestamp("1970-01-01")

    if last_date.date() >= today.date():
        return "NO_NEW_DATA"

    market_obj = s3.get_object(Bucket=S3_BUCKET, Key="historical-equity/market_features.parquet")
    market = pd.read_parquet(BytesIO(market_obj['Body'].read()))
    market["date"] = pd.to_datetime(market["date"], dayfirst=True)

    new_market_rows = market[market["date"].dt.date > last_date.date()]
    if new_market_rows.empty:
        return "NO_NEW_MARKET_ROWS"

    merged_obj = s3.get_object(Bucket=S3_BUCKET, Key="historical-equity/final_merged.parquet")
    merged = pd.read_parquet(BytesIO(merged_obj['Body'].read()))
    if "date" in merged.columns:
        merged = merged.drop(columns=["date"])

    df_new = pd.merge(new_market_rows, merged, on="ticker", how="inner")

    macro_data_obj = s3.get_object(Bucket=S3_BUCKET, Key="historical-equity/macro_data.csv")
    macro_data = pd.read_csv(StringIO(macro_data_obj["Body"].read().decode("utf-8")))
    macro_data["month"] = pd.to_datetime(macro_data["date"], dayfirst=True).dt.to_period("M")
    df_new["month"] = df_new["date"].dt.to_period("M")

    macro_data = macro_data.drop(columns=["date"])

    df_merged = pd.merge(df_new, macro_data, on="month", how="inner")
    df_merged = df_merged.drop(columns=["month"])

    df_merged = financial_enrichment(df_merged)
    df_merged = risk_enrichment(df_merged)
    df_merged = run_all_equity_predictions(df_merged)
    df_merged["total_value"] = df_merged["total_value"] * 1000
    df_merged["date"] = pd.to_datetime(df_merged["date"], dayfirst=True).dt.date

    with get_snowflake_conn() as ctx:
        write_pandas(ctx, df_merged, "EQUITY",
                     chunk_size=20000, quote_identifiers=True)

    # ===================== POSTGRESQL LOAD =====================
    try:

        POSTGRES_COLUMN_MAP = {
            "class": "class_name",
            "CUSIP": "cusip",
            "Shares": "shares",
            "Discretion": "discretion",
            "Sole": "sole",
            "Shared": "shared",
            "None": "none_ownership",
            "totalAssets": "total_assets",
            "totalDebt": "total_debt",
            "shortTermDebt": "short_term_debt",
            "longTermDebt": "long_term_debt",
            "netIncome": "net_income",
            "marketCap": "market_cap",
            "dividendYield": "dividend_yield",
            "Altman_Z": "altman_z",
            "daily_VaR_95": "daily_var_95",
            "daily_VaR_99": "daily_var_99",
            "daily_CVaR_95": "daily_cvar_95",
            "daily_CVaR_99": "daily_cvar_99",
            "daily_portfolio_VaR_95": "daily_portfolio_var_95",
            "daily_portfolio_VaR_99": "daily_portfolio_var_99",
            "daily_portfolio_CVaR_95": "daily_portfolio_cvar_95",
            "daily_portfolio_CVaR_99": "daily_portfolio_cvar_99",
            "HHI_sector": "hhi_sector",
            "Amihud_illiquidity": "amihud_illiquidity",
        }

        df_pg = df_merged.rename(columns=POSTGRES_COLUMN_MAP)

        integer_cols = [
            "volume", "other_manager", "sole", "shared",
            "none_ownership", "pred_macro_regime"
        ]

        for col in integer_cols:
            if col in df_pg.columns:
                df_pg[col] = (
                    df_pg[col]
                    .fillna(0)
                    .astype(float)
                    .round()
                    .astype(int)
                )

   

        with get_postgre_conn() as pg_conn:

            with pg_conn.cursor() as pg_cur:

                pg_cur.execute("""
                    SELECT column_name 
                    FROM information_schema.columns
                    WHERE table_schema='public'
                    AND table_name='equity_data'
                    ORDER BY ordinal_position
                """)

                pg_cols_order = [
                    row[0] for row in pg_cur.fetchall()
                    if row[0].lower() != "id"
                ]

                df_pg = df_pg[pg_cols_order]

                BATCH_SIZE = 100_000
                copy_sql = (
                    f"COPY public.equity_data ({','.join(pg_cols_order)}) "
                    f"FROM STDIN WITH CSV"
                )

                for start in range(0, len(df_pg), BATCH_SIZE):
                    end = start + BATCH_SIZE
                    chunk = df_pg.iloc[start:end]

                    buf = StringIO()
                    chunk.to_csv(buf, index=False, header=False)
                    buf.seek(0)

                    with pg_cur.copy(copy_sql) as copy:
                        copy.write(buf.getvalue())

                pg_conn.commit()


    except Exception as e:
        print("Postgres upload failed:", e)


    return f"UPDATED_ROWS_{len(df_merged)}"
