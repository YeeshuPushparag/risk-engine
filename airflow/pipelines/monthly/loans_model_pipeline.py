import pandas as pd
import numpy as np
import xgboost as xgb
import joblib
import boto3
from io import BytesIO
from snowflake.connector.pandas_tools import write_pandas
from connections.snowflake_conn import get_snowflake_conn

S3_BUCKET = "pushparag-loan-bucket"
OUTPUT_TABLE = "LOANS"

s3 = boto3.client("s3")


# =========================================================
# SAFE LABEL ENCODING
# =========================================================
def safe_label_transform(encoder, series):
    series = series.astype(str)
    known = set(encoder.classes_)
    series = series.apply(lambda x: x if x in known else "__UNK__")
    if "__UNK__" not in encoder.classes_:
        encoder.classes_ = np.append(encoder.classes_, "__UNK__")
    return encoder.transform(series)


# =========================================================
# ENHANCE LOAN RISK METRICS
# =========================================================
def enhance_loan_risk_metrics(df):
    df = df.copy()

    required_cols = ["close", "pred_credit_spread", "credit_spread",
                     "coupon_rate", "notional_usd", "loan_age_months",
                     "time_to_maturity_months"]
    for col in required_cols:
        if col not in df.columns:
            df[col] = np.nan

    df["loan_age_months"].fillna(0, inplace=True)
    df["time_to_maturity_months"].fillna(0, inplace=True)

    # PD / LGD / EAD
    df["PD"] = np.clip(df["pred_credit_spread"] / 10, 0.01, 0.25)
    df["LGD"] = 0.45
    df["EAD"] = df["notional_usd"]

    # Expected Loss
    df["Expected_Loss"] = df["PD"] * df["LGD"] * df["EAD"]

    # Carry P&L
    df["carry_pnl_current"] = (df["coupon_rate"] * df["EAD"]) / 12
    df["carry_pnl_cumulative"] = df["carry_pnl_current"] * df["loan_age_months"]

    # Spread P&L
    df["spread_diff"] = df["pred_credit_spread"] - df["credit_spread"]
    df["spread_pnl"] = df["spread_diff"] * df["EAD"] / 10000

    # Total P&L
    df["total_pnl"] = df["carry_pnl_current"] + df["spread_pnl"]

    # RAROC
    df["RAROC"] = np.where(df["Expected_Loss"] > 0,
                           df["total_pnl"] / df["Expected_Loss"], np.nan)

    # Stage
    df["PD_change_ratio"] = np.where(df["credit_spread"] > 0,
                                     df["pred_credit_spread"] / df["credit_spread"],
                                     1.0)
    df["stage"] = np.where(df["PD_change_ratio"] > 1.5, 2, 1)

    df.drop(["spread_diff", "PD_change_ratio"], axis=1, inplace=True)
    return df


# =========================================================
# HARDEN TYPES FOR SNOWFLAKE / PYARROW
# =========================================================
def enforce_arrow_safe_types(df):
    for col in ["date", "issue_date", "maturity_date"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    numeric_cols = df.select_dtypes(include=["number"]).columns
    df[numeric_cols] = df[numeric_cols].fillna(0)
    object_cols = df.select_dtypes(include=["object"]).columns
    df[object_cols] = df[object_cols].fillna("").astype(str)
    return df


# =========================================================
# AIRFLOW PIPELINE
# =========================================================
def run_loans_model_pipeline():

    # -------------------------
    # LOAD DATA
    # -------------------------
    print("Loading loans and macro data from S3...")
    loans_obj = s3.get_object(Bucket=S3_BUCKET,
                              Key="loan_enriched_fx_bonds_commod_derivatives_collateral.parquet")
    loans = pd.read_parquet(BytesIO(loans_obj["Body"].read()))
    loans.drop(columns=["notional_oc"], errors="ignore", inplace=True)

    macro_obj = s3.get_object(Bucket=S3_BUCKET, Key="macro_data.csv")
    macro = pd.read_csv(BytesIO(macro_obj["Body"].read()))

    # -------------------------
    # DATE + MONTH-YEAR
    # -------------------------
    loans["date"] = pd.to_datetime(loans["date"], errors="coerce")
    macro["date"] = pd.to_datetime(macro["date"], errors="coerce")

    loans["month_year"] = loans["date"].dt.to_period("M").astype(str)
    macro["month_year"] = macro["date"].dt.to_period("M").astype(str)

    # -------------------------
    # LAST PROCESSED
    # -------------------------
    with get_snowflake_conn() as ctx:
        cur = ctx.cursor()
        cur.execute('SELECT MAX("date") FROM "LOANS"')
        last_date = cur.fetchone()[0]
    last_month = pd.Period(last_date, freq="M") if last_date else None
    if last_month:
        loans = loans[loans["date"].dt.to_period("M") > last_month]
    if loans.empty:
        print("No new rows to process.")
        return "NO_NEW_DATA"

    # -------------------------
    # MERGE MACRO
    # -------------------------
    macro = macro.drop(columns=["date"]).drop_duplicates(subset=["month_year"])
    loans = loans.merge(macro, on="month_year", how="left").drop(columns=["month_year"])

    # -------------------------
    # FEATURE ENGINEERING
    # -------------------------
    for col in ["issue_date", "maturity_date"]:
        loans[col] = pd.to_datetime(loans[col], errors="coerce")

    loans["spread_rate"] = loans["spread_bps"] / 10000.0
    loans["loan_age_months"] = ((loans["date"] - loans["issue_date"]).dt.days / 30).clip(lower=0)
    loans["time_to_maturity_months"] = ((loans["maturity_date"] - loans["date"]).dt.days / 30).clip(lower=0)
    loans["interest_rate_monthly"] = (loans["coupon_rate"] + loans["spread_rate"]) / 12.0
    loans["interest_income"] = loans["notional_usd"] * loans["interest_rate_monthly"]

    if "collateral_value" not in loans.columns:
        loans["collateral_value"] = loans["exposure_before_collateral"] * loans["collateral_ratio"]
    loans["exposure_pct_collateralized"] = (
        loans["collateral_value"] / loans["exposure_before_collateral"]
    ).replace([np.inf, -np.inf], np.nan).fillna(0)

    # -------------------------
    # ROLLING WINDOW FEATURES
    # -------------------------
    loans = loans.sort_values(["loan_id", "date"])
    WINDOW = 3
    loans["cs_roll_std"] = loans.groupby("loan_id")["credit_spread"]\
        .transform(lambda s: s.rolling(WINDOW, min_periods=2).std())
    loans["fxv_roll_std"] = loans.groupby("loan_id")["fx_volatility"]\
        .transform(lambda s: s.rolling(WINDOW, min_periods=2).std())
    loans["cmd_roll_std"] = loans.groupby("loan_id")["vol_20d"]\
        .transform(lambda s: s.rolling(WINDOW, min_periods=2).std())

    for col in ["cs_roll_std", "fxv_roll_std", "cmd_roll_std"]:
        mu, sd = loans[col].mean(), loans[col].std(ddof=0)
        loans[col] = (loans[col] - mu) / sd if sd > 0 else 0
    loans["volatility_index"] = loans[["cs_roll_std", "fxv_roll_std", "cmd_roll_std"]].mean(axis=1)

    # -------------------------
    # MACRO / RATIO FEATURES
    # -------------------------
    loans["credit_spread_ratio"] = (loans["credit_spread"] / loans["yield_to_maturity"])\
        .replace([np.inf, -np.inf], np.nan).fillna(0)
    loans["profitability_ratio"] = (loans["pnl"] / loans["exposure_before_collateral"])\
        .replace([np.inf, -np.inf], np.nan).fillna(0)
    loans["utilization_ratio"] = (loans["net_exposure"] / loans["notional_usd"])\
        .replace([np.inf, -np.inf], np.nan).fillna(0)

    # -------------------------
    # STANDARDIZE MACRO STRESS
    # -------------------------
    for col in ["unrate", "fedfunds"]:
        mu, sd = loans[col].mean(), loans[col].std(ddof=0)
        loans[f"{col}_z"] = (loans[col] - mu) / sd if sd > 0 else 0
    loans["macro_stress_score"] = loans["unrate_z"] + loans["fedfunds_z"]

    # -------------------------
    # LOAD MODEL + ENCODERS
    # -------------------------
    print("Loading ML model + encoders...")
    model = xgb.XGBRegressor()
    model.load_model(bytearray(s3.get_object(Bucket=S3_BUCKET, Key="loans_model_creditspread_xgb.json")["Body"].read()))
    features = joblib.load(BytesIO(s3.get_object(Bucket=S3_BUCKET, Key="loans_features.pkl")["Body"].read()))
    encoders = joblib.load(BytesIO(s3.get_object(Bucket=S3_BUCKET, Key="loans_label_encoders.pkl")["Body"].read()))

    loans_orig = loans.copy()
    for col, enc in encoders.items():
        loans[col] = safe_label_transform(enc, loans[col])

    for f in features:
        if f not in loans.columns:
            loans[f] = 0
    loans["pred_credit_spread"] = model.predict(loans[features].values)

    for col in encoders.keys():
        loans[col] = loans_orig[col]

    # -------------------------
    # ENHANCE RISK METRICS
    # -------------------------
    loans = enhance_loan_risk_metrics(loans)

    # -------------------------
    # STRICT SCHEMA PROJECTION
    # -------------------------
    allowed_columns = [
        "loan_id","ticker","sector","industry","currency","date","issue_date","maturity_date",
        "rate_type","coupon_rate","spread_bps","spread_rate","notional_usd","credit_rating",
        "credit_spread","yield_to_maturity","fx_rate","fx_volatility","carry_daily","close",
        "vol_20d","gdp","unrate","cpi","fedfunds","loan_age_months","time_to_maturity_months",
        "interest_income","exposure_pct_collateralized","macro_stress_score","volatility_index",
        "credit_spread_ratio","profitability_ratio","utilization_ratio","counterparty","funding_cost",
        "liquidity_score","pred_credit_spread","PD","LGD","EAD","Expected_Loss",
        "carry_pnl_current","carry_pnl_cumulative","spread_pnl","total_pnl","RAROC","stage"
    ]
    loans = loans[[c for c in allowed_columns if c in loans.columns]]
    loans = loans.loc[:, ~loans.columns.duplicated()]
    loans = enforce_arrow_safe_types(loans)

    # -------------------------
    # WRITE TO SNOWFLAKE
    # -------------------------
    print("Writing to Snowflake...")
    with get_snowflake_conn() as ctx:
        success, nchunks, nrows, _ = write_pandas(ctx, loans, OUTPUT_TABLE, chunk_size=100_000, quote_identifiers=True)
    print(f"SUCCESS={success}, rows={nrows}")
    return "SUCCESS"