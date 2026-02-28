import pandas as pd
import numpy as np
import xgboost as xgb
import joblib
import boto3
from io import BytesIO
from snowflake.connector.pandas_tools import write_pandas
from connections.snowflake_conn import get_snowflake_conn


S3_BUCKET = "pushparag-loan-bucket"
s3 = boto3.client("s3")

OUTPUT_TABLE = "LOANS"


# --------------------------------------------------------------------
# SAFE LABEL TRANSFORM
# --------------------------------------------------------------------
def safe_label_transform(encoder, series):
    series = series.astype(str)
    known = set(encoder.classes_)
    series = series.apply(lambda x: x if x in known else "__UNK__")

    if "__UNK__" not in encoder.classes_:
        encoder.classes_ = np.append(encoder.classes_, "__UNK__")

    return encoder.transform(series)


# --------------------------------------------------------------------
# RISK METRICS
# --------------------------------------------------------------------
def enhance_loan_risk_metrics(df):

    df = df.copy()

    df["loan_age_months"] = df["loan_age_months"].fillna(0)
    df["time_to_maturity_months"] = df["time_to_maturity_months"].fillna(0)

    df["PD"] = np.clip(df["pred_credit_spread"] / 10, 0.01, 0.25)
    df["LGD"] = 0.45
    df["EAD"] = df["notional_usd"]
    df["Expected_Loss"] = df["PD"] * df["LGD"] * df["EAD"]

    df["carry_pnl_current"] = (df["coupon_rate"] * df["EAD"]) / 12
    df["carry_pnl_cumulative"] = df["carry_pnl_current"] * df["loan_age_months"]

    spread_diff = df["pred_credit_spread"] - df["credit_spread"]
    df["spread_pnl"] = spread_diff * df["EAD"] / 10000

    df["total_pnl"] = df["carry_pnl_current"] + df["spread_pnl"]

    df["RAROC"] = np.where(
        df["Expected_Loss"] > 0,
        df["total_pnl"] / df["Expected_Loss"],
        np.nan,
    )

    ratio = np.where(
        df["credit_spread"] > 0,
        df["pred_credit_spread"] / df["credit_spread"],
        1.0,
    )

    df["stage"] = np.where(ratio > 1.5, "Stage 2", "Stage 1")

    return df


# --------------------------------------------------------------------
# AIRFLOW CALLABLE
# --------------------------------------------------------------------
def run_loans_model_pipeline():

    print("Loading enriched loans...")
    obj = s3.get_object(
        Bucket=S3_BUCKET,
        Key="loan_enriched_fx_bonds_commod_derivatives_collateral.parquet",
    )
    loans = pd.read_parquet(BytesIO(obj["Body"].read()))

    print("Loading macro...")
    macro_obj = s3.get_object(Bucket=S3_BUCKET, Key="macro_data.csv")
    macro = pd.read_csv(BytesIO(macro_obj["Body"].read()))

    loans["date"] = pd.to_datetime(loans["date"], errors="coerce")
    macro["date"] = pd.to_datetime(macro["date"], errors="coerce")

    loans["month_year"] = loans["date"].dt.to_period("M").astype(str)
    macro["month_year"] = macro["date"].dt.to_period("M").astype(str)

    # -------------------------------------------------
    # LAST MONTH
    # -------------------------------------------------
    with get_snowflake_conn() as ctx:
        cur = ctx.cursor()
        cur.execute('SELECT MAX("date") FROM "LOANS"')
        last_date = cur.fetchone()[0]

    last_month = pd.Period(last_date, freq="M") if last_date else None
    print("Last month:", last_month)

    if last_month:
        loans = loans[loans["date"].dt.to_period("M") > last_month]

    if loans.empty:
        print("No new rows.")
        return "NO_NEW_DATA"

    macro = macro.drop(columns=["date"])
    loans = loans.merge(macro, on="month_year", how="left")
    loans.drop(columns=["month_year"], inplace=True)

    # -------------------------------------------------
    # FEATURE ENGINEERING
    # -------------------------------------------------
    for c in ["issue_date", "maturity_date"]:
        loans[c] = pd.to_datetime(loans[c], errors="coerce")

    loans["spread_rate"] = loans["spread_bps"] / 10000.0

    loans["loan_age_months"] = (
        (loans["date"] - loans["issue_date"]).dt.days / 30
    ).clip(lower=0)

    loans["time_to_maturity_months"] = (
        (loans["maturity_date"] - loans["date"]).dt.days / 30
    ).clip(lower=0)

    loans = loans.fillna(0).infer_objects(copy=False)

    # -------------------------------------------------
    # LOAD MODEL
    # -------------------------------------------------
    print("Loading model...")

    model_obj = s3.get_object(
        Bucket=S3_BUCKET,
        Key="loans_model_creditspread_xgb.json",
    )

    booster = xgb.XGBRegressor()
    booster.load_model(bytearray(model_obj["Body"].read()))

    features = joblib.load(
        BytesIO(s3.get_object(Bucket=S3_BUCKET, Key="loans_features.pkl")["Body"].read())
    )

    encoders = joblib.load(
        BytesIO(s3.get_object(Bucket=S3_BUCKET, Key="loans_label_encoders.pkl")["Body"].read())
    )

    original = loans.copy()

    for col, enc in encoders.items():
        loans[col] = safe_label_transform(enc, loans[col])

    for f in features:
        if f not in loans.columns:
            loans[f] = 0

    loans["pred_credit_spread"] = booster.predict(loans[features])

    for col in encoders.keys():
        loans[col] = original[col]

    loans = enhance_loan_risk_metrics(loans)

    # -------------------------------------------------
    # HARD CLEAN (prevents ALL Snowflake errors)
    # -------------------------------------------------
    loans.columns = loans.columns.str.lower()
    loans = loans.loc[:, ~loans.columns.duplicated()]
    loans = loans.loc[:, ~loans.columns.str.endswith(("_x", "_y"))]

    for col in ["credit_rating", "counterparty", "stage"]:
        if col in loans.columns:
            loans[col] = loans[col].astype(str)

    loans.reset_index(drop=True, inplace=True)

    # -------------------------------------------------
    # WRITE
    # -------------------------------------------------
    print("Writing to Snowflake...")

    with get_snowflake_conn() as ctx:
        success, nchunks, nrows, _ = write_pandas(
            ctx,
            loans,
            OUTPUT_TABLE,
            chunk_size=100000,
            quote_identifiers=True,
        )

    print(f"SUCCESS={success} rows={nrows}")

    return "SUCCESS"