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

    required = [
        "close", "pred_credit_spread", "credit_spread",
        "coupon_rate", "notional_usd",
        "loan_age_months", "time_to_maturity_months"
    ]

    for col in required:
        if col not in df.columns:
            df[col] = np.nan

    df["loan_age_months"] = df["loan_age_months"].fillna(0)
    df["time_to_maturity_months"] = df["time_to_maturity_months"].fillna(0)

    df["PD"] = np.clip(df["pred_credit_spread"] / 10, 0.01, 0.25)
    df["LGD"] = 0.45
    df["EAD"] = df["notional_usd"]
    df["Expected_Loss"] = df["PD"] * df["LGD"] * df["EAD"]

    df["carry_pnl_current"] = (df["coupon_rate"] * df["EAD"]) / 12
    df["carry_pnl_cumulative"] = df["carry_pnl_current"] * df["loan_age_months"]

    df["spread_diff"] = df["pred_credit_spread"] - df["credit_spread"]
    df["spread_pnl"] = df["spread_diff"] * df["EAD"] / 10000

    df["total_pnl"] = df["carry_pnl_current"] + df["spread_pnl"]

    df["RAROC"] = np.where(
        df["Expected_Loss"] > 0,
        df["total_pnl"] / df["Expected_Loss"],
        np.nan,
    )

    df["PD_change_ratio"] = np.where(
        df["credit_spread"] > 0,
        df["pred_credit_spread"] / df["credit_spread"],
        1.0,
    )

    df["stage"] = np.where(df["PD_change_ratio"] > 1.5, "Stage 2", "Stage 1")

    df.drop(["spread_diff", "PD_change_ratio"], axis=1, inplace=True)
    return df


# --------------------------------------------------------------------
# AIRFLOW CALLABLE
# --------------------------------------------------------------------
def run_loans_model_pipeline():

    print("Loading enriched loans from S3...")
    obj = s3.get_object(
        Bucket=S3_BUCKET,
        Key="loan_enriched_fx_bonds_commod_derivatives_collateral.parquet",
    )
    loans = pd.read_parquet(BytesIO(obj["Body"].read()))

    print("Loading macro from S3...")
    macro_obj = s3.get_object(Bucket=S3_BUCKET, Key="macro_data.csv")
    macro = pd.read_csv(BytesIO(macro_obj["Body"].read()))

    loans["date"] = pd.to_datetime(loans["date"], errors="coerce")
    macro["date"] = pd.to_datetime(macro["date"], errors="coerce")

    loans["month_year"] = loans["date"].dt.to_period("M").astype(str)
    macro["month_year"] = macro["date"].dt.to_period("M").astype(str)

    print("Checking Snowflake LOANS last processed month...")

    with get_snowflake_conn() as ctx:
        cur = ctx.cursor()
        cur.execute('SELECT MAX("date") FROM "LOANS"')
        last_date = cur.fetchone()[0]

    # AIRFLOW FIX (handle empty table)
    last_month = pd.Period(last_date, freq="M") if last_date else None
    print(f"Last processed month: {last_month}")

    if last_month:
        new_rows = loans[loans["date"].dt.to_period("M") > last_month]
    else:
        new_rows = loans.copy()

    if new_rows.empty:
        print("No new data in Snowflake.")
        return "NO_NEW_DATA"

    macro = macro.drop(columns=["date"])
    new_rows = new_rows.merge(
        macro, on="month_year", how="left"
    ).drop(columns=["month_year"])

    # ===================== HISTORY =====================
    if last_month:
        with get_snowflake_conn() as ctx:
            cur = ctx.cursor()
            cur.execute('SELECT * FROM "LOANS"')
            prev_final = cur.fetch_pandas_all()

        prev_final["date"] = pd.to_datetime(prev_final["date"])
        working = pd.concat([prev_final, new_rows], ignore_index=True)
    else:
        working = new_rows.copy()

    working = working.sort_values(["loan_id", "date"])

    for c in ["issue_date", "maturity_date"]:
        working[c] = pd.to_datetime(working[c], errors="coerce")

    working["spread_rate"] = working["spread_bps"] / 10000.0

    working["loan_age_months"] = (
        (working["date"] - working["issue_date"]).dt.days / 30
    ).clip(lower=0)

    working["time_to_maturity_months"] = (
        (working["maturity_date"] - working["date"]).dt.days / 30
    ).clip(lower=0)

    new_final = working.copy()

    new_final = new_final.fillna(0)

    # AIRFLOW FIX — stabilize dtypes after fillna
    new_final = new_final.infer_objects(copy=False)

    # ===================== ML =====================
    print("Loading ML model from S3...")

    model_obj = s3.get_object(
        Bucket=S3_BUCKET,
        Key="loans_model_creditspread_xgb.json",
    )

    booster = xgb.XGBRegressor()
    booster.load_model(bytearray(model_obj["Body"].read()))

    feat_obj = s3.get_object(Bucket=S3_BUCKET, Key="loans_features.pkl")
    features = joblib.load(BytesIO(feat_obj["Body"].read()))

    enc_obj = s3.get_object(Bucket=S3_BUCKET, Key="loans_label_encoders.pkl")
    encoders = joblib.load(BytesIO(enc_obj["Body"].read()))

    df_orig = new_final.copy()

    for col, enc in encoders.items():
        new_final[col] = safe_label_transform(enc, new_final[col])

    for f in features:
        if f not in new_final.columns:
            new_final[f] = 0

    new_final["pred_credit_spread"] = booster.predict(
        new_final.reindex(columns=features)
    )

    for col in encoders.keys():
        new_final[col] = df_orig[col]

    new_final = enhance_loan_risk_metrics(new_final)

    # ===================== AIRFLOW CRITICAL FIXES =====================
    for col in ["credit_rating", "counterparty"]:
        if col in new_final.columns:
            new_final[col] = new_final[col].astype(str)

    new_final = new_final.reset_index(drop=True)

    # ===================== WRITE =====================
    print("Writing new rows to Snowflake LOANS...")

    with get_snowflake_conn() as ctx:
        success, nchunks, nrows, _ = write_pandas(
            ctx,
            new_final,
            OUTPUT_TABLE,
            chunk_size=100000,
            quote_identifiers=True,
        )

    print(f"Snowflake upload success={success}, rows={nrows}, chunks={nchunks}")

    return "SUCCESS"