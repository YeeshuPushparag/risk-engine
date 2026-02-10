import pandas as pd
import numpy as np
import xgboost as xgb
import joblib
import os
import boto3
from io import BytesIO, StringIO
from snowflake.connector.pandas_tools import write_pandas
from connections.snowflake_conn import get_snowflake_conn
from connections.postgre_conn import get_postgre_conn
import psycopg


S3_BUCKET = "monthly-loans"
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

    required = ["close", "pred_credit_spread", "credit_spread", "coupon_rate",
                "notional_usd", "loan_age_months", "time_to_maturity_months"]

    for col in required:
        if col not in df.columns:
            df[col] = np.nan

    df["loan_age_months"].fillna(0, inplace=True)
    df["time_to_maturity_months"].fillna(0, inplace=True)

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
        np.nan
    )

    df["PD_change_ratio"] = np.where(
        df["credit_spread"] > 0,
        df["pred_credit_spread"] / df["credit_spread"],
        1.0
    )

    df["stage"] = np.where(df["PD_change_ratio"] > 1.5, "Stage 2", "Stage 1")

    df.drop(["spread_diff", "PD_change_ratio"], axis=1, inplace=True)
    return df


# --------------------------------------------------------------------
# AIRFLOW CALLABLE
# --------------------------------------------------------------------
def run_loans_model_pipeline():
    # ===================== LOAD INPUTS FROM S3 =====================
    print("Loading enriched loans from S3...")
    obj = s3.get_object(
        Bucket=S3_BUCKET,
        Key="loan_enriched_fx_bonds_commod_derivatives_collateral.parquet",
    )
    loans = pd.read_parquet(BytesIO(obj["Body"].read()))

    print("Loading macro from S3...")
    macro_obj = s3.get_object(Bucket=S3_BUCKET, Key="macro_data.csv")
    macro = pd.read_csv(BytesIO(macro_obj["Body"].read()))

    # Use fast parsing method from previous code
    loans["date"] = pd.to_datetime(loans["date"], dayfirst=True, errors="coerce")
    macro["date"] = pd.to_datetime(macro["date"], dayfirst=True, errors="coerce")

    loans["month_year"] = loans["date"].dt.to_period("M").astype(str)
    macro["month_year"] = macro["date"].dt.to_period("M").astype(str)

    # ===================== FIND LAST MONTH IN SNOWFLAKE =====================
    print("Checking Snowflake LOANS last processed month...")

    with get_snowflake_conn() as ctx:
        cur = ctx.cursor()
        cur.execute('SELECT MAX("date") FROM "LOANS"')
        last_date = cur.fetchone()[0]

    # Convert to month period
    last_month = pd.Period(last_date, freq="M")
    print(f"Last processed month: {last_month}")

    # ===================== FILTER NEW =====================
    new_rows = loans[loans["date"].dt.to_period("M") > last_month]

    if new_rows.empty:
        print("No new data in Snowflake. Done.")
        return "NO_NEW_DATA"

    print("Processing new window:")
    print(f"Date range: {new_rows['date'].min()} -> {new_rows['date'].max()}")

    # Merge with macro data
    macro = macro.drop(columns=["date"])
    new_rows = new_rows.merge(macro, on="month_year", how="left").drop(columns=["month_year"])

    # ===================== BUILD ROLLING HISTORY =====================
    print("Loading SNOWFLAKE existing history for rolling...")

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

    # ===================== FEATURE ENGINEERING =====================
    for c in ["issue_date", "maturity_date"]:
        working[c] = pd.to_datetime(working[c], errors="coerce")

    if working["coupon_rate"].abs().median() >= 1:
        working["coupon_rate"] /= 100.0

    working["spread_rate"] = working["spread_bps"] / 10000.0

    working["loan_age_months"] = ((working["date"] - working["issue_date"]).dt.days / 30).clip(lower=0)
    working["time_to_maturity_months"] = ((working["maturity_date"] - working["date"]).dt.days / 30).clip(lower=0)

    working["interest_rate_monthly"] = (working["coupon_rate"] + working["spread_rate"]) / 12.0
    working["interest_income"] = working["notional_usd"] * working["interest_rate_monthly"]

    if "collateral_value" not in working.columns:
        working["collateral_value"] = working["exposure_before_collateral"] * working["collateral_ratio"]

    working["exposure_pct_collateralized"] = (
        working["collateral_value"] / working["exposure_before_collateral"]
    ).replace([np.inf, -np.inf], np.nan).fillna(0)

    for col in ["unrate", "fedfunds"]:
        mu, sd = working[col].mean(), working[col].std(ddof=0)
        working[f"{col}_z"] = (working[col] - mu) / sd if sd > 0 else 0

    working["macro_stress_score"] = working["unrate_z"] + working["fedfunds_z"]

    # ===================== ROLLING =====================
    working = working.sort_values(["loan_id", "date"])
    WINDOW = 3

    working["cs_roll_std"] = working.groupby("loan_id")["credit_spread"].transform(
        lambda s: s.rolling(WINDOW, min_periods=2).std()
    )

    working["fxv_roll_std"] = working.groupby("loan_id")["fx_volatility"].transform(
        lambda s: s.rolling(WINDOW, min_periods=2).std()
    )

    working["cmd_roll_std"] = working.groupby("loan_id")["vol_20d"].transform(
        lambda s: s.rolling(WINDOW, min_periods=2).std()
    )

    for c in ["cs_roll_std", "fxv_roll_std", "cmd_roll_std"]:
        mu, sd = working[c].mean(), working[c].std(ddof=0)
        working[c] = (working[c] - mu) / sd if sd > 0 else 0

    working["volatility_index"] = working[
        ["cs_roll_std", "fxv_roll_std", "cmd_roll_std"]
    ].mean(axis=1)

    # ===================== ONLY NEW =====================
    if last_month:
        mask = working["date"].dt.to_period("M") > last_month
        new_final = working.loc[mask].copy()
    else:
        new_final = working.copy()

    new_final["credit_spread_ratio"] = (
        new_final["credit_spread"] / new_final["yield_to_maturity"]
    ).replace([np.inf, -np.inf], np.nan)

    new_final["profitability_ratio"] = (
        new_final["pnl"] / new_final["exposure_before_collateral"]
    ).replace([np.inf, -np.inf], np.nan)

    new_final["utilization_ratio"] = (
        new_final["net_exposure"] / new_final["notional_usd"]
    ).replace([np.inf, -np.inf], np.nan)

    new_final = new_final.fillna(0)

    # ===================== ML =====================
    print("Loading ML model from S3...")

    model_obj = s3.get_object(Bucket=S3_BUCKET, Key="loans_model_creditspread_xgb.json")
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

    # ---- Predict ----
    new_final["pred_credit_spread"] = booster.predict(
        new_final.reindex(columns=features)
    )

    for col in encoders.keys():
        new_final[col] = df_orig[col]

    new_final = enhance_loan_risk_metrics(new_final)


    # ===================== WRITE TO SNOWFLAKE =====================
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

    # ===================== WRITE TO POSTGRES =====================
    try:
        print("Uploading to PostgreSQL loan_data...")

        df_pg = new_final.copy()

        df_pg["month"] = (
            pd.to_datetime(df_pg["date"])
            .dt.to_period("M")
            .dt.to_timestamp()
            .dt.date
        )

        if "stage" in df_pg.columns:
            df_pg["stage"] = (
                df_pg["stage"]
                .astype(str)
                .str.extract(r"(\d+)")
                .fillna(0)
                .astype(int)
            )

        for c in ["loan_age_months", "time_to_maturity_months"]:
            if c in df_pg.columns:
                df_pg[c] = (
                    df_pg[c]
                    .fillna(0)
                    .astype(float)
                    .round()
                    .astype(int)
                )

        with get_postgre_conn() as pg_conn:
            with pg_conn.cursor() as pg_cur:

                pg_cur.execute("""SELECT column_name
                                  FROM information_schema.columns
                                  WHERE table_schema='public'
                                  AND table_name='loan_data'
                                  ORDER BY ordinal_position""")

                pg_cols_order = [r[0] for r in pg_cur.fetchall() if r[0] != "id"]

                df_pg = df_pg[pg_cols_order]

                buf = StringIO()
                df_pg.to_csv(buf, index=False, header=False)
                buf.seek(0)

                quoted = [f'"{c}"' for c in pg_cols_order]

                copy_sql = (
                    f"COPY public.loan_data ({','.join(quoted)}) FROM STDIN WITH CSV"
                )

                with pg_cur.copy(copy_sql) as copy:
                    copy.write(buf.getvalue())

                pg_conn.commit()

        print(f"PostgreSQL upload complete. Rows: {len(df_pg)}")

    except Exception as e:
        print("PostgreSQL upload FAILED:", e)

    print("DONE!")
    return "SUCCESS"
