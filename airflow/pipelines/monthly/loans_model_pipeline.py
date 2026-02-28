import pandas as pd
import numpy as np
import xgboost as xgb
import joblib
import boto3
from io import BytesIO, StringIO
from snowflake.connector.pandas_tools import write_pandas
from connections.snowflake_conn import get_snowflake_conn
from connections.postgre_conn import get_postgre_conn  # Postgres connection

S3_BUCKET = "pushparag-loan-bucket"
OUTPUT_TABLE = "LOANS"
s3 = boto3.client("s3")


# =========================
# SAFE LABEL ENCODING
# =========================
def safe_label_transform(encoder, series):
    series = series.astype(str)
    known = set(encoder.classes_)
    series = series.apply(lambda x: x if x in known else "__UNK__")
    if "__UNK__" not in encoder.classes_:
        encoder.classes_ = np.append(encoder.classes_, "__UNK__")
    return encoder.transform(series)


# =========================
# ENHANCE LOAN RISK METRICS
# =========================
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
    df["PD"] = np.clip(df["pred_credit_spread"] / 10, 0.01, 0.25)
    df["LGD"] = 0.45
    df["EAD"] = df["notional_usd"]
    df["Expected_Loss"] = df["PD"] * df["LGD"] * df["EAD"]
    df["carry_pnl_current"] = (df["coupon_rate"] * df["EAD"]) / 12
    df["carry_pnl_cumulative"] = df["carry_pnl_current"] * df["loan_age_months"]
    df["spread_diff"] = df["pred_credit_spread"] - df["credit_spread"]
    df["spread_pnl"] = df["spread_diff"] * df["EAD"] / 10000
    df["total_pnl"] = df["carry_pnl_current"] + df["spread_pnl"]
    df["RAROC"] = np.where(df["Expected_Loss"] > 0, df["total_pnl"] / df["Expected_Loss"], np.nan)
    df["PD_change_ratio"] = np.where(df["credit_spread"] > 0,
                                     df["pred_credit_spread"] / df["credit_spread"], 1.0)
    df["stage"] = np.where(df["PD_change_ratio"] > 1.5, 2, 1)
    df.drop(["spread_diff", "PD_change_ratio"], axis=1, inplace=True)
    return df


# =========================
# HARDEN TYPES FOR SNOWFLAKE
# =========================
def enforce_snowflake_types(df):
    df = df.copy()
    for col in ["date", "issue_date", "maturity_date"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime("%Y-%m-%d").fillna("")

    numeric_cols = df.select_dtypes(include=["number"]).columns
    df[numeric_cols] = df[numeric_cols].fillna(0)

    object_cols = df.select_dtypes(include=["object"]).columns
    df[object_cols] = df[object_cols].fillna("").astype(str)

    if "stage" in df.columns:
        df["stage"] = df["stage"].astype(str).fillna("0")

    df = df.reset_index(drop=True)
    return df


# =========================
# MAIN PIPELINE
# =========================
def run_loans_model_pipeline():
    print("Starting Loans Model Pipeline...")

    # -------------------------
    # LOAD DATA FROM S3
    # -------------------------
    loans_obj = s3.get_object(
        Bucket=S3_BUCKET,
        Key="loan_enriched_fx_bonds_commod_derivatives_collateral.parquet"
    )
    loans = pd.read_parquet(BytesIO(loans_obj["Body"].read()))

    # -------------------------
    # VALID DATES
    # -------------------------
    for col in ["date", "issue_date", "maturity_date"]:
        if col in loans.columns:
            loans[col] = pd.to_datetime(loans[col], errors="coerce")
    loans = loans[loans["date"].notna()]

    today_period = pd.Period(pd.Timestamp.today(), freq="M")
    loans = loans[loans["date"].dt.to_period("M") <= today_period]

    # -------------------------
    # LOAD MACRO DATA
    # -------------------------
    try:
        macro_obj = s3.get_object(Bucket=S3_BUCKET, Key="macro_data.csv")
        macro = pd.read_csv(BytesIO(macro_obj["Body"].read()))
        macro["date"] = pd.to_datetime(macro["date"], errors="coerce")
        macro = macro[macro["date"].notna()]
        loans["month_year"] = loans["date"].dt.to_period("M").astype(str)
        macro["month_year"] = macro["date"].dt.to_period("M").astype(str)
        macro = macro.drop(columns=["date"]).drop_duplicates(subset=["month_year"])
        loans = loans.merge(macro, on="month_year", how="left").drop(columns=["month_year"])
    except s3.exceptions.NoSuchKey:
        print("Macro data not found, proceeding without it")

    # -------------------------
    # FILTER LAST PROCESSED
    # -------------------------
    with get_snowflake_conn() as ctx:
        cur = ctx.cursor()
        cur.execute(f'SELECT MAX("date") FROM "{OUTPUT_TABLE}"')
        last_date = cur.fetchone()[0]

    last_month = pd.Period(last_date, freq="M") if last_date else None
    if last_month:
        loans = loans[loans["date"].dt.to_period("M") > last_month]

    if loans.empty:
        print("No new rows to process.")
        return "NO_NEW_DATA"

    # -------------------------
    # FEATURE ENGINEERING
    # -------------------------
    loans["spread_rate"] = loans.get("spread_bps", 0) / 10000.0
    loans["loan_age_months"] = ((loans["date"] - loans["issue_date"]).dt.days / 30).clip(lower=0)
    loans["time_to_maturity_months"] = ((loans["maturity_date"] - loans["date"]).dt.days / 30).clip(lower=0)
    loans["interest_rate_monthly"] = (loans.get("coupon_rate", 0) + loans["spread_rate"]) / 12.0
    loans["interest_income"] = loans.get("notional_usd", 0) * loans["interest_rate_monthly"]

    if "collateral_value" not in loans.columns and "exposure_before_collateral" in loans.columns:
        loans["collateral_value"] = loans["exposure_before_collateral"] * loans.get("collateral_ratio", 0)
    loans["exposure_pct_collateralized"] = (
        loans.get("collateral_value", 0) / loans.get("exposure_before_collateral", 1)
    ).replace([np.inf, -np.inf], np.nan).fillna(0)

    # -------------------------
    # ROLLING FEATURES
    # -------------------------
    loans = loans.sort_values(["loan_id", "date"])
    WINDOW = 3
    for col, new_col in [("credit_spread", "cs_roll_std"),
                         ("fx_volatility", "fxv_roll_std"),
                         ("vol_20d", "cmd_roll_std")]:
        if col in loans.columns:
            loans[new_col] = loans.groupby("loan_id")[col].transform(lambda s: s.rolling(WINDOW, min_periods=2).std())
            mu, sd = loans[new_col].mean(), loans[new_col].std(ddof=0)
            loans[new_col] = (loans[new_col] - mu) / sd if sd > 0 else 0
        else:
            loans[new_col] = 0
    loans["volatility_index"] = loans[["cs_roll_std", "fxv_roll_std", "cmd_roll_std"]].mean(axis=1)

    # -------------------------
    # MODEL PREDICTION
    # -------------------------
    print("Loading ML model + encoders...")
    model = xgb.XGBRegressor()
    model.load_model(bytearray(s3.get_object(Bucket=S3_BUCKET, Key="loans_model_creditspread_xgb.json")["Body"].read()))
    features = joblib.load(BytesIO(s3.get_object(Bucket=S3_BUCKET, Key="loans_features.pkl")["Body"].read()))
    encoders = joblib.load(BytesIO(s3.get_object(Bucket=S3_BUCKET, Key="loans_label_encoders.pkl")["Body"].read()))

    loans_orig = loans.copy()
    for col, enc in encoders.items():
        if col in loans.columns:
            loans[col] = safe_label_transform(enc, loans[col])
    for f in features:
        if f not in loans.columns:
            loans[f] = 0
    loans["pred_credit_spread"] = model.predict(loans[features].values)
    for col in encoders.keys():
        loans[col] = loans_orig.get(col, loans[col])

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

    # -------------------------
    # WRITE TO SNOWFLAKE
    # -------------------------
    loans_sf = enforce_snowflake_types(loans)
    if not loans_sf.empty:
        print(f"Pushing {len(loans_sf):,} rows to Snowflake...")
        with get_snowflake_conn() as ctx:
            success, nchunks, nrows, _ = write_pandas(
                ctx, loans_sf, OUTPUT_TABLE, chunk_size=100_000, quote_identifiers=True
            )
        print(f"Snowflake push result: SUCCESS={success}, rows={nrows}")

    # =========================
    # WRITE TO POSTGRES
    # =========================
    try:
        print("Uploading to PostgreSQL loan_data...")

        df_pg = loans.copy()

        # Compute month from Snowflake 'date'
        df_pg["month"] = pd.to_datetime(df_pg["date"]).dt.to_period("M").dt.to_timestamp().dt.date

        # Drop 'date' column (Snowflake-only)
        if "date" in df_pg.columns:
            df_pg = df_pg.drop(columns=["date"])

        # Fix stage to int
        if "stage" in df_pg.columns:
            df_pg["stage"] = df_pg["stage"].astype(float).round().astype(int)

        # Round integer columns
        for c in ["loan_age_months", "time_to_maturity_months"]:
            if c in df_pg.columns:
                df_pg[c] = df_pg[c].fillna(0).astype(float).round().astype(int)

        # Align column order with Postgres
        with get_postgre_conn() as pg_conn:
            with pg_conn.cursor() as pg_cur:
                pg_cur.execute("""SELECT column_name
                                  FROM information_schema.columns
                                  WHERE table_schema='public'
                                  AND table_name='loan_data'
                                  ORDER BY ordinal_position""")
                pg_cols_order = [r[0] for r in pg_cur.fetchall() if r[0] != "id"]

                df_pg = df_pg[[c for c in pg_cols_order if c in df_pg.columns]]

                buf = StringIO()
                df_pg.to_csv(buf, index=False, header=False)
                buf.seek(0)

                quoted = [f'"{c}"' for c in pg_cols_order]
                copy_sql = f"COPY public.loan_data ({','.join(quoted)}) FROM STDIN WITH CSV"

                with pg_cur.copy(copy_sql) as copy:
                    copy.write(buf.getvalue())

                pg_conn.commit()

        print(f"PostgreSQL upload complete. Rows: {len(df_pg)}")

    except Exception as e:
        print("PostgreSQL upload FAILED:", e)

    return f"SUCCESS_SNOWFLAKE_{len(loans_sf)}_ROWS_PG_{len(df_pg)}_ROWS"