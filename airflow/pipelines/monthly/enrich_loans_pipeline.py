import os
import pandas as pd
import numpy as np
import boto3
from io import BytesIO
from connections.snowflake_conn import get_snowflake_conn


# =========================
# CONFIG
# =========================
S3_BUCKET = "monthly-loans"

LOAN_BASE_KEY = "loan_synthetic_base.parquet"
OUTPUT_KEY = "loan_enriched_fx_bonds_commod_derivatives_collateral.parquet"

s3 = boto3.client("s3")


# =========================
# HELPERS
# =========================
def fast_parse_dates(series):
    s = pd.to_datetime(series, errors="coerce", dayfirst=True)
    mask = s.isna()
    if mask.any():
        s[mask] = pd.to_datetime(series[mask], format="%d-%m-%Y", errors="coerce")
    return s


def load_snowflake_table(name, last_month):
    print(f"Loading {name} incrementally from Snowflake...")

    where_clause = ""
    params = ()

    if last_month is not None:
        param_date = f"{last_month.year}-{last_month.month:02d}-01"

        where_clause = '''
            WHERE DATE_TRUNC('MONTH', TO_DATE("date", 'YYYY-MM-DD'))
                  > DATE_TRUNC('MONTH', TO_DATE(%s, 'YYYY-MM-DD'))
        '''
        params = (param_date,)

    query = f'''
        SELECT *
        FROM "{name}"
        {where_clause}
    '''

    with get_snowflake_conn() as ctx:
        cur = ctx.cursor()
        cur.execute(query, params)
        df = cur.fetch_pandas_all()

    print(f"{name} loaded: {len(df):,} rows")
    return df


def read_s3_parquet(key):
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return pd.read_parquet(BytesIO(obj["Body"].read()))
    except s3.exceptions.NoSuchKey:
        return None


def write_s3_parquet(df, key):
    buf = BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=buf)
    print(f"Uploaded {key} to S3")


# =========================
# AIRFLOW CALLABLE
# =========================
def run_enrich_loans_pipeline():
    # 1️⃣ Load base loans from S3
    print("Loading loan_synthetic_base from S3...")
    loans = read_s3_parquet(LOAN_BASE_KEY)
    if loans is None:
        raise RuntimeError("loan_synthetic_base.parquet not found in S3")

    loans["issue_date"] = fast_parse_dates(loans["issue_date"])
    loans["maturity_date"] = fast_parse_dates(loans["maturity_date"])

    # 2️⃣ Load previous enriched dataset
    prev = read_s3_parquet(OUTPUT_KEY)

    if prev is not None:
        prev["date"] = fast_parse_dates(prev["date"])
        last_processed_month = prev["date"].dt.to_period("M").max()
        print(f"Last processed month: {last_processed_month}")
    else:
        last_processed_month = None
        print("No previous enriched file → FULL RUN")

    # 3️⃣ Load incremental Snowflake data
    fx = load_snowflake_table("FX", last_processed_month)
    bonds = load_snowflake_table("BONDS", last_processed_month)
    commod = load_snowflake_table("COMMODITY", last_processed_month)
    deriv = load_snowflake_table("DERIVATIVES", last_processed_month)
    collateral = load_snowflake_table("COLLATERAL", last_processed_month)

    for df in [fx, bonds, commod, deriv, collateral]:
        df["date"] = fast_parse_dates(df["date"])
        df["month_year"] = df["date"].dt.to_period("M")

    # 4️⃣ If incremental, cut old
    if last_processed_month is not None:
        fx = fx[fx["month_year"] > last_processed_month]
        bonds = bonds[bonds["month_year"] > last_processed_month]
        commod = commod[commod["month_year"] > last_processed_month]
        deriv = deriv[deriv["month_year"] > last_processed_month]
        collateral = collateral[collateral["month_year"] > last_processed_month]

    if fx.empty and bonds.empty and commod.empty and deriv.empty and collateral.empty:
        print("No NEW months. Done.")
        return "NO_NEW_DATA"

    # 5️⃣ Months set
    new_months = pd.concat([
        fx["month_year"],
        bonds["month_year"],
        commod["month_year"],
        deriv["month_year"],
        collateral["month_year"]
    ]).dropna().unique()

    new_months = pd.PeriodIndex(new_months).sort_values()
    print(f"Processing months: {new_months.min()} -> {new_months.max()}")

    # 6️⃣ Aggregations
    fx_month = fx.groupby(["ticker", "month_year"], as_index=False).agg({
        "fx_rate": "mean",
        "fx_volatility": "mean",
        "carry_daily": "mean"
    })

    bonds_month = bonds.groupby(["ticker", "month_year"], as_index=False).agg({
        "credit_spread": "mean",
        "yield_to_maturity": "mean",
        "credit_rating": lambda x: x.mode()[0] if len(x.mode()) else None
    })

    commod_month = commod.groupby(["sector", "month_year"], as_index=False).agg({
        "close": "mean",
        "vol_20d": "mean"
    })

    deriv_month = deriv.groupby(["ticker", "month_year"], as_index=False).agg({
        "notional": "mean",
        "exposure_before_collateral": "mean",
        "collateral_value": "mean",
        "net_exposure": "mean",
        "collateral_ratio": "mean",
        "margin_call_flag": lambda x: 1 if (x == 1).any() else 0,
        "pnl": "mean"
    })

    collat_month = collateral.groupby(["ticker", "month_year"], as_index=False).agg({
        "counterparty": lambda x: '|'.join(sorted(set(map(str, x)))[:3]),
        "funding_cost": "mean",
        "liquidity_score": "mean",
        "margin_call_amount": "mean"
    })

    # 7️⃣ Build Loan × Month panel
    month_df = pd.DataFrame({"month_year": new_months})
    month_df["month_start"] = month_df["month_year"].dt.to_timestamp()

    loans_expanded = (
        loans.assign(key=1)
        .merge(month_df.assign(key=1), on="key")
        .drop("key", axis=1)
    )

    loans_expanded = loans_expanded[
        (loans_expanded["month_start"] >= loans_expanded["issue_date"]) &
        (loans_expanded["month_start"] <= loans_expanded["maturity_date"])
    ]

    loans_expanded["date"] = loans_expanded["month_start"].dt.date

    # 8️⃣ Merge enrichments
    merged = (
        loans_expanded
        .merge(fx_month, on=["ticker", "month_year"], how="inner")
        .merge(bonds_month, on=["ticker", "month_year"], how="left")
        .merge(commod_month, on=["sector", "month_year"], how="left")
        .merge(deriv_month, on=["ticker", "month_year"], how="left")
        .merge(collat_month, on=["ticker", "month_year"], how="left")
    )

    merged.drop(columns=["month_year", "month_start"], inplace=True)

    print(f"New rows created: {merged.shape}")

    # 9️⃣ Append + Save to S3
    if prev is not None:
        merged = pd.concat([prev, merged], ignore_index=True)
        merged.drop_duplicates(subset=["loan_id", "date"], inplace=True)

    write_s3_parquet(merged, OUTPUT_KEY)

    print("Incremental update complete.")
    print("Final shape:", merged.shape)

    return "SUCCESS"
