import pandas as pd
import boto3
from io import BytesIO
from connections.snowflake_conn import get_snowflake_conn

S3_BUCKET = "pushparag-loan-bucket"
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

def load_snowflake_table_chunked(name, last_month, chunk_size=100_000):
    """Load Snowflake table incrementally in chunks to save memory."""
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
    query = f'SELECT * FROM "{name}" {where_clause}'

    dfs = []
    with get_snowflake_conn() as ctx:
        cur = ctx.cursor()
        cur.execute(query, params)
        while True:
            chunk = cur.fetch_pandas_batches(chunk_size)
            batch = pd.concat(list(chunk), ignore_index=True) if chunk else pd.DataFrame()
            if batch.empty:
                break
            batch["date"] = fast_parse_dates(batch["date"])
            batch["month_year"] = batch["date"].dt.to_period("M")
            dfs.append(batch)
    if dfs:
        df = pd.concat(dfs, ignore_index=True)
    else:
        df = pd.DataFrame()
    print(f"{name} loaded: {len(df):,} rows")
    return df

# =========================
# MAIN PIPELINE
# =========================

def run_enrich_loans_pipeline():
    import time
    start_time = time.time()
    print(f"Starting pipeline at {time.strftime('%H:%M:%S')}")

    # Load base loans
    loans = read_s3_parquet(LOAN_BASE_KEY)
    if loans is None:
        raise RuntimeError("loan_synthetic_base.parquet not found in S3")
    loans["issue_date"] = fast_parse_dates(loans["issue_date"])
    loans["maturity_date"] = fast_parse_dates(loans["maturity_date"])
    print(f"Loaded {len(loans):,} loans")

    # Load previous enriched data
    prev = read_s3_parquet(OUTPUT_KEY)
    if prev is not None:
        prev["date"] = fast_parse_dates(prev["date"])
        last_processed_month = prev["date"].dt.to_period("M").max()
        print(f"Last processed month: {last_processed_month}")
    else:
        last_processed_month = None
        print("No previous enriched file → FULL RUN")

    # Load incremental Snowflake tables
    table_names = ["FX", "BONDS", "COMMODITY", "DERIVATIVES", "COLLATERAL"]
    tables = {name: load_snowflake_table_chunked(name, last_processed_month) for name in table_names}

    if all(df.empty for df in tables.values()):
        print("No new months to process.")
        return "NO_NEW_DATA"

    # Determine new months
    new_months = pd.concat([df["month_year"] for df in tables.values() if not df.empty]).dropna().unique()
    new_months = pd.PeriodIndex(new_months).sort_values()
    print(f"Processing months: {new_months.min()} -> {new_months.max()}")

    # Active loans cross join with months
    month_df = pd.DataFrame({"month_start": new_months.to_timestamp(), "month_year": new_months})
    min_date, max_date = month_df["month_start"].min(), month_df["month_start"].max()
    active_loans = loans[(loans["maturity_date"] >= min_date) & (loans["issue_date"] <= max_date)].copy()
    print(f"Active loans: {len(active_loans):,}")

    # Cross join efficiently
    active_loans["key"] = 1
    month_df["key"] = 1
    loans_expanded = active_loans.merge(month_df, on="key").drop("key", axis=1)
    loans_expanded = loans_expanded[
        (loans_expanded["month_start"] >= loans_expanded["issue_date"]) &
        (loans_expanded["month_start"] <= loans_expanded["maturity_date"])
    ]
    loans_expanded["date"] = loans_expanded["month_start"].dt.date
    print(f"Expanded rows: {len(loans_expanded):,}")

    # Aggregate Snowflake tables monthly
    fx_month = tables["FX"].groupby(["ticker", "month_year"], as_index=False).agg({
        "fx_rate": "mean", "fx_volatility": "mean", "carry_daily": "mean"
    })
    bonds_month = tables["BONDS"].groupby(["ticker", "month_year"], as_index=False).agg({
        "credit_spread": "mean", "yield_to_maturity": "mean",
        "credit_rating": lambda x: x.mode()[0] if len(x.mode()) else None
    })
    commod_month = tables["COMMODITY"].groupby(["sector", "month_year"], as_index=False).agg({
        "close": "mean", "vol_20d": "mean"
    })
    deriv_month = tables["DERIVATIVES"].groupby(["ticker", "month_year"], as_index=False).agg({
        "notional": "mean", "exposure_before_collateral": "mean",
        "collateral_value": "mean", "net_exposure": "mean",
        "collateral_ratio": "mean",
        "margin_call_flag": lambda x: 1 if (x == 1).any() else 0,
        "pnl": "mean"
    })
    collat_month = tables["COLLATERAL"].groupby(["ticker", "month_year"], as_index=False).agg({
        "counterparty": lambda x: '|'.join(sorted(set(map(str, x)))[:3]),
        "funding_cost": "mean", "liquidity_score": "mean",
        "margin_call_amount": "mean"
    })

    # Merge monthly tables into loans_expanded
    merged = loans_expanded
    if not fx_month.empty: merged = merged.merge(fx_month, on=["ticker", "month_year"], how="left", sort=False)
    if not bonds_month.empty: merged = merged.merge(bonds_month, on=["ticker", "month_year"], how="left", sort=False)
    if not commod_month.empty: merged = merged.merge(commod_month, on=["sector", "month_year"], how="left", sort=False)
    if not deriv_month.empty: merged = merged.merge(deriv_month, on=["ticker", "month_year"], how="left", sort=False)
    if not collat_month.empty: merged = merged.merge(collat_month, on=["ticker", "month_year"], how="left", sort=False)
    merged.drop(columns=["month_year", "month_start"], inplace=True, errors='ignore')

    # Append previous enriched file if exists
    if prev is not None:
        merged = pd.concat([prev, merged], ignore_index=True)
        merged.drop_duplicates(subset=["loan_id", "date"], inplace=True)

    # Write final dataset to S3
    write_s3_parquet(merged, OUTPUT_KEY)

    total_time = time.time() - start_time
    print(f"Pipeline completed in {total_time:.1f} seconds. Final shape: {merged.shape}")
    return "SUCCESS"
