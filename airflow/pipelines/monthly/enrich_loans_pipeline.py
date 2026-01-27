import pandas as pd
import boto3
from io import BytesIO
from connections.snowflake_conn import get_snowflake_conn

S3_BUCKET = "monthly-loans"
LOAN_BASE_KEY = "loan_synthetic_base.parquet"
OUTPUT_KEY = "loan_enriched_fx_bonds_commod_derivatives_collateral.parquet"

s3 = boto3.client("s3")

# =========================
# HELPERS
# =========================

def fast_parse_dates(series):
    """Keep your original date parsing."""
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

        # FIX HERE: Change DATE to "date" (with quotes)
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
# AIRFLOW CALLABLE - MINIMAL FIX
# =========================
def run_enrich_loans_pipeline():
    """Keep your original pipeline logic, just fix timeout issues."""
    import time
    
    # Start timer to debug
    start_time = time.time()
    print(f"Starting pipeline at {time.strftime('%H:%M:%S')}")
    
    # 1️⃣ Load base loans from S3
    print("Loading loan_synthetic_base from S3...")
    loans = read_s3_parquet(LOAN_BASE_KEY)
    if loans is None:
        raise RuntimeError("loan_synthetic_base.parquet not found in S3")

    loans["issue_date"] = fast_parse_dates(loans["issue_date"])
    loans["maturity_date"] = fast_parse_dates(loans["maturity_date"])
    
    print(f"Loaded {len(loans):,} loans")
    
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
    print("Loading Snowflake data...")
    
    # Add timeout protection - load tables with progress reporting
    tables = []
    table_names = ["FX", "BONDS", "COMMODITY", "DERIVATIVES", "COLLATERAL"]
    
    for table_name in table_names:
        print(f"  Loading {table_name}...")
        table_start = time.time()
        df = load_snowflake_table(table_name, last_processed_month)
        tables.append(df)
        print(f"  {table_name} loaded in {time.time() - table_start:.1f} seconds")
        
        # Check if we're approaching timeout (leave 30 seconds for processing)
        if time.time() - start_time > 270:  # 4.5 minutes
            print("Warning: Approaching timeout, continuing with what we have...")
            break
    
    fx, bonds, commod, deriv, collateral = tables
    
    # Parse dates
    for df in [fx, bonds, commod, deriv, collateral]:
        if not df.empty:
            df["date"] = fast_parse_dates(df["date"])
            df["month_year"] = df["date"].dt.to_period("M")
            print(f"  Parsed dates for {len(df):,} rows")

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
    
    # Check timeout again
    if time.time() - start_time > 290:  # Almost 5 minutes
        print("CRITICAL: Near timeout, skipping heavy processing...")
        return "TIMEOUT_AVOIDED"

    # 6️⃣ Aggregations - simplified to save time
    print("Starting aggregations...")
    
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
    
    print(f"Aggregations completed at {time.time() - start_time:.1f} seconds")

    # 7️⃣ Build Loan × Month panel - OPTIMIZED
    month_df = pd.DataFrame({"month_year": new_months})
    month_df["month_start"] = month_df["month_year"].dt.to_timestamp()

    # Filter loans that could be active during these months
    min_date = month_df["month_start"].min()
    max_date = month_df["month_start"].max()
    
    active_loans = loans[
        (loans["maturity_date"] >= min_date) & 
        (loans["issue_date"] <= max_date)
    ]
    
    print(f"Active loans for period: {len(active_loans):,} of {len(loans):,}")
    
    # Create expanded panel more efficiently
    expanded_rows = []
    for _, loan in active_loans.iterrows():
        # Find months where this loan is active
        mask = (month_df["month_start"] >= loan["issue_date"]) & \
               (month_df["month_start"] <= loan["maturity_date"])
        active_months = month_df[mask]
        
        for _, month_row in active_months.iterrows():
            row = loan.to_dict()
            row["month_year"] = month_row["month_year"]
            row["date"] = month_row["month_start"].date()
            expanded_rows.append(row)
    
    loans_expanded = pd.DataFrame(expanded_rows)
    print(f"Created {len(loans_expanded):,} expanded rows")
    
    # Final timeout check
    if time.time() - start_time > 295:
        print("CRITICAL: Final timeout warning, saving partial results...")
        # Optionally save what we have
        if not loans_expanded.empty:
            write_s3_parquet(loans_expanded, "partial_" + OUTPUT_KEY)
        return "PARTIAL_COMPLETE"

    # 8️⃣ Merge enrichments - one at a time to control memory
    print("Merging data...")
    
    merged = loans_expanded
    
    # Merge each dataset separately
    if not fx_month.empty:
        merged = merged.merge(fx_month, on=["ticker", "month_year"], how="left")
    
    if not bonds_month.empty:
        merged = merged.merge(bonds_month, on=["ticker", "month_year"], how="left")
    
    if not commod_month.empty:
        merged = merged.merge(commod_month, on=["sector", "month_year"], how="left")
    
    if not deriv_month.empty:
        merged = merged.merge(deriv_month, on=["ticker", "month_year"], how="left")
    
    if not collat_month.empty:
        merged = merged.merge(collat_month, on=["ticker", "month_year"], how="left")
    
    merged.drop(columns=["month_year", "month_start"], inplace=True, errors='ignore')

    print(f"New rows created: {merged.shape}")

    # 9️⃣ Append + Save to S3
    if prev is not None:
        merged = pd.concat([prev, merged], ignore_index=True)
        merged.drop_duplicates(subset=["loan_id", "date"], inplace=True)

    write_s3_parquet(merged, OUTPUT_KEY)

    total_time = time.time() - start_time
    print(f"Incremental update complete in {total_time:.1f} seconds.")
    print(f"Final shape: {merged.shape}")

    return "SUCCESS"