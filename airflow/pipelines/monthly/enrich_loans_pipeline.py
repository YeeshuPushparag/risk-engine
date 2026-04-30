import os
import time
import hashlib
from io import BytesIO, StringIO
import numpy as np
import pandas as pd
from dateutil import parser
import boto3
import requests
from datetime import datetime, timedelta
from connections.snowflake_conn import get_snowflake_conn
from connections.postgre_conn import get_postgre_conn
from snowflake.connector.pandas_tools import write_pandas

S3_BUCKET = "pushparag-loan-bucket"
LOAN_BASE_KEY = "loan_synthetic_base.parquet"
OUTPUT_KEY = "loan_enriched_fx_bonds_commod_derivatives_collateral.parquet"

s3 = boto3.client("s3")


# ============================================================
# PRODUCTION-GRADE ALERTING (CRITICAL ONLY - NO SUCCESS)
# ============================================================
def send_critical_alert(message: str, context: dict = None):
    """
    Send CRITICAL alert to Slack only for unrecoverable pipeline failures.
    
    Triggers only for:
    - S3 base loan load failure
    - Snowflake table load failures (after retries)
    - Atomic S3 write failure
    - Complete pipeline failures
    
    Does NOT send for:
    - Retry attempts (only final failure)
    - Individual table failures (pipeline continues)
    - Missing macro data (continues anyway)
    """
    payload = {
        "level": "CRITICAL",
        "pipeline": "enrich_loans_pipeline",
        "message": message,
        "context": context or {},
        "timestamp": datetime.utcnow().isoformat(),
    }
    
    # Always print to stdout for log aggregation
    print(f"[CRITICAL] {message} | context={payload['context']}")
    
    # Send to Slack (safe, non-blocking)
    webhook = os.getenv("SLACK_WEBHOOK_URL")
    if webhook:
        try:
            truncated_msg = message[:500] + "..." if len(message) > 500 else message
            run_id = context.get('run_id', 'unknown') if context else 'unknown'
            text = f"*[CRITICAL]* enrich_loans_pipeline\n" \
                   f"{truncated_msg}\n" \
                   f"run_id: {run_id}"
            requests.post(webhook, json={"text": text}, timeout=3)
        except Exception as e:
            print(f"[ALERT ERROR] Slack notification failed: {e}")


# ============================================================
# RETRY DECORATOR (PRODUCTION-GRADE)
# ============================================================
def retry_with_backoff(func, retries=3, backoff_factor=2, exceptions=(Exception,), 
                        critical_name=None, run_id=None):
    """
    Retry a function with exponential backoff.
    Only sends CRITICAL alert if all retries are exhausted and critical_name provided.
    """
    last_exception = None
    
    for attempt in range(retries + 1):
        try:
            return func()
        except exceptions as e:
            last_exception = e
            if attempt < retries:
                wait_time = backoff_factor ** attempt
                print(f"  Retry {attempt + 1}/{retries} after {wait_time}s: {e}")
                time.sleep(wait_time)
            else:
                print(f"  All {retries} retries exhausted: {e}")
    
    # Only send critical alert if this is a critical operation
    if critical_name:
        send_critical_alert(
            f"{critical_name} failed after {retries} retries",
            context={"run_id": run_id, "error": str(last_exception)}
        )
    
    raise last_exception


# ============================================================
# SAFE S3 ATOMIC WRITE (temp -> copy -> delete)
# ============================================================
def atomic_write_parquet_to_s3(df, bucket, key, run_id=None):
    """
    Atomically write DataFrame as parquet to S3.
    Protocol: PUT to _temp/key -> COPY to final -> DELETE temp.
    Never leaves partial/corrupt file.
    Sends CRITICAL alert on failure.
    """
    temp_key = f"_temp/{key}"
    buf = BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    raw_bytes = buf.getvalue()
    
    try:
        # Step 1: Write to temp
        s3.put_object(Bucket=bucket, Key=temp_key, Body=raw_bytes)
        
        # Step 2: Copy to final
        s3.copy_object(
            Bucket=bucket,
            CopySource={"Bucket": bucket, "Key": temp_key},
            Key=key,
        )
        print(f"  [S3 ATOMIC WRITE] s3://{bucket}/{key}  rows={len(df):,}")
        
    except Exception as e:
        send_critical_alert(
            f"Atomic S3 write failed for key {key}",
            context={"run_id": run_id, "key": key, "temp_key": temp_key, "error": str(e)}
        )
        print(f"  [S3 WRITE ERROR] {e}")
        raise
        
    finally:
        # Step 3: Always clean up temp
        try:
            s3.delete_object(Bucket=bucket, Key=temp_key)
        except Exception:
            pass


# ============================================================
# S3 LOAD WITH RETRY (CRITICAL FOR BASE LOANS)
# ============================================================
def read_s3_parquet_with_retry(key, run_id=None, retries=3, critical=True):
    """Read parquet from S3 with retry logic."""
    def _load():
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return pd.read_parquet(BytesIO(obj["Body"].read()))
    
    try:
        return retry_with_backoff(_load, retries=retries,
                                   critical_name=f"S3 parquet load: {key}" if critical else None,
                                   run_id=run_id)
    except Exception as e:
        if critical:
            raise
        print(f"  WARNING: S3 read failed for {key}: {e}")
        return None


def write_s3_parquet_atomic(df, key, run_id=None):
    """Write parquet to S3 atomically with critical alert on failure."""
    atomic_write_parquet_to_s3(df, S3_BUCKET, key, run_id=run_id)


# ============================================================
# DATE HELPERS
# ============================================================
def fast_parse_dates(series):
    """Parse dates safely with multiple formats."""
    s = pd.to_datetime(series, errors="coerce", dayfirst=True)
    mask = s.isna()
    if mask.any():
        s[mask] = pd.to_datetime(series[mask], format="%d-%m-%Y", errors="coerce")
    return s


# ============================================================
# SNOWFLAKE LOAD WITH RETRY (SOFT FAIL - NO ALERTS PER TABLE)
# ============================================================
def load_snowflake_table_chunked_with_retry(name, last_month, run_id=None, retries=2):
    """
    Load table incrementally from Snowflake with retry.
    NO ALERT per table - pipeline continues if individual table fails.
    """
    
    def _load():
        print(f"  Loading {name} incrementally from Snowflake...")
        
        where_clause = ""
        params = ()
        
        if last_month is not None:
            param_date = f"{last_month.year}-{last_month.month:02d}-01"
            where_clause = """
                WHERE DATE_TRUNC('MONTH', TO_DATE("date", 'YYYY-MM-DD'))
                      > DATE_TRUNC('MONTH', TO_DATE(%s, 'YYYY-MM-DD'))
            """
            params = (param_date,)
        
        query = f'SELECT * FROM "{name}" {where_clause}'
        
        dfs = []
        with get_snowflake_conn() as ctx:
            cur = ctx.cursor()
            cur.execute(query, params)
            for batch in cur.fetch_pandas_batches():
                batch["date"] = fast_parse_dates(batch["date"])
                batch = batch[batch["date"].notna()]
                batch["month_year"] = batch["date"].dt.to_period("M")
                dfs.append(batch)
        
        df = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
        print(f"    {name} loaded: {len(df):,} rows")
        return df
    
    try:
        # No critical alert for individual table failures - pipeline continues
        return retry_with_backoff(_load, retries=retries)
    except Exception as e:
        print(f"  WARNING: Failed to load {name}: {e}")
        return pd.DataFrame()  # Return empty DataFrame on failure - don't fail entire pipeline


# ============================================================
# AGGREGATION HELPERS
# ============================================================
def aggregate_monthly(df, group_cols, agg_dict):
    """Aggregate DataFrame to monthly level."""
    if df.empty:
        return df
    return df.groupby(group_cols, as_index=False).agg(agg_dict)


# ============================================================
# MAIN PIPELINE (WITH CRITICAL ALERTS ONLY)
# ============================================================
def run_enrich_loans_pipeline():
    """
    Complete production-grade loan enrichment pipeline with:
    1. Retry on all external calls (S3, Snowflake)
    2. Atomic S3 writes (temp -> copy -> delete) - no partial files
    3. Slack alerts ONLY for critical failures (base loans, S3 write, pipeline crash)
    4. Partial success handling (one table failure doesn't kill pipeline)
    5. Run-level tracking with run_id
    6. Memory-efficient processing (keeps prev + new only)
    
    NO SUCCESS ALERT - this is not the final loan pipeline.
    """
    pipeline_start = time.time()
    run_id = datetime.utcnow().isoformat()
    
    print(f"\n{'='*66}")
    print(f"  LOAN ENRICHMENT PIPELINE START - run_id={run_id}")
    print(f"{'='*66}\n")
    
    # Track failures for partial success reporting
    failed_tables = []
    succeeded_tables = []
    
    try:
        # ============================================================
        # STEP 1: Load base loans from S3 (CRITICAL - sends alert on failure)
        # ============================================================
        print("[STEP 1] Loading base loans from S3...")
        
        loans = read_s3_parquet_with_retry(LOAN_BASE_KEY, run_id=run_id, retries=3, critical=True)
        if loans is None:
            raise RuntimeError("loan_synthetic_base.parquet not found in S3")
        
        loans["issue_date"] = fast_parse_dates(loans["issue_date"])
        loans["maturity_date"] = fast_parse_dates(loans["maturity_date"])
        print(f"  Loaded {len(loans):,} loans")
        
        # ============================================================
        # STEP 2: Load previous enriched data (SOFT FAIL - no alert)
        # ============================================================
        print("[STEP 2] Loading previous enriched data...")
        
        prev = read_s3_parquet_with_retry(OUTPUT_KEY, run_id=run_id, retries=2, critical=False)
        
        if prev is not None:
            prev["date"] = fast_parse_dates(prev["date"])
            prev = prev[prev["date"].notna()]
            prev = prev[prev["date"] <= pd.Timestamp.today()]
            last_processed_month = prev["date"].dt.to_period("M").max() if not prev.empty else None
            print(f"  Last processed month: {last_processed_month}")
            print(f"  Previous rows: {len(prev):,}")
        else:
            last_processed_month = None
            print("  No previous enriched file - FULL RUN")
        
        # ============================================================
        # STEP 3: Load Snowflake tables incrementally (SOFT FAIL - no alerts)
        # ============================================================
        print("[STEP 3] Loading Snowflake tables incrementally...")
        
        table_names = ["FX", "BONDS", "COMMODITY", "DERIVATIVES", "COLLATERAL"]
        tables = {}
        
        for name in table_names:
            df = load_snowflake_table_chunked_with_retry(name, last_processed_month, run_id=run_id, retries=2)
            if not df.empty:
                succeeded_tables.append(name)
                tables[name] = df
            else:
                failed_tables.append(name)
                tables[name] = pd.DataFrame()
                print(f"  WARNING: {name} returned no data or failed - continuing")
        
        # Check if we have any data to process
        if all(df.empty for df in tables.values()):
            print("  No new months to process from any table.")
            return "NO_NEW_DATA"
        
        print(f"  Succeeded tables: {succeeded_tables}")
        print(f"  Failed/empty tables: {failed_tables}")
        
        # ============================================================
        # STEP 4: Calculate new months to process
        # ============================================================
        print("[STEP 4] Calculating months to process...")
        
        new_months = pd.concat([df["month_year"] for df in tables.values() if not df.empty])
        new_months = pd.PeriodIndex(new_months).dropna().sort_values().unique()
        
        today_period = pd.Period(pd.Timestamp.today(), freq="M")
        new_months = new_months[new_months <= today_period]
        
        if len(new_months) == 0:
            print("  No valid months to process after filtering.")
            return "NO_NEW_DATA"
        
        print(f"  New months count: {len(new_months)}")
        print(f"  Range: {new_months.min()} -> {new_months.max()}")
        
        # ============================================================
        # STEP 5: Active loans cross join with months
        # ============================================================
        print("[STEP 5] Expanding loans to monthly view...")
        
        month_df = pd.DataFrame({"month_start": new_months.to_timestamp(), "month_year": new_months})
        min_date = month_df["month_start"].min()
        max_date = month_df["month_start"].max()
        
        active_loans = loans[(loans["maturity_date"] >= min_date) & (loans["issue_date"] <= max_date)].copy()
        print(f"  Active loans: {len(active_loans):,}")
        
        # Cross join
        active_loans["key"] = 1
        month_df["key"] = 1
        loans_expanded = active_loans.merge(month_df, on="key").drop("key", axis=1)
        loans_expanded = loans_expanded[
            (loans_expanded["month_start"] >= loans_expanded["issue_date"]) &
            (loans_expanded["month_start"] <= loans_expanded["maturity_date"])
        ]
        loans_expanded["date"] = loans_expanded["month_start"]
        print(f"  Expanded rows: {len(loans_expanded):,}")
        
        # ============================================================
        # STEP 6: Aggregate Snowflake tables monthly
        # ============================================================
        print("[STEP 6] Aggregating tables to monthly level...")
        
        fx_month = aggregate_monthly(tables["FX"], ["ticker", "month_year"], {
            "fx_rate": "mean", "fx_volatility": "mean", "carry_daily": "mean"
        }) if not tables["FX"].empty else pd.DataFrame()
        
        bonds_month = aggregate_monthly(tables["BONDS"], ["ticker", "month_year"], {
            "credit_spread": "mean",
            "yield_to_maturity": "mean",
            "credit_rating": lambda x: x.mode()[0] if len(x.mode()) else None
        }) if not tables["BONDS"].empty else pd.DataFrame()
        
        commod_month = aggregate_monthly(tables["COMMODITY"], ["sector", "month_year"], {
            "close": "mean", "vol_20d": "mean"
        }) if not tables["COMMODITY"].empty else pd.DataFrame()
        
        deriv_month = aggregate_monthly(tables["DERIVATIVES"], ["ticker", "month_year"], {
            "notional": "mean",
            "exposure_before_collateral": "mean",
            "collateral_value": "mean",
            "net_exposure": "mean",
            "collateral_ratio": "mean",
            "margin_call_flag": lambda x: 1 if (x == 1).any() else 0,
            "pnl": "mean"
        }) if not tables["DERIVATIVES"].empty else pd.DataFrame()
        
        collat_month = aggregate_monthly(tables["COLLATERAL"], ["ticker", "month_year"], {
            "counterparty": lambda x: '|'.join(sorted(set(map(str, x)))[:3]),
            "funding_cost": "mean",
            "liquidity_score": "mean",
            "margin_call_amount": "mean"
        }) if not tables["COLLATERAL"].empty else pd.DataFrame()
        
        # ============================================================
        # STEP 7: Merge all tables
        # ============================================================
        print("[STEP 7] Merging all tables...")
        
        merged = loans_expanded
        
        # FX merge
        if not fx_month.empty:
            merged = merged.merge(fx_month, on=["ticker", "month_year"], how="left", sort=False)
        
        # BONDS merge
        if not bonds_month.empty:
            merged = merged.merge(bonds_month, on=["ticker", "month_year"], how="left", sort=False)
        
        # COMMODITY merge (by sector)
        if not commod_month.empty:
            merged = merged.merge(commod_month, on=["sector", "month_year"], how="left", sort=False)
        
        # DERIVATIVES merge
        if not deriv_month.empty:
            merged = merged.merge(deriv_month, on=["ticker", "month_year"], how="left", sort=False)
        
        # COLLATERAL merge
        if not collat_month.empty:
            merged = merged.merge(collat_month, on=["ticker", "month_year"], how="left", sort=False)
        
        # Drop temporary columns
        merged.drop(columns=["month_year", "month_start"], inplace=True, errors="ignore")
        
        # ============================================================
        # STEP 8: Combine with previous data and deduplicate
        # ============================================================
        print("[STEP 8] Combining with previous data...")
        
        if prev is not None and not prev.empty:
            merged = pd.concat([prev, merged], ignore_index=True)
            merged.drop_duplicates(subset=["loan_id", "date"], keep="last", inplace=True)
        
        if merged.empty:
            print("  No output rows generated")
            return "NO_OUTPUT_ROWS"
        
        print(f"  Final rows after merge: {len(merged):,}")
        
        # Convert date to datetime
        merged["date"] = pd.to_datetime(merged["date"])
        
        # ============================================================
        # STEP 9: Add pipeline metadata
        # ============================================================
        print("[STEP 9] Adding pipeline metadata...")
        
        merged["pipeline_name"] = "enrich_loans_pipeline"
        merged["pipeline_run_id"] = run_id
        merged["data_source"] = "s3 + snowflake"
        merged["input_source"] = "loan_base + fx + bonds + commodity + derivatives + collateral"
        merged["transformation"] = "loan_enrichment_monthly_v1"
        merged["record_created_at"] = datetime.utcnow()
        
        # ============================================================
        # STEP 10: Write to S3 (ATOMIC - CRITICAL - sends alert on failure)
        # ============================================================
        print("[STEP 10] Writing to S3 (atomic write)...")
        
        write_s3_parquet_atomic(merged, OUTPUT_KEY, run_id=run_id)
        
        # ============================================================
        # STEP 11: Success summary (NO SLACK ALERT - not final pipeline)
        # ============================================================
        total_time = time.time() - pipeline_start
        
        print(f"\n{'='*66}")
        print(f"  LOAN ENRICHMENT PIPELINE SUCCESS - run_id={run_id}")
        print(f"  Final rows: {len(merged):,}")
        print(f"  Succeeded tables: {succeeded_tables}")
        print(f"  Failed/empty tables: {failed_tables if failed_tables else 'None'}")
        print(f"  Duration: {total_time:.1f}s")
        print(f"{'='*66}\n")
        
        return "SUCCESS"
    
    except Exception as e:
        # Send CRITICAL alert for any unhandled exception
        send_critical_alert(
            f"Pipeline failed with unhandled exception",
            context={"run_id": run_id, "error": str(e)}
        )
        
        total_time = time.time() - pipeline_start
        print(f"\n{'='*66}")
        print(f"  LOAN ENRICHMENT PIPELINE FAILED - run_id={run_id}")
        print(f"  Error: {str(e)}")
        print(f"  Succeeded tables: {succeeded_tables}")
        print(f"  Failed tables: {failed_tables}")
        print(f"  Duration: {total_time:.1f}s")
        print(f"{'='*66}\n")
        
        # Re-raise for Airflow to catch
        raise