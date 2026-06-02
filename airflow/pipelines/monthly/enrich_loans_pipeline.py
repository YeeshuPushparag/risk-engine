import os
import time
import hashlib
from io import BytesIO, StringIO
import numpy as np
import pandas as pd
from dateutil import parser
import boto3
import requests
import json
from datetime import datetime, timedelta
from connections.snowflake_conn import get_snowflake_conn
from connections.postgre_conn import get_postgre_conn
from snowflake.connector.pandas_tools import write_pandas

S3_BUCKET = "yeeshu-loan-bucket"
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
# RUN SUMMARY WRITER
# ============================================================
def write_run_summary(summary: dict) -> None:
    """
    Write pipeline run summary to S3 for observability and audit.

    Args:
        summary: Dictionary containing run metadata including:
            - pipeline_run_id
            - pipeline_name
            - run_ts
            - status (SUCCESS/FAILED)
            - mode
            - start_date
            - rows_processed
            - processing_time_s
            - tables_loaded (list)
            - error (if failed)
    """
    key = (
        f"metadata/enrich_loans_pipeline/"
        f"pipeline_run_id={summary['pipeline_run_id']}/"
        f"run_summary.json"
    )

    try:
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=key,
            Body=json.dumps(
                summary,
                default=str,
                indent=2,
            ),
        )
        print(f"  [RUN SUMMARY] Wrote {key}")
    except Exception as e:
        # Don't fail the pipeline if summary write fails, but log the error
        print(f"  [RUN SUMMARY][WARN] Failed to write summary to S3: {e}")

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


def fast_parse_dates(series):
    """
    Safe production-grade datetime parser.

    Handles:
    - ISO dates
    - parquet timestamps
    - Snowflake timestamps
    - epoch ns integers
    """

    # First attempt
    s = pd.to_datetime(
        series,
        errors="coerce",
        utc=True,
    )

    # Handle epoch nanoseconds explicitly
    mask = s.isna()

    if mask.any():

        numeric_vals = pd.to_numeric(
            series[mask],
            errors="coerce",
        )

        s.loc[mask] = pd.to_datetime(
            numeric_vals,
            unit="ns",
            errors="coerce",
            utc=True,
        )

    return s.dt.tz_localize(None)

# ============================================================
# SNOWFLAKE LOAD WITH RETRY (SOFT FAIL - NO ALERTS PER TABLE)
# ============================================================
def load_snowflake_table_chunked_with_retry(name, start_date_override, run_id=None, retries=2):
    """
    Load table incrementally from Snowflake with retry.
    NO ALERT per table - pipeline continues if individual table fails.
    
    For replay/backfill: loads ALL data >= start_date_override
    For incremental: loads ONLY new data based on watermark logic (handled in pipeline)
    """
    
    def _load():
        print(f"  Loading {name} from Snowflake...")
        
        where_clause = ""
        params = ()
        
        if start_date_override is not None:
            param_date = f"{start_date_override.year}-{start_date_override.month:02d}-01"
            where_clause = """
                WHERE DATE_TRUNC('MONTH', CAST("date" AS DATE))
                    >= DATE_TRUNC('MONTH', CAST(%s AS DATE))
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

        send_critical_alert(
            f"{name} Snowflake load failed",
            context={
                "run_id": run_id,
                "table": name,
                "error": str(e),
            }
        )

        raise RuntimeError(
            f"{name} load failed: {e}"
        )


# ============================================================
# AGGREGATION HELPERS
# ============================================================
def aggregate_monthly(df, group_cols, agg_dict):
    """Aggregate DataFrame to monthly level."""
    if df.empty:
        return df
    return df.groupby(group_cols, as_index=False).agg(agg_dict)


# ============================================================
# MONTH WINDOW OVERWRITE LOGIC
# ============================================================
def replace_month_window(prev_df, new_df, recompute_start_period):
    """
    Deterministically replace months >= recompute_start_period.
    
    Args:
        prev_df: Previous enriched parquet data
        new_df: Newly recomputed data for months >= recompute_start_period
        recompute_start_period: Period to start recompute from
    
    Returns:
        Combined DataFrame with no duplicate month states
    """
    if prev_df is None or prev_df.empty:
        return new_df
    
    if new_df.empty:
        return prev_df
    
    # Ensure date column is datetime
    prev_df["date"] = pd.to_datetime(prev_df["date"])
    new_df["date"] = pd.to_datetime(new_df["date"])
    
    # Create month_year column for comparison
    prev_df["month_year"] = prev_df["date"].dt.to_period("M")
    new_df["month_year"] = new_df["date"].dt.to_period("M")
    
    # Keep rows from previous that are BEFORE recompute window
    historical_mask = prev_df["month_year"] < recompute_start_period
    historical_rows = prev_df[historical_mask].copy()
    
    # Remove temporary column from historical rows
    historical_rows.drop(columns=["month_year"], inplace=True)
    
    # Remove temporary column from new rows
    new_df_clean = new_df.drop(columns=["month_year"], inplace=False)
    
    # Combine historical + recomputed rows
    final_df = pd.concat([historical_rows, new_df_clean], ignore_index=True)
    
    return final_df


# ============================================================
# MAIN PIPELINE (WITH CRITICAL ALERTS ONLY)
# ============================================================
def run_enrich_loans_pipeline(
    start_date_override=None,
    replay=False,
    airflow_metadata=None,
):
    """
    Complete production-grade loan enrichment pipeline with deterministic replay/backfill.
    
    Args:
        start_date_override: datetime or date string (YYYY-MM-DD) for backfill/replay start
        replay: If True, force full recompute from start_date_override
    
    Returns:
        str: Pipeline status (SUCCESS, NO_NEW_DATA, NO_OUTPUT_ROWS)
    
    Mode Determination:
        - replay=True: Full deterministic recompute from start_date_override
        - start_date_override provided: Backfill from start_date_override
        - both False: Incremental append-only mode
    
    Replay/Backfill Semantics:
        - Recompute ALL months >= start_date_override
        - Remove overlapping months from existing enriched parquet
        - Append recomputed months deterministically
        - No duplicate month states
    
    Incremental Semantics:
        - Load previous enriched parquet
        - Get watermark from LAST processed month in parquet
        - Process only months AFTER watermark
        - Append new rows only
    """
    pipeline_start = time.time()
    run_ts = datetime.utcnow()
    run_id = run_ts.isoformat()
    
    # ============================================================
    # MODE DETERMINATION
    # ============================================================
    if replay:
        if not start_date_override:
            raise ValueError("replay=True requires start_date_override")
        mode = "replay"
        print(f"\n{'='*66}")
        print(f"  LOAN ENRICHMENT PIPELINE - REPLAY MODE")
        print(f"  run_id={run_id}")
        print(f"  replay_start={start_date_override}")
        print(f"{'='*66}\n")
    elif start_date_override:
        mode = "backfill"
        print(f"\n{'='*66}")
        print(f"  LOAN ENRICHMENT PIPELINE - BACKFILL MODE")
        print(f"  run_id={run_id}")
        print(f"  backfill_start={start_date_override}")
        print(f"{'='*66}\n")
    else:
        mode = "incremental"
        print(f"\n{'='*66}")
        print(f"  LOAN ENRICHMENT PIPELINE - INCREMENTAL MODE")
        print(f"  run_id={run_id}")
        print(f"{'='*66}\n")
    
    # Convert start_date_override to period if provided
    recompute_start_period = None
    if start_date_override:
        if isinstance(start_date_override, str):
            start_date_override = pd.to_datetime(start_date_override)
        recompute_start_period = pd.Period(start_date_override, freq="M")
    
    # Track success reporting
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
        
        watermark = None
        if prev is not None and not prev.empty:
            prev["date"] = fast_parse_dates(prev["date"])
            prev = prev[prev["date"].notna()]
            prev = prev[prev["date"] <= pd.Timestamp.today()]
            
            # Get watermark from enriched parquet (NOT from source tables)
            watermark = prev["date"].dt.to_period("M").max() if not prev.empty else None
            print(f"  Watermark from enriched parquet: {watermark}")
            print(f"  Previous rows: {len(prev):,}")
        else:
            print("  No previous enriched file - FULL RUN")
        
        # ============================================================
        # STEP 3: Determine date range for Snowflake load
        # ============================================================
        print("[STEP 3] Determining date range for Snowflake load...")
        
        # For replay/backfill: load ALL data >= start_date_override
        # For incremental: load ONLY data > watermark (if watermark exists)
        snowflake_start_date = None
        
        if mode in ["replay", "backfill"]:
            # Load everything from start_date_override onward
            snowflake_start_date = start_date_override
            print(f"  Loading Snowflake data from {snowflake_start_date.date()} onward")
        elif mode == "incremental":
            if mode == "incremental":
                if watermark:
                    snowflake_start_date = (watermark + 1).start_time
                    print(f"  Loading Snowflake data from {snowflake_start_date.date()} onward")
                else:
                    # No previous data - load last 3 months only
                    today_period = pd.Period(pd.Timestamp.today(), freq="M")
                    snowflake_start_date = (today_period - 3 + 1).start_time
                    print(f"  No previous parquet - loading last 3 months only")
                    print(f"  Loading Snowflake data from {snowflake_start_date.date()} onward")
        
        # ============================================================
        # STEP 4: Load Snowflake tables (SOFT FAIL - no alerts)
        # ============================================================
        print("[STEP 4] Loading Snowflake tables...")
        
        table_names = ["FX", "BONDS", "COMMODITY", "DERIVATIVES", "COLLATERAL"]
        tables = {}
        
        for name in table_names:
            df = load_snowflake_table_chunked_with_retry(
                name, snowflake_start_date, run_id=run_id, retries=2
            )
            if not df.empty:
                succeeded_tables.append(name)
                tables[name] = df
 
        
        # Check if we have any data to process
        if all(df.empty for df in tables.values()):
            print("  No new months to process from any table.")
            return "NO_NEW_DATA"
        
        print(f"  Succeeded tables: {succeeded_tables}")
        
        # ============================================================
        # STEP 5: Calculate months to process
        # ============================================================
        print("[STEP 5] Calculating months to process...")
        
        # Determine months available from source tables
        source_months = pd.concat([df["month_year"] for df in tables.values() if not df.empty])
        source_months = pd.PeriodIndex(source_months).dropna().sort_values().unique()
        
        today_period = pd.Period(pd.Timestamp.today(), freq="M")
        source_months = source_months[source_months <= today_period]
        
        # Filter months based on mode
        if mode == "incremental" and watermark:
            # Only process months AFTER watermark
            months_to_process = source_months[source_months > watermark]
        else:
            # Process all loaded months
            months_to_process = source_months
        
        if len(months_to_process) == 0:
            print("  No valid months to process after filtering.")
            return "NO_NEW_DATA"
        
        print(f"  Months to process: {len(months_to_process)}")
        print(f"  Range: {months_to_process.min()} -> {months_to_process.max()}")
        
        # ============================================================
        # STEP 6: Active loans cross join with months
        # ============================================================
        print("[STEP 6] Expanding loans to monthly view...")
        
        month_df = pd.DataFrame({"month_start": months_to_process.to_timestamp(), "month_year": months_to_process})
        min_date = month_df["month_start"].min()
        max_date = month_df["month_start"].max()
        
        active_loans = loans[(loans["maturity_date"] >= min_date) & (loans["issue_date"] <= max_date)].copy()
        print(f"  Active loans: {len(active_loans):,}")
        
        # Drop date column if exists to avoid date_x/date_y conflict
        if "date" in active_loans.columns:
            active_loans = active_loans.drop(columns=["date"])

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
        # STEP 7: Aggregate Snowflake tables monthly
        # ============================================================
        print("[STEP 7] Aggregating tables to monthly level...")
        
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
        # STEP 8: Merge all tables
        # ============================================================
        print("[STEP 8] Merging all tables...")
        
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
        # STEP 9: Combine with previous data deterministically
        # ============================================================
        print("[STEP 9] Combining with previous data deterministically...")
        
        if mode in ["replay", "backfill"] and recompute_start_period:
            # Replace months >= recompute_start_period with recomputed data
            final_df = replace_month_window(prev, merged, recompute_start_period)
            print(f"  Replaced months from {recompute_start_period} onward")
        else:
            # Incremental mode: simple append
            if prev is not None and not prev.empty:
                final_df = pd.concat([prev, merged], ignore_index=True)
                # Remove any duplicates (keep latest for each loan_id + date)
                final_df.drop_duplicates(subset=["loan_id", "date"], keep="last", inplace=True)
            else:
                final_df = merged
        
        if final_df.empty:
            print("  No output rows generated")
            return "NO_OUTPUT_ROWS"
        
        print(f"  Final rows: {len(final_df):,}")
        
        # Convert date to datetime
        final_df["date"] = pd.to_datetime(final_df["date"])
        
        # ============================================================
        # STEP 10: Add pipeline metadata
        # ============================================================
        print("[STEP 10] Adding pipeline metadata...")
        
        final_df["pipeline_name"] = "enrich_loans_pipeline"
        final_df["pipeline_run_id"] = run_id
        final_df["run_mode"] = mode
        final_df["data_source"] = "s3 + snowflake"
        final_df["input_source"] = "loan_base + fx + bonds + commodity + derivatives + collateral"
        final_df["transformation"] = "loan_enrichment_monthly_v1"
        final_df["record_created_at"] = datetime.utcnow()
        
        
        # ============================================================
        # STEP 11: Write to S3 (ATOMIC - CRITICAL - sends alert on failure)
        # ============================================================
        print("[STEP 11] Writing to S3 (atomic write)...")
        
        write_s3_parquet_atomic(final_df, OUTPUT_KEY, run_id=run_id)
        
        # ============================================================
        # STEP 12: Success summary (NO SLACK ALERT - not final pipeline)
        # ============================================================
        total_time = time.time() - pipeline_start
        
        print(f"\n{'='*66}")
        print(f"  LOAN ENRICHMENT PIPELINE SUCCESS - run_id={run_id}")
        print(f"  Mode: {mode}")
        print(f"  Final rows: {len(final_df):,}")
        print(f"  Succeeded tables: {succeeded_tables}")
        print(f"  Duration: {total_time:.1f}s")
        print(f"{'='*66}\n")
        
        # Write run summary to S3
        run_summary = {
            "pipeline_run_id": run_id,
            "pipeline_name": "enrich_loans_pipeline",
            "run_ts": run_ts.isoformat(),
            "status": "SUCCESS",
            "mode": mode,
            "start_date": str(start_date_override) if start_date_override else None,
            "rows_processed": len(final_df),
            "processing_time_s": round(total_time, 2),
            "tables_loaded": succeeded_tables,
            "output_key": OUTPUT_KEY,
            "airflow": airflow_metadata,
        }
        write_run_summary(run_summary)
        
        return "SUCCESS"
    
    except Exception as e:
        total_time = time.time() - pipeline_start
        
        # Send CRITICAL alert for any unhandled exception
        send_critical_alert(
            f"Pipeline failed with unhandled exception",
            context={"run_id": run_id, "mode": mode, "error": str(e)}
        )
        
        print(f"\n{'='*66}")
        print(f"  LOAN ENRICHMENT PIPELINE FAILED - run_id={run_id}")
        print(f"  Mode: {mode}")
        print(f"  Error: {str(e)}")
        print(f"  Succeeded tables: {succeeded_tables}")
        print(f"  Duration: {total_time:.1f}s")
        print(f"{'='*66}\n")
        
        # Write failure summary to S3
        run_summary = {
            "pipeline_run_id": run_id,
            "pipeline_name": "enrich_loans_pipeline",
            "run_ts": run_ts.isoformat(),
            "status": "FAILED",
            "mode": mode,
            "start_date": str(start_date_override) if start_date_override else None,
            "processing_time_s": round(total_time, 2),
            "tables_loaded": succeeded_tables,
            "error": str(e),
            "airflow": airflow_metadata,
        }
        try:
            write_run_summary(run_summary)
        except Exception:
            pass  # Don't mask the original exception
        
        # Re-raise for Airflow to catch
        raise