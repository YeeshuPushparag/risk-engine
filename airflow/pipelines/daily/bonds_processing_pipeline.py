"""
bonds_processing_pipeline.py
==============================
SECOND PIPELINE — Feature Loading + ML Predictions + Database Writes

Responsibilities
----------------
- Load Layer 2 features from S3 (mode-aware strategy):
    Incremental  <- rolling 30-day serving parquet
    Backfill/Replay <- latest Layer 2 partition per date (by S3 LastModified)
- Rename first-pipeline lineage columns to source_* prefix
- Run XGBoost ML predictions (pred_spread_5d, pred_pd_21d)
- Attach second-pipeline lineage metadata
- Validate output
- Write Snowflake BONDS_HISTORY (append-only, always INSERT, includes run_mode)
- Write Snowflake BONDS (clean deterministic state):
    Replay / Backfill <- DELETE window <- INSERT (atomic transaction)
    Incremental       <- MERGE latest-state rows using
                     (bond_id, ticker, date)
- Write Postgres bond_data (serving layer, trim to last 2 days)
- NO direct FRED API calls
- NO S3 raw snapshot reads (raw is first-pipeline's concern)

Storage reads
-------------
Incremental:
    s3://<bucket>/historical-bonds/rolling/bonds_features_30d.parquet
    Watermark: MAX(date) from Snowflake BONDS clean table

Backfill / Replay:
    s3://<bucket>/historical-bonds/features/
        year=Y/month=MM/day=DD/run_id=<id>/data.parquet   <- latest per date

Latest partition resolution (CRITICAL)
---------------------------------------
Multiple run_id folders may exist for the same date partition
(e.g. original run + replay + correction). The pipeline MUST load only
the latest parquet for each date, determined by S3 LastModified.

Loading ALL run_ids for a date causes:
  - stale data contamination
  - duplicate feature states
  - non-deterministic replay behaviour

Implemented by: get_latest_feature_keys() + load_bond_features_from_layer2()

Failure semantics
-----------------
Pipeline is consistency-first, NOT availability-first.
  - Snowflake BONDS_HISTORY failure <- FAIL
  - Snowflake BONDS failure         <- FAIL
  - Postgres failure                <- FAIL
Partial success is NOT allowed under any mode.

Mode detection
--------------
Derived exclusively from function parameters — never from data columns.
    replay=True                   <- mode = "replay"
    start_date_override provided  <- mode = "backfill"
    default                       <- mode = "incremental"

Airflow notes
-------------
- Entry point: process_bonds(start_date_override, replay)
- DAG order: run_bonds_update >> run_bonds_processing
  (This pipeline always runs after the first pipeline has produced Layer 2.)
- Recommended: retries=2, retry_delay=timedelta(minutes=5)
- max_active_runs=1 (Snowflake writes are not concurrent-safe)
"""

import os
import json
import time
import pandas as pd
import numpy as np
import xgboost as xgb
import joblib
import boto3
import requests
from botocore.exceptions import ClientError
from datetime import datetime, timedelta, timezone, date as date_type
from io import BytesIO, StringIO
from connections.snowflake_conn import get_snowflake_conn
from connections.postgre_conn import get_postgre_conn
from snowflake.connector.pandas_tools import write_pandas
import pendulum

# =============================================================
# CONFIG  —  single source of truth. No magic numbers in code.
# =============================================================

CONFIG: dict = {
    # S3 bucket
    "s3_bucket": "yeeshu-bond-bucket",

    # Macro Data Source
    "macro_bucket": "yeeshu-loan-bucket",
    "macro_key":    "macro_data.csv",


    # S3 path prefixes (mirrors first pipeline CONFIG)
    "features_prefix": "historical-bonds/features/",
    "rolling_key":     "historical-bonds/rolling/bonds_features_30d.parquet",

    # S3 model artefact paths (at bucket root)
    "model_spread_key":    "bond_model_spread5d_xgb.json",
    "model_pd_key":        "bond_model_pd21d_xgb.json",
    "features_spread_key": "bond_features_spread5d.pkl",
    "features_pd_key":     "bond_features_pd21d.pkl",

    # Snowflake table names — single source of truth for table identifiers
    "snowflake_clean_table":   "BONDS",           # deterministic latest-state table
    "snowflake_history_table": "BONDS_HISTORY",   # append-only audit/lineage table
    "snowflake_chunk":         20_000,

    # PostgreSQL
    "postgres_table":  "bond_data",
    "postgres_schema": "public",

    # Final output columns for Snowflake CLEAN + Postgres (ordered contract).
    "final_cols": [
        "bond_id", "ticker", "sector", "industry", "credit_rating",
        "coupon_rate", "issue_date", "maturity_date", "maturity_years",
        "date", "benchmark_yield", "corporate_yield", "credit_spread",
        "bond_price", "yield_to_maturity", "implied_hazard",
        "implied_pd_annual", "implied_pd_multi_year", "implied_rating",
        "market_synthetic_score",
        "DGS10", "DGS10_ma", "dgs10_anom", "fill_method_flag",
        "gdp", "unrate", "fedfunds", "cpi",
        "pred_spread_5d", "pred_pd_21d", "vol",
        "issue_size",
        "units_issued",
        "units_outstanding",
        "market_value",
        "outstanding_pct",
        # Source (first pipeline) lineage
        "source_pipeline", "source_run_id", "source_data_source",
        "source_input_source", "source_transformation", "source_created_at",
        # Second pipeline lineage
        "pipeline_name", "pipeline_run_id", "data_source",
        "input_source", "transformation", "record_created_at",
    ],

    # Lineage — bump transformation when model or output logic changes
    "pipeline_name":  "bonds_processing_pipeline",
    "data_source":    "layer2_features+xgboost",
    "input_source":   "bonds_features_30d+layer2_partitions",
    "transformation": "bond_processing_v1",
}

# Aliases (avoids CONFIG["..."] noise at call sites)
SNOWFLAKE_CLEAN_TABLE   = CONFIG["snowflake_clean_table"]    # "BONDS"
SNOWFLAKE_HISTORY_TABLE = CONFIG["snowflake_history_table"]  # "BONDS_HISTORY"


# =============================================================
# CUSTOM EXCEPTIONS
# =============================================================

class BondProcessingError(Exception):
    """Raised for unrecoverable second-pipeline failures."""


class DataValidationError(Exception):
    """Raised when output data validation checks fail."""


# =============================================================
# PRODUCTION-GRADE ALERTING (CRITICAL ONLY)
# =============================================================

def send_critical_alert(message: str, context: dict = None) -> None:
    """
    Send CRITICAL alert to Slack only for unrecoverable pipeline failures.

    Triggers for:
    - Any Snowflake write failure (HISTORY or CLEAN)
    - Any Postgres write failure
    - Layer 2 feature load failure (after retries)
    - Output validation failure
    - Any unhandled pipeline exception

    Does NOT trigger for:
    - Individual retry attempts (only on final exhaustion)
    - Model/feature load failures (soft-fail: prediction columns set to NaN)
    """
    payload = {
        "level":     "CRITICAL",
        "pipeline":  CONFIG["pipeline_name"],
        "message":   message,
        "context":   context or {},  
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    print(f"[CRITICAL] {message} | context={payload['context']}")

    webhook = os.getenv("SLACK_WEBHOOK_URL")
    if webhook:
        try:
            truncated = message[:500] + "..." if len(message) > 500 else message
            run_id    = context.get("run_id", "unknown") if context else "unknown"
            text = (
                f"*[CRITICAL]* {CONFIG['pipeline_name']}\n"
                f"{truncated}\n"
                f"run_id: {run_id}"
            )
            requests.post(webhook, json={"text": text}, timeout=3)
        except Exception as e:
            print(f"[ALERT ERROR] Slack notification failed: {e}")





# =============================================================
# RUN SUMMARY WRITER
# =============================================================

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
            - end_date
            - rows_processed
            - processing_time_s
            - tables_written (list)
            - error (if failed)
    """
    key = (
        f"historical-bonds/bond-risk/metadata/"
        f"pipeline_run_id={summary['pipeline_run_id']}/"
        f"run_summary.json"
    )

    try:
        get_s3().put_object(
            Bucket=CONFIG["s3_bucket"],
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


# =============================================================
# RETRY WITH EXPONENTIAL BACKOFF
# =============================================================

def retry_with_backoff(
    func,
    retries=3,
    backoff_factor=2,
    exceptions=(Exception,),
    critical_name=None,
    run_id=None,
):
    """
    Retry a callable with exponential backoff.

    Sends CRITICAL alert only when all retries are exhausted AND
    critical_name is provided. Individual retry attempts log to stdout only.

    Args:
        func:           Zero-argument callable to retry.
        retries:        Maximum number of retry attempts (not counting first).
        backoff_factor: Wait = backoff_factor ** attempt.
        exceptions:     Exception types to catch and retry.
        critical_name:  Human-readable operation name for alert messages.
                        Pass None for non-critical / soft-fail operations.
        run_id:         Pipeline run identifier for alert context.

    Returns:
        Return value of func() on success.

    Raises:
        Last caught exception when all retries are exhausted.
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

    if critical_name:
        send_critical_alert(
            f"{critical_name} failed after {retries} retries",
            context={"run_id": run_id, "error": str(last_exception)},
        )

    raise last_exception


# =============================================================
# MARKET DATE HELPERS
# =============================================================

def get_market_end_date() -> date_type:
    """
    Determine the appropriate end date for market data processing.
    
    - If before 4:00 PM ET (market close): use yesterday
    - If after 4:00 PM ET: use today
    
    This ensures you don't pull incomplete current-day data before market close.
    """
    # Get current time in US Eastern timezone
    eastern = pendulum.timezone("America/New_York")
    
    now_et = datetime.now(eastern)
    current_date_et = now_et.date()
    current_time_et = now_et.time()
    
    # Market close is 4:00 PM ET
    market_close = datetime.strptime("16:00", "%H:%M").time()
    
    if current_time_et < market_close:
        # Before market close -> use yesterday
        end_date = current_date_et - timedelta(days=1)
        print(f"  [MARKET] Before 4:00 PM ET — using yesterday as end_date: {end_date}")
    else:
        # After market close -> use today
        end_date = current_date_et
        print(f"  [MARKET] After 4:00 PM ET — using today as end_date: {end_date}")
    
    return end_date

# =============================================================
# TIMEZONE HELPERS
# =============================================================

def utc_now() -> datetime:
    """Current datetime as a tz-aware UTC object."""
    return datetime.now(timezone.utc)


def today_utc() -> date_type:
    """Today's date in UTC."""
    return utc_now().date()


# =============================================================
# S3 HELPERS
# =============================================================

def get_s3():
    """Return a boto3 S3 client per-call to avoid stale sessions."""
    return boto3.client("s3")


def s3_key_exists(bucket: str, key: str) -> bool:
    """Return True if the S3 key exists, False otherwise."""
    try:
        get_s3().head_object(Bucket=bucket, Key=key)
        return True
    except ClientError:
        return False


def read_csv_from_s3(bucket: str, key: str) -> pd.DataFrame:
    """Read a CSV from S3. Raises BondProcessingError on failure."""
    try:
        print(f"  [S3 READ CSV]     s3://{bucket}/{key}")
        obj = get_s3().get_object(Bucket=bucket, Key=key)
        return pd.read_csv(BytesIO(obj["Body"].read()))
    except ClientError as exc:
        raise BondProcessingError(f"Failed to read s3://{bucket}/{key}: {exc}") from exc


def _read_parquet_from_s3(bucket: str, key: str) -> pd.DataFrame:
    """Read a Parquet from S3. Raises BondProcessingError on failure."""
    try:
        print(f"  [S3 READ PARQUET] s3://{bucket}/{key}")
        obj = get_s3().get_object(Bucket=bucket, Key=key)
        return pd.read_parquet(BytesIO(obj["Body"].read()))
    except ClientError as exc:
        raise BondProcessingError(f"Failed to read s3://{bucket}/{key}: {exc}") from exc


def _read_bytes_from_s3(bucket: str, key: str) -> bytes:
    """Read raw bytes from S3. Raises BondProcessingError on failure."""
    try:
        print(f"  [S3 READ BYTES]   s3://{bucket}/{key}")
        return get_s3().get_object(Bucket=bucket, Key=key)["Body"].read()
    except ClientError as exc:
        raise BondProcessingError(f"Failed to read s3://{bucket}/{key}: {exc}") from exc




# =============================================================
# MACRO DATA LOADER 
# =============================================================
def load_macro_data(run_id: str) -> pd.DataFrame:
    """Load macro data from S3 using CONFIG macro_bucket and macro_key."""
    return retry_with_backoff(
        lambda: read_csv_from_s3(CONFIG["macro_bucket"], CONFIG["macro_key"]),
        retries=3,
        critical_name="Macro data load",
        run_id=run_id,
    )

# =============================================================
# LAYER 2 FEATURE LOADING — latest-partition-per-date strategy
# =============================================================

def get_latest_feature_keys(
    bucket:     str,
    start_date: date_type,
    end_date:   date_type,
) -> dict:
    """
    For each business day in [start_date, end_date], find the S3 key for the
    LATEST run_id partition by S3 LastModified timestamp.

    Multiple run_id folders may exist per date (original run + replay +
    corrections). Loading all of them would cause duplicate rows and
    non-deterministic results. This function guarantees exactly ONE key per
    date — always the most recently written one.

    Algorithm:
      For each business day:
        1. List all objects under:
               historical-bonds/features/year=Y/month=MM/day=DD/
           using a paginator (handles >1000 objects safely).
        2. Among objects whose key ends with "data.parquet", select the
           one with the greatest LastModified timestamp.
        3. If no objects found, log a warning (not fatal — the date will
           simply be absent from the loaded DataFrame).

    Args:
        bucket:     S3 bucket name.
        start_date: Inclusive window start.
        end_date:   Inclusive window end.

    Returns:
        dict: {date_type <- s3_key_str} — one entry per date with data.
              Dates with no partitions are omitted with a WARNING log.

    Raises:
        BondProcessingError: If S3 listing itself fails (not just missing keys).
    """
    s3_client   = get_s3()
    result      = {}
    business_days = pd.bdate_range(
        pd.Timestamp(start_date),
        pd.Timestamp(end_date),
    ).date  # array of Python date objects — business days only

    for current_date in business_days:
        date_prefix = (
            f"{CONFIG['features_prefix']}"
            f"year={current_date.year}/"
            f"month={current_date.month:02d}/"
            f"day={current_date.day:02d}/"
        )

        try:
            paginator = s3_client.get_paginator("list_objects_v2")
            pages     = paginator.paginate(Bucket=bucket, Prefix=date_prefix)
        except ClientError as exc:
            raise BondProcessingError(
                f"S3 listing failed for prefix s3://{bucket}/{date_prefix}: {exc}"
            ) from exc

        latest_key      = None
        latest_modified = None

        for page in pages:
            for obj in page.get("Contents", []):
                key      = obj["Key"]
                modified = obj["LastModified"]
                # Only consider files named data.parquet (one per run_id folder)
                if not key.endswith("data.parquet"):
                    continue
                if latest_modified is None or modified > latest_modified:
                    latest_key      = key
                    latest_modified = modified

        if latest_key:
            result[current_date] = latest_key
        else:
            print(
                f"  [LAYER2 LOAD][WARN] No feature partition found for "
                f"date={current_date} — skipping."
            )

    print(
        f"  [LAYER2 LOAD] Resolved {len(result)} latest partitions "
        f"for window [{start_date}, {end_date}]."
    )
    return result


def load_bond_features_from_layer2(
    bucket:     str,
    start_date: date_type,
    end_date:   date_type,
    run_id:     str,
) -> pd.DataFrame:
    """
    Load Layer 2 features for a date window using the latest-partition strategy.

    Steps:
      1. Call get_latest_feature_keys() to resolve one S3 key per date.
      2. Load each parquet file and concatenate.
      3. Normalise the date column and sort by (bond_id, date).

    Raises BondProcessingError if no partitions are found for the window
    (the first pipeline must run before this pipeline for any given window).

    Args:
        bucket:     S3 bucket name.
        start_date: Inclusive window start.
        end_date:   Inclusive window end.
        run_id:     Current pipeline run_id (used for alert context only).

    Returns:
        Concatenated DataFrame of all latest partitions in the window.

    Raises:
        BondProcessingError: If no partitions found or S3 read fails.
    """
    latest_keys = retry_with_backoff(
        lambda: get_latest_feature_keys(bucket, start_date, end_date),
        retries=3,
        critical_name=f"Layer 2 partition key resolution [{start_date}, {end_date}]",
        run_id=run_id,
    )

    if not latest_keys:
        raise BondProcessingError(
            f"No Layer 2 feature partitions found for window [{start_date}, {end_date}]. "
            f"Ensure bonds_update_pipeline ran successfully for this window first."
        )

    frames = []
    for current_date, key in sorted(latest_keys.items()):
        try:
            df = _read_parquet_from_s3(bucket, key)
            frames.append(df)
        except BondProcessingError as exc:
            # A single partition load failure is unrecoverable — the window
            # would be incomplete, which violates consistency-first contract.
            send_critical_alert(
                f"Layer 2 partition load failed for date={current_date}",
                context={"run_id": run_id, "key": key, "error": str(exc)},
            )
            raise

    combined         = pd.concat(frames, ignore_index=True)
    combined["date"] = pd.to_datetime(combined["date"]).dt.date
    combined         = combined.sort_values(["bond_id", "date"]).reset_index(drop=True)

    print(
        f"  [LAYER2 LOAD] Loaded {len(combined):,} rows from "
        f"{len(latest_keys)} partitions."
    )
    return combined


def load_rolling_features(bucket: str, run_id: str) -> pd.DataFrame:
    """
    Load the 30-day rolling serving parquet for incremental mode.

    Raises BondProcessingError if the rolling parquet is absent — the first
    pipeline must have run successfully before the second pipeline can operate
    in incremental mode.

    Args:
        bucket: S3 bucket name.
        run_id: Current pipeline run_id (used for alert context only).

    Returns:
        DataFrame from the rolling parquet with date column normalised.
    """
    key = CONFIG["rolling_key"]

    def _load():
        if not s3_key_exists(bucket, key):
            raise BondProcessingError(
                f"Rolling parquet not found: s3://{bucket}/{key}. "
                f"Run bonds_update_pipeline first."
            )
        return _read_parquet_from_s3(bucket, key)

    df          = retry_with_backoff(
        _load,
        retries=3,
        critical_name=f"Rolling parquet load: {key}",
        run_id=run_id,
    )
    df["date"]  = pd.to_datetime(df["date"]).dt.date
    print(
        f"  [ROLLING LOAD] Loaded {len(df):,} rows from rolling parquet "
        f"(date range: {min(df['date'])} <- {max(df['date'])})."
    )
    return df


# =============================================================
# LINEAGE HELPERS
# =============================================================

def rename_source_lineage(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rename first-pipeline lineage columns to source_* prefix.

    This preserves the full provenance chain: the final Snowflake/Postgres
    output contains both source_* (first pipeline) and pipeline_* (second
    pipeline) lineage side by side.
    
    Drop first pipeline's run_mode - second pipeline adds its own.
    
    """
    rename_map = {
        "pipeline_name":     "source_pipeline",
        "pipeline_run_id":   "source_run_id",
        "data_source":       "source_data_source",
        "input_source":      "source_input_source",
        "transformation":    "source_transformation",
        "record_created_at": "source_created_at",
    }
    
    existing = {k: v for k, v in rename_map.items() if k in df.columns}
    df = df.rename(columns=existing)
    
    # Drop first pipeline's run_mode - not needed in second pipeline
    if "run_mode" in df.columns:
        df = df.drop(columns=["run_mode"])
    
    return df


def drop_metadata_for_serving(df: pd.DataFrame) -> pd.DataFrame:
    """
    Strip all pipeline lineage columns before writing to Postgres serving layer.

    run_mode is history-table-only; all other lineage columns are dropped from
    the Postgres write to keep the serving table lean.
    """
    drop_cols = [
        "source_pipeline", "source_run_id", "source_data_source",
        "source_input_source", "source_transformation", "source_created_at",
        "pipeline_name", "pipeline_run_id", "data_source",
        "input_source", "transformation", "record_created_at",
        "fill_method_flag"
    ]
    return df.drop(columns=[c for c in drop_cols if c in df.columns])


def _add_pipeline_metadata(
    df:     pd.DataFrame,
    run_id: str,
    run_ts: datetime,
) -> pd.DataFrame:
    """Stamp every output row with second-pipeline lineage."""
    df = df.copy()
    df["pipeline_name"]     = CONFIG["pipeline_name"]
    df["pipeline_run_id"]   = run_id
    df["data_source"]       = CONFIG["data_source"]
    df["input_source"]      = CONFIG["input_source"]
    df["transformation"]    = CONFIG["transformation"]
    df["record_created_at"] = run_ts.isoformat()
    return df


# =============================================================
# ML PREDICTIONS  (UNCHANGED logic — soft-fail on model load)
# =============================================================

def run_predictions(df: pd.DataFrame, bucket: str) -> pd.DataFrame:
    """
    Load XGBoost models + feature lists from S3 and generate:
      - pred_spread_5d
      - pred_pd_21d

    Model loading failure <- soft-fail (prediction columns set to NaN,
    pipeline continues — risk metrics from feature engineering are still valid).

    Feature / data issues in a successfully loaded model <- DataValidationError
    (hard fail — a model with correct features but bad data is untrustworthy).

    Unchanged from original pipeline.
    """
    if df.empty:
        return df

    # ── Load models + feature lists ──────────────────────────────────────
    try:
        model_spread = xgb.Booster()
        model_spread.load_model(
            bytearray(_read_bytes_from_s3(bucket, CONFIG["model_spread_key"]))
        )
        model_pd = xgb.Booster()
        model_pd.load_model(
            bytearray(_read_bytes_from_s3(bucket, CONFIG["model_pd_key"]))
        )
        features_spread = joblib.load(
            BytesIO(_read_bytes_from_s3(bucket, CONFIG["features_spread_key"]))
        )
        features_pd = joblib.load(
            BytesIO(_read_bytes_from_s3(bucket, CONFIG["features_pd_key"]))
        )
        model_loaded = True

    except BondProcessingError as exc:
        print(
            f"  [PREDICTIONS][WARN] Model loading failed — "
            f"prediction columns set to NaN: {exc}"
        )
        model_loaded = False

    if not model_loaded:
        df = df.copy()
        df["pred_spread_5d"] = np.nan
        df["pred_pd_21d"]    = np.nan
        return df

    df = df.copy()

    # ── Credit spread model ───────────────────────────────────────────────
    missing_spread = [f for f in features_spread if f not in df.columns]
    if missing_spread:
        raise DataValidationError(
            f"[BOND MODEL] Missing spread model features: {missing_spread}"
        )
    null_spread = [c for c in features_spread if df[c].isna().any()]
    if null_spread:
        raise DataValidationError(
            f"[BOND MODEL] Null values in spread model features: {null_spread}"
        )
    df["pred_spread_5d"] = model_spread.predict(xgb.DMatrix(df[features_spread]))
    print(f"  [PREDICTIONS] pred_spread_5d computed for {len(df):,} rows.")

    # ── PD model ─────────────────────────────────────────────────────────
    missing_pd = [f for f in features_pd if f not in df.columns]
    if missing_pd:
        raise DataValidationError(
            f"[BOND MODEL] Missing PD model features: {missing_pd}"
        )
    null_pd = [c for c in features_pd if df[c].isna().any()]
    if null_pd:
        raise DataValidationError(
            f"[BOND MODEL] Null values in PD model features: {null_pd}"
        )
    df["pred_pd_21d"] = model_pd.predict(xgb.DMatrix(df[features_pd]))
    print(f"  [PREDICTIONS] pred_pd_21d computed for {len(df):,} rows.")

    return df


# =============================================================
# OUTPUT VALIDATION  (UNCHANGED)
# =============================================================

def validate_output(
    df:       pd.DataFrame,
    run_date: date_type,
    run_id:   str,
) -> None:
    """
    Validate the final DataFrame before any database write.
    Raises DataValidationError on hard failure and sends CRITICAL alert.
    """
    errors = []

    if df.empty:
        errors.append("Output DataFrame is empty — nothing to upload.")

    else:
        # Freshness — validate against latest business day
        max_date = pd.to_datetime(df["date"]).dt.date.max()

        expected_business_date = (
            pd.bdate_range(end=pd.Timestamp(run_date), periods=1)[0]
            .date()
        )

        if max_date < expected_business_date:
            errors.append(
                f"Freshness check failed: latest date={max_date}, "
                f"expected >= {expected_business_date}"
            )

        # fill_method_flag integrity
        if "fill_method_flag" not in df.columns:
            errors.append("fill_method_flag column is missing.")
        else:
            valid_flags   = {"REAL", "FORWARD_FILLED", "BACKWARD_FILLED"}
            invalid_flags = set(df["fill_method_flag"].dropna().unique()) - valid_flags
            if invalid_flags:
                errors.append(
                    f"Unexpected fill_method_flag values: {invalid_flags}"
                )

    if errors:
        send_critical_alert(
            "Bond processing pipeline output validation failed",
            context={"run_id": run_id, "errors": errors},
        )
        raise DataValidationError("; ".join(errors))

    fill_summary = (
        df["fill_method_flag"].value_counts().to_dict()
        if "fill_method_flag" in df.columns else {}
    )
    print(
        f"  [VALIDATION] OK — {len(df):,} rows, "
        f"max_date={max_date}, fill_flags={fill_summary}"
    )


# =============================================================
# SNOWFLAKE HISTORY TABLE (append-only, always INSERT)
# =============================================================

def write_to_snowflake_history(df: pd.DataFrame, run_mode: str, run_id: str) -> int:
    """
    Append rows to BONDS_HISTORY. Never updates or deletes existing rows.

    Adds a run_mode column so every row is traceable to the exact pipeline
    run that produced it. Duplicates across runs are expected by design —
    this table is an immutable audit trail.

    Raises BondProcessingError on failure (after retries). Pipeline does NOT
    continue if this write fails.

    Args:
        df:       Fully processed DataFrame (without run_mode).
        run_mode: One of "incremental", "backfill", "replay".
        run_id:   Pipeline run identifier for alert context.

    Returns:
        Number of rows inserted.
    """
    df_hist             = df.copy()
    df_hist["run_mode"] = run_mode   # lineage column — history table only

    def do_insert():
        with get_snowflake_conn() as ctx:
            success, _, nrows, _ = write_pandas(
                ctx,
                df_hist,
                SNOWFLAKE_HISTORY_TABLE,
                chunk_size        = CONFIG["snowflake_chunk"],
                quote_identifiers = True,
            )
            if not success:
                raise RuntimeError(
                    f"write_pandas returned success=False for {SNOWFLAKE_HISTORY_TABLE}"
                )
            print(
                f"  [HISTORY] Inserted {nrows:,} rows into {SNOWFLAKE_HISTORY_TABLE}."
            )
            return nrows

    try:
        return retry_with_backoff(
            do_insert,
            retries=3,
            critical_name=f"Snowflake HISTORY insert ({SNOWFLAKE_HISTORY_TABLE})",
            run_id=run_id,
        )
    except Exception as exc:
        send_critical_alert(
            f"Snowflake HISTORY insert failed for table {SNOWFLAKE_HISTORY_TABLE}",
            context={
                "run_id": run_id,
                "table":  SNOWFLAKE_HISTORY_TABLE,
                "rows":   len(df),
                "error":  str(exc),
            },
        )
        raise BondProcessingError(
            f"Snowflake HISTORY insert failed: {exc}"
        ) from exc


# =============================================================
# SNOWFLAKE CLEAN TABLE — DELETE + INSERT (replay / backfill)
# =============================================================

def _snowflake_clean_delete_insert(
    df: pd.DataFrame,
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
    run_id: str,
) -> None:
    """
    Atomically replace a date window in the BONDS clean table.

    Architecture
    ------------
    BONDS_HISTORY:
        append-only audit table
        duplicates across runs are expected and allowed

    BONDS:
        deterministic latest-state table
        must NEVER contain conflicting rows for the same business key

    Replay/backfill semantics
    -------------------------
    Replay and backfill recompute an entire historical window and replace
    that window atomically inside the clean table.

    Business uniqueness
    -------------------
    bond_id + ticker + date

    Protocol
    --------
    1. Create a TEMP staging table.
    2. Bulk load dataframe into staging via write_pandas().
    3. BEGIN explicit Snowflake transaction.
    4. DELETE existing rows for target replay/backfill window.
    5. INSERT recomputed rows from staging table.
    6. COMMIT transaction.
    7. ROLLBACK automatically on any failure.

    Guarantees
    ----------
    - No partial replay state.
    - No conflicting clean-table rows.
    - Deterministic replay/backfill behavior.
    - Temp staging table disappears automatically after session close.

    Important Notes
    ---------------
    write_pandas() itself is NOT transactional because Snowflake internally
    uses staged COPY operations. Therefore staging must occur BEFORE opening
    the explicit transaction.

    Args
    ----
    df:
        Fully processed dataframe ready for Snowflake clean-table write.

    start_date:
        Inclusive replay/backfill window start.

    end_date:
        Inclusive replay/backfill window end.

    run_id:
        Pipeline run identifier for observability and alerting.

    Raises
    ------
    BondProcessingError:
        Raised after rollback and critical alert emission.
    """

    start_str = (
        pd.Timestamp(start_date)
        .strftime("%Y-%m-%d")
    )

    end_str = (
        pd.Timestamp(end_date)
        .strftime("%Y-%m-%d")
    )

    temp_table = (
        f"{SNOWFLAKE_CLEAN_TABLE}_STAGE_"
        f"{int(time.time())}"
    )

    try:

        with get_snowflake_conn() as ctx:

            with ctx.cursor() as cs:

                # =====================================================
                # STEP 1 — CREATE TEMP STAGING TABLE
                # =====================================================

                cs.execute(f"""
                    CREATE TEMPORARY TABLE {temp_table}
                    LIKE "{SNOWFLAKE_CLEAN_TABLE}"
                """)

                # =====================================================
                # STEP 2 — LOAD DATAFRAME INTO STAGING TABLE
                # =====================================================

                write_pandas(
                    conn=ctx,
                    df=df,
                    table_name=temp_table,
                    chunk_size=CONFIG["snowflake_chunk"],
                    quote_identifiers=True,
                    auto_create_table=False,
                )

                # =====================================================
                # STEP 3 — OPEN EXPLICIT TRANSACTION
                # =====================================================

                cs.execute("BEGIN")

                try:

                    # =================================================
                    # STEP 4 — DELETE EXISTING WINDOW
                    # =================================================

                    cs.execute(f"""
                        DELETE FROM "{SNOWFLAKE_CLEAN_TABLE}"
                        WHERE "date"
                        BETWEEN '{start_str}'::DATE
                            AND '{end_str}'::DATE
                    """)

                    deleted_rows = (
                        cs.rowcount
                        if cs.rowcount is not None
                        else 0
                    )

                    print(
                        f"  [CLEAN] Deleted "
                        f"{deleted_rows:,} rows from "
                        f"{SNOWFLAKE_CLEAN_TABLE} "
                        f"for window "
                        f"[{start_str}, {end_str}]."
                    )

                    # =================================================
                    # STEP 5 — INSERT RECOMPUTED WINDOW
                    # =================================================

                    cs.execute(f"""
                        INSERT INTO "{SNOWFLAKE_CLEAN_TABLE}"
                        SELECT *
                        FROM {temp_table}
                    """)

                    inserted_rows = (
                        cs.rowcount
                        if cs.rowcount is not None
                        else len(df)
                    )

                    print(
                        f"  [CLEAN] Inserted "
                        f"{inserted_rows:,} rows into "
                        f"{SNOWFLAKE_CLEAN_TABLE}."
                    )

                    # =================================================
                    # STEP 6 — COMMIT TRANSACTION
                    # =================================================

                    cs.execute("COMMIT")

                except Exception:

                    # =============================================
                    # STEP 7 — ROLLBACK ON FAILURE
                    # =============================================

                    cs.execute("ROLLBACK")

                    raise

    except Exception as exc:

        send_critical_alert(
            (
                "Snowflake CLEAN delete+insert failed "
                f"for window [{start_str}, {end_str}]"
            ),
            context={
                "run_id": run_id,
                "table": SNOWFLAKE_CLEAN_TABLE,
                "mode": "delete_insert",
                "start": start_str,
                "end": end_str,
                "rows": len(df),
                "error": str(exc),
            },
        )

        raise BondProcessingError(
            f"Snowflake CLEAN delete+insert failed: {exc}"
        ) from exc

# =============================================================
# SNOWFLAKE CLEAN TABLE — MERGE (incremental)
# =============================================================

def _snowflake_clean_merge(
    df: pd.DataFrame,
    run_id: str,
) -> int:
    """
    Incremental UPSERT into BONDS clean table.

    Business key:
        (bond_id, ticker, date)

    Guarantees:
    - deterministic latest-state rows
    - idempotent retries
    - replay overlap safety
    - no conflicting clean-table rows
    """

    key_columns = [
        "bond_id",
        "ticker",
        "date",
    ]

    # =========================================================
    # DUPLICATE VALIDATION
    # =========================================================

    dupes = (
        df.groupby(key_columns)
        .size()
        .reset_index(name="cnt")
    )

    dup_rows = dupes[dupes["cnt"] > 1]

    if not dup_rows.empty:

        sample = dup_rows.head(10)

        raise ValueError(
            "Duplicate bond business keys detected "
            f"before MERGE. Sample:\n{sample}"
        )

    temp_table = (
        f"{SNOWFLAKE_CLEAN_TABLE}_STAGE_"
        f"{int(time.time())}"
    )

    try:

        with get_snowflake_conn() as ctx:

            with ctx.cursor() as cs:

                # =====================================================
                # CREATE TEMP STAGE TABLE
                # =====================================================

                cs.execute(f'''
                    CREATE TEMPORARY TABLE {temp_table}
                    LIKE "{SNOWFLAKE_CLEAN_TABLE}"
                ''')

                # =====================================================
                # LOAD INTO STAGE TABLE
                # =====================================================

                success, _, nrows, _ = write_pandas(
                    conn=ctx,
                    df=df,
                    table_name=temp_table,
                    chunk_size=CONFIG["snowflake_chunk"],
                    quote_identifiers=True,
                    auto_create_table=False,
                )

                if not success:
                    raise RuntimeError(
                        "write_pandas failed for temp stage table"
                    )

                # =====================================================
                # BUILD MERGE SQL
                # =====================================================

                all_columns = list(df.columns)

                update_columns = [
                    c for c in all_columns
                    if c not in key_columns
                ]

                merge_on = " AND ".join([
                    f't."{c}" = s."{c}"'
                    for c in key_columns
                ])

                update_set = ", ".join([
                    f't."{c}" = s."{c}"'
                    for c in update_columns
                ])

                insert_columns = ", ".join([
                    f'"{c}"'
                    for c in all_columns
                ])

                insert_values = ", ".join([
                    f's."{c}"'
                    for c in all_columns
                ])

                merge_sql = f"""
                    MERGE INTO "{SNOWFLAKE_CLEAN_TABLE}" t
                    USING {temp_table} s
                    ON {merge_on}

                    WHEN MATCHED THEN
                        UPDATE SET
                            {update_set}

                    WHEN NOT MATCHED THEN
                        INSERT ({insert_columns})
                        VALUES ({insert_values})
                """

                # =====================================================
                # EXECUTE MERGE
                # =====================================================

                cs.execute(merge_sql)

                affected_rows = cs.rowcount if cs.rowcount is not None else len(df)

                print(
                    f"  [CLEAN] MERGE complete — "
                    f"{affected_rows:,} rows affected in "
                    f"{SNOWFLAKE_CLEAN_TABLE}"
                )

                return nrows

    except Exception as exc:

        send_critical_alert(
            "Snowflake CLEAN MERGE failed",
            context={
                "run_id": run_id,
                "table": SNOWFLAKE_CLEAN_TABLE,
                "mode": "merge",
                "rows": len(df),
                "error": str(exc),
            },
        )

        raise BondProcessingError(
            f"Snowflake CLEAN MERGE failed: {exc}"
        ) from exc

# =============================================================
# SNOWFLAKE CLEAN TABLE — UNIFIED WRITE DISPATCHER
# =============================================================

def write_to_snowflake_clean(
    df:         pd.DataFrame,
    mode:       str,
    start_date: pd.Timestamp,
    end_date:   pd.Timestamp,
    run_id:     str,
) -> int:
    """
    Route to the correct Snowflake CLEAN write strategy based on mode.

    - "replay" / "backfill": DELETE window then INSERT (atomic transaction).
    - "incremental":         incremental <- MERGE latest-state rows

    Wraps the strategy call in retry_with_backoff. Raises on exhaustion.

    Args:
        df:         Fully processed DataFrame (without run_mode).
        mode:       "incremental" | "backfill" | "replay".
        start_date: Window start used for replay/backfill DELETE.
        end_date:   Window end used for replay/backfill DELETE.
        run_id:     Pipeline run identifier for alert context.

    Returns:
        Number of rows written.
    """
    if mode in ("replay", "backfill"):
        strategy_name = f"Snowflake CLEAN delete+insert ({mode})"

        def do_write():
            _snowflake_clean_delete_insert(df, start_date, end_date, run_id)
            return len(df)

    else:  # incremental

        strategy_name = "Snowflake CLEAN MERGE (incremental)"

        def do_write():
            return _snowflake_clean_merge(
                df,
                run_id,
            )

    return retry_with_backoff(
        do_write,
        retries=3,
        critical_name=strategy_name,
        run_id=run_id,
    )



# =============================================================
# POSTGRES SERVING LAYER (consistency-first: RAISES on failure)
# =============================================================
def write_to_postgres(
    df: pd.DataFrame,
    mode: str,
    retries: int = 3,
) -> None:
    """
    Write enriched bond rows to the Postgres serving layer.

    Rules:
    - If incoming DF has >2 unique dates:
        -> trim in memory to latest 2 dates
    - If final DF has 2 unique dates:
        -> FULL REPLACE (DELETE + INSERT)
        -> NO trim needed
    - If final DF has 1 unique date:
        -> INCREMENTAL APPEND
        -> then trim table to latest 2 dates
    """

    if df.empty:
        print("  [POSTGRES] Empty DataFrame — nothing to write.")
        return

    # ─────────────────────────────────────────────
    # Prepare serving-layer DataFrame
    # ─────────────────────────────────────────────
    df_pg = drop_metadata_for_serving(df.copy())

    # ─────────────────────────────────────────────
    # Step 1: Trim in memory if >2 unique dates
    # ─────────────────────────────────────────────
    unique_dates = sorted(df_pg["date"].unique())

    if len(unique_dates) > 2:

        latest_2_dates = unique_dates[-2:]

        df_pg = df_pg[
            df_pg["date"].isin(latest_2_dates)
        ].copy()

        print(
            f"  [POSTGRES] Trimmed incoming DF "
            f"to latest 2 dates: {latest_2_dates}"
        )

    # Recalculate after trimming
    unique_dates = sorted(df_pg["date"].unique())
    unique_date_count = len(unique_dates)

    print(
        f"  [POSTGRES] Final incoming DF has "
        f"{unique_date_count} unique date(s)"
    )

    last_error = None

    for attempt in range(retries + 1):

        try:

            with get_postgre_conn() as pg_conn:

                with pg_conn.cursor() as pg_cur:

                    # ─────────────────────────────────────────────
                    # Step 2: Validate live schema
                    # ─────────────────────────────────────────────
                    pg_cur.execute(
                        """
                        SELECT column_name
                        FROM information_schema.columns
                        WHERE table_schema = 'public'
                          AND table_name = 'bond_data'
                        ORDER BY ordinal_position
                        """
                    )

                    pg_cols_order = [
                        row[0]
                        for row in pg_cur.fetchall()
                        if row[0].lower() != "id"
                    ]

                    if not pg_cols_order:

                        raise RuntimeError(
                            "Schema query returned 0 columns "
                            "for public.bond_data"
                        )

                    missing = (
                        set(pg_cols_order)
                        - set(df_pg.columns)
                    )

                    if missing:

                        raise ValueError(
                            f"DataFrame missing PG columns: "
                            f"{missing}"
                        )

                    # Exact column order
                    df_ordered = df_pg[
                        pg_cols_order
                    ].copy()

                    # ─────────────────────────────────────────────
                    # Step 3: Write strategy
                    # ─────────────────────────────────────────────
                    if unique_date_count == 2:

                        # FULL REPLACE
                        pg_cur.execute(
                            "DELETE FROM public.bond_data"
                        )

                        print(
                            "  [POSTGRES] Deleted all existing rows "
                            "(2-day refresh)"
                        )

                    elif unique_date_count == 1:

                        load_date = unique_dates[0]

                        if mode == "incremental":

                            print(
                                "  [POSTGRES] Incremental append mode "
                                "(1-day load)"
                            )

                        else:

                            # replay/backfill
                            pg_cur.execute(
                                """
                                DELETE FROM public.bond_data
                                WHERE date = %s
                                """,
                                (load_date,)
                            )

                            deleted = (
                                pg_cur.rowcount
                                if pg_cur.rowcount is not None
                                else 0
                            )

                            print(
                                f"  [POSTGRES] Deleted {deleted:,} existing rows "
                                f"for {load_date} ({mode} mode)"
                            )

                    else:

                        raise ValueError(
                            f"Unexpected unique_date_count="
                            f"{unique_date_count}"
                        )

                    # ─────────────────────────────────────────────
                    # Step 4: Bulk INSERT via COPY
                    # ─────────────────────────────────────────────
                    columns_quoted = [
                        f'"{col}"'
                        for col in pg_cols_order
                    ]

                    copy_sql = (
                        f"COPY public.bond_data "
                        f"({', '.join(columns_quoted)}) "
                        f"FROM STDIN WITH CSV"
                    )

                    buf = StringIO()

                    df_ordered.to_csv(
                        buf,
                        index=False,
                        header=False
                    )

                    buf.seek(0)

                    with pg_cur.copy(copy_sql) as copy:
                        copy.write(buf.getvalue())

                    # ─────────────────────────────────────────────
                    # Step 5: Trim ONLY for incremental loads
                    # ─────────────────────────────────────────────
                    if (
                        unique_date_count == 1
                        and mode == "incremental"
                    ):
                        pg_cur.execute(
                            """
                            DELETE FROM public.bond_data
                            WHERE date < (
                                SELECT MAX(date)
                                FROM public.bond_data
                            ) - INTERVAL '1 day'
                            """
                        )

                        trimmed = (
                            pg_cur.rowcount
                            if pg_cur.rowcount is not None
                            else 0
                        )

                        print(
                            f"  [POSTGRES] Trimmed "
                            f"{trimmed:,} old rows"
                        )

                    else:

                        print(
                            "  [POSTGRES] No trim needed "
                        )

                # Commit transaction
                pg_conn.commit()

            print(
                f"  [POSTGRES] Success — inserted "
                f"{len(df_ordered):,} rows from "
                f"{unique_date_count} unique date(s)."
            )

            return

        except Exception as e:

            last_error = e

            if attempt < retries:

                wait_time = 2 ** attempt

                print(
                    f"  [POSTGRES] Retry "
                    f"{attempt + 1}/{retries} "
                    f"after {wait_time}s: {e}"
                )

                time.sleep(wait_time)

            else:

                print(
                    f"  [POSTGRES] All "
                    f"{retries} retries exhausted: {e}"
                )

    # ─────────────────────────────────────────────
    # Final failure
    # ─────────────────────────────────────────────
    send_critical_alert(
        f"Postgres bond write failed after {retries} retries",
        context={
            "error": str(last_error),
            "rows": len(df_pg),
        },
    )

    raise BondProcessingError(
        f"Postgres write to public.bond_data "
        f"failed after {retries} retries: "
        f"{last_error}"
    )

# =============================================================
# MAIN PIPELINE  —  process_bonds  (SECOND PIPELINE)
# =============================================================

def process_bonds(
    start_date_override: str  = None,
    replay:              bool = False,
    airflow_metadata:    dict = None,
) -> str:
    """
    SECOND PIPELINE — Bond ML predictions and database writes.

    Mode detection (derived from parameters ONLY — never from data columns):
    ┌──────────────────────────────────┬──────────────┐
    │ Condition                        │ mode         │
    ├──────────────────────────────────┼──────────────┤
    │ replay = True                    │ "replay"     │
    │ start_date_override provided     │ "backfill"   │
    │ default (neither)                │ "incremental"│
    └──────────────────────────────────┴──────────────┘

    Feature loading strategy:
    ┌──────────────┬──────────────────────────────────────────────────────┐
    │ Mode         │ Source                                               │
    ├──────────────┼──────────────────────────────────────────────────────┤
    │ replay       │ Latest Layer 2 partition per date (S3 LastModified)  │
    │ backfill     │ Latest Layer 2 partition per date (S3 LastModified)  │
    │ incremental  │ Rolling 30-day serving parquet                       │
    └──────────────┴──────────────────────────────────────────────────────┘

    Snowflake writes (TWO TABLES):
    - BONDS_HISTORY (append-only): always INSERT, includes run_mode column
    - BONDS (clean):               DELETE window + INSERT (replay/backfill)
                                   INSERT only (incremental, watermark-safe)

    Postgres writes (serving layer):
    - Rules:
        * 1 unique date:  Append + trim to last 2 days
        * 2 unique dates: Replace entire table (no trim needed)
        * >2 unique dates: Trim to latest 2 days in memory, then replace
    - Never writes data just to delete it later

    Failure contract:
    - Pipeline fails if EITHER Snowflake write OR Postgres write fails.
    - Partial success is NOT allowed.
    - Model failures are soft-fail (NaN prediction columns).
    - FRED data issues surface as DataValidationError (hard fail).

    Args:
        start_date_override: "YYYY-MM-DD" string. Required for replay mode.
                             If provided without replay=True <- backfill mode.
        replay:              True <- loads latest Layer 2 partitions.
                             False + start_date_override <- backfill.
                             False + no override <- incremental.

    Returns:
        str: Status string for Airflow XCom
             ("ALREADY_UPDATED" | "NO_FEATURES_LOADED" | "SUCCESS_<n>_ROWS")

    Raises:
        BondProcessingError:   For unrecoverable database or S3 failures.
        DataValidationError:   For output data contract violations.
    """
    pipeline_start = time.time()
    run_ts         = utc_now()
    run_id         = f"proc_{run_ts.strftime('%Y%m%d_%H%M%S')}"
    today          = today_utc()
    bucket         = CONFIG["s3_bucket"]

    # ──────────────────────────────────────────
    # MODE DETECTION — single, explicit decision
    # ──────────────────────────────────────────
    if replay:
        mode = "replay"
    elif start_date_override:
        mode = "backfill"
    else:
        mode = "incremental"

    if mode == "replay" and not start_date_override:
        raise BondProcessingError("Replay mode requires start_date_override.")

    end_date = today

    print(f"\n{'=' * 66}")
    print(f"  BOND PROCESSING PIPELINE (SECOND) START")
    print(f"  run_id              : {run_id}")
    print(f"  mode                : {mode}")
    print(f"  start_date_override : {start_date_override}")
    print(f"  replay              : {replay}")
    print(f"  end_date            : {end_date}")
    print(f"{'=' * 66}\n")

    try:
        # ══════════════════════════════════════════════════════════════
        # STEP 1 — Determine processing window from mode
        # ══════════════════════════════════════════════════════════════
        print("  [STEP 1] Determining processing window...")

        if mode in ("replay", "backfill"):
            start_date = pd.Timestamp(start_date_override).date()
            print(
                f"  [{mode.upper()}] Processing window: "
                f"{start_date} <- {end_date}"
            )

        else:  # incremental
            # Watermark from Snowflake BONDS clean table
            def get_watermark():
                with get_snowflake_conn() as ctx:
                    with ctx.cursor() as cs:
                        cs.execute(
                            f'SELECT MAX("date") FROM "{SNOWFLAKE_CLEAN_TABLE}"'
                        )
                        return cs.fetchone()[0]

            last_date_sf = retry_with_backoff(
                get_watermark,
                retries=2,
                critical_name="Snowflake BONDS watermark query",
                run_id=run_id,
            )

            if last_date_sf is not None:
                last_date_sf = pd.Timestamp(last_date_sf).date()
            else:
                last_date_sf = pd.Timestamp("1970-01-01").date()

            print(f"  [INCREMENTAL] Last processed date: {last_date_sf}")
            print(f"  [INCREMENTAL] Today              : {today}")

            if last_date_sf >= today:
                print(
                    "  [INCREMENTAL] Watermark is current — no new data to process."
                )
                return "ALREADY_UPDATED"

            start_date = last_date_sf  # filter uses date > last_date_sf

        # ══════════════════════════════════════════════════════════════
        # STEP 2 — Load features (mode-aware strategy)
        # ══════════════════════════════════════════════════════════════
        print(f"\n  [STEP 2] Loading features (mode='{mode}')...")

        if mode in ("replay", "backfill"):
            # Load ONLY the latest Layer 2 partition per date.
            # Guarantees deterministic, non-contaminated feature state.
            features_df = load_bond_features_from_layer2(
                bucket     = bucket,
                start_date = start_date,
                end_date   = end_date,
                run_id     = run_id,
            )

        else:  # incremental
            # Load the 30-day rolling serving parquet and filter to new rows
            rolling_df  = load_rolling_features(bucket, run_id)
            features_df = rolling_df[
                rolling_df["date"] > last_date_sf
            ].copy().reset_index(drop=True)
            print(
                f"  [INCREMENTAL] Rows after watermark filter "
                f"(date > {last_date_sf}): {len(features_df):,}"
            )

        if features_df.empty:
            print(
                "  No feature rows in the processing window — "
                "ensure bonds_update_pipeline ran for this window."
            )
            return "NO_FEATURES_LOADED"

        print(f"  Loaded {len(features_df):,} feature rows.")

        # ══════════════════════════════════════════════════════════════
        # STEP 3 — Rename first-pipeline lineage to source_* prefix
        # ══════════════════════════════════════════════════════════════
        print("\n  [STEP 3] Renaming source lineage columns...")
        features_df = rename_source_lineage(features_df)


        # ══════════════════════════════════════════════════════════════
        # STEP 4 — Load and merge macro data
        # ══════════════════════════════════════════════════════════════
        print("\n  [STEP 4] Loading and merging macro data...")
        
        macro_df = load_macro_data(run_id)
        
        # Ensure date columns are datetime
        features_df["date"] = pd.to_datetime(features_df["date"])
        macro_df["date"] = pd.to_datetime(macro_df["date"])
        
        # Create month-year keys for merging (macro data is monthly)
        features_df["mm_yy"] = features_df["date"].dt.strftime("%m-%y")
        macro_df["mm_yy"] = macro_df["date"].dt.strftime("%m-%y")
        
        # Drop original date from macro to avoid conflicts
        macro_for_merge = macro_df.drop(columns=["date"])
        
        # Left join macro data
        features_df = features_df.merge(macro_for_merge, on="mm_yy", how="left").drop(columns=["mm_yy"])
        
        print(f"  Macro columns added: {[c for c in ['gdp', 'unrate', 'fedfunds', 'cpi'] if c in features_df.columns]}")

        # ══════════════════════════════════════════════════════════════
        # STEP 5 — Run ML predictions (soft-fail on model load failure)
        # ══════════════════════════════════════════════════════════════
        print("\n  [STEP 5] Running ML model predictions...")
        features_df = run_predictions(features_df, bucket)

        # ══════════════════════════════════════════════════════════════
        # STEP 6 — Stamp second-pipeline lineage metadata
        # ══════════════════════════════════════════════════════════════
        print("\n  [STEP 5] Stamping second-pipeline lineage metadata...")
        features_df = _add_pipeline_metadata(features_df, run_id, run_ts)

        # Convert date to Python date for DB compatibility
        features_df["date"] = pd.to_datetime(features_df["date"]).dt.date

        # Select final output columns — only those present in DataFrame
        df_to_upload = features_df[
            [c for c in CONFIG["final_cols"] if c in features_df.columns]
        ].copy()

        snowflake_rows = len(df_to_upload)
        print(f"\n  Final rows for DB writes: {snowflake_rows:,}")

        # ══════════════════════════════════════════════════════════════
        # STEP 7 — Validate output before any DB write
        # ══════════════════════════════════════════════════════════════
        market_end = get_market_end_date()
        validate_output(df_to_upload, market_end, run_id=run_id)

        # Resolve timestamp objects for Snowflake write calls
        snowflake_start = pd.Timestamp(start_date)
        snowflake_end   = pd.Timestamp(end_date)

        # ══════════════════════════════════════════════════════════════
        # STEP 8 — Write to Snowflake HISTORY (append-only, always INSERT)
        # Failure here <- pipeline FAILS.
        # ══════════════════════════════════════════════════════════════
        print(
            f"\n  [STEP 7] Writing to Snowflake HISTORY table "
            f"({SNOWFLAKE_HISTORY_TABLE})..."
        )
        write_to_snowflake_history(df_to_upload, run_mode=mode, run_id=run_id)

        # ══════════════════════════════════════════════════════════════
        # STEP 9 — Write to Snowflake CLEAN (deterministic latest state)
        # replay/backfill <- DELETE window + INSERT (atomic transaction)
        # incremental     <- MERGE latest-state rows using(bond_id, ticker, date)
        # Failure here <- pipeline FAILS.
        # ══════════════════════════════════════════════════════════════
        print(
            f"\n  [STEP 8] Writing to Snowflake CLEAN table "
            f"({SNOWFLAKE_CLEAN_TABLE}) using mode='{mode}'..."
        )
        write_to_snowflake_clean(
            df_to_upload,
            mode       = mode,
            start_date = snowflake_start,
            end_date   = snowflake_end,
            run_id     = run_id,
        )

        # ══════════════════════════════════════════════════════════════
        # STEP 10 — Write to Postgres serving layer
        # Rules based on unique dates in DataFrame:
        #   - 1 date:  Append + trim to last 2 days
        #   - 2 dates: Replace entire table (no trim needed)
        #   - >2 dates: Trim to latest 2 days in memory, then replace
        # Failure here -> pipeline FAILS (consistency-first).
        # ══════════════════════════════════════════════════════════════
        print(f"\n  [STEP 9] Writing to Postgres serving layer...")
        write_to_postgres(df_to_upload, mode=mode)

        # ══════════════════════════════════════════════════════════════
        # STEP 10 — Pipeline success
        # ══════════════════════════════════════════════════════════════
        processing_time = round(time.time() - pipeline_start, 2)

        fill_summary = (
            df_to_upload["fill_method_flag"].value_counts().to_dict()
            if "fill_method_flag" in df_to_upload.columns else {}
        )

        print(f"\n{'=' * 66}")
        print(f"  BOND PROCESSING PIPELINE SUCCESS")
        print(f"  run_id           : {run_id}")
        print(f"  mode             : {mode}")
        print(f"  rows processed   : {snowflake_rows:,}")
        print(f"  window           : {start_date} <- {end_date}")
        print(f"  duration         : {processing_time}s")
        print(f"  fill_flags       : {fill_summary}")
        print(f"  Snowflake HISTORY: OK")
        print(f"  Snowflake CLEAN  : OK")
        print(f"  Postgres         : OK")
        print(f"{'=' * 66}\n")

        # Write run summary to S3
        run_summary = {
            "pipeline_run_id": run_id,
            "pipeline_name": CONFIG["pipeline_name"],
            "run_ts": run_ts.isoformat(),
            "status": "SUCCESS",
            "mode": mode,
            "start_date": str(start_date),
            "end_date": str(end_date),
            "rows_processed": snowflake_rows,
            "processing_time_s": processing_time,
            "fill_method_flags": fill_summary,
            "source_run_ids": (
                df_to_upload["source_run_id"]
                .dropna()
                .astype(str)
                .unique()
                .tolist()
                if "source_run_id" in df_to_upload.columns
                else []
            ),
            "tables_written": [
                SNOWFLAKE_HISTORY_TABLE,
                SNOWFLAKE_CLEAN_TABLE,
                CONFIG["postgres_table"],
            ],
            "airflow": airflow_metadata,
        }
        write_run_summary(run_summary)

 
        return f"SUCCESS_{snowflake_rows}_ROWS"

    except Exception as exc:
        processing_time = round(time.time() - pipeline_start, 2)

        send_critical_alert(
            "Bond processing pipeline failed",
            context={
                "run_id":   run_id,
                "mode":     mode,
                "duration": f"{processing_time}s",
                "error":    str(exc),
            },
        )

        print(f"\n{'=' * 66}")
        print(f"  BOND PROCESSING PIPELINE FAILED")
        print(f"  run_id   : {run_id}")
        print(f"  mode     : {mode}")
        print(f"  error    : {exc}")
        print(f"  duration : {processing_time}s")
        print(f"{'=' * 66}\n")

        # Write failure summary to S3 (ONCE)
        run_summary = {
            "pipeline_run_id": run_id,
            "pipeline_name": CONFIG["pipeline_name"],
            "run_ts": run_ts.isoformat(),
            "status": "FAILED",
            "mode": mode,
            "processing_time_s": processing_time,
            "error": str(exc),
            "airflow": airflow_metadata,
        }
        write_run_summary(run_summary)

        raise