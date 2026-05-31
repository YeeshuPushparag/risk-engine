"""
bonds_update_pipeline.py
=========================
FIRST PIPELINE — Ingestion + Feature Engineering

Responsibilities
----------------
- Load static base bond universe from S3
- Generate one row per bond per NYSE market day
- Fetch FRED DGS10 (live API for incremental/backfill; raw layer for replay)
- Load DGS10 warm-up history from raw DGS10 partitions (all modes)
- Build DGS10 rolling features (DGS10_ma, dgs10_anom, fill_method_flag)
- Merge macro data (GDP, UNRATE, FEDFUNDS, CPI)
- Write Layer 2 feature partitions (one parquet per date per run_id)
- Maintain 30-day rolling serving parquet
- Save run metadata for observability
- NO ML predictions
- NO Snowflake writes
- NO Postgres writes

S3 storage layout
-----------------
RAW DGS10 (single source of truth, immutable, append-only, partitioned by date):
    s3://<bucket>/historical-bonds/raw/dgs10/
        year=Y/month=MM/day=DD/run_id=<id>/data.parquet

    One file per market day per run_id.
    Replay always reads the LATEST file for each date (by S3 LastModified).
    No run_id lookup needed.  No separate dgs10_history layer.
    Warm-up rows for rolling windows are reconstructed directly from these
    partitions by reading dates before the processing window start.

LAYER 2 FEATURES (versioned per run_id, partitioned by date):
    s3://<bucket>/historical-bonds/features/
        year=Y/month=MM/day=DD/run_id=<id>/data.parquet
    s3://<bucket>/historical-bonds/features/latest.json

ROLLING SERVING LAYER (mutable, last 30 market days):
    s3://<bucket>/historical-bonds/rolling/bonds_features_30d.parquet

RUN METADATA (observability only — NOT used for replay resolution):
    s3://<bucket>/historical-bonds/run_metadata/<run_id>.json

REMOVED (replaced by raw/dgs10):
    historical-bonds/raw/fred/           <- GONE
    historical-bonds/raw/dgs10_history/  <- GONE

Mode detection
--------------
Derived exclusively from function parameters — never from data columns.
    replay_from_raw=True          -> mode = "replay"
    start_date_override provided  -> mode = "backfill"
    default                       -> mode = "incremental"

Mode behaviour
--------------
incremental:
    - Watermark = max(date) from rolling parquet (or today-30d if absent)
    - Fetch fresh FRED DGS10 and generate rows from watermark+1 -> today
    - Write one raw DGS10 partition per market day under current run_id
    - Load warm-up from raw DGS10 partitions before the window
    - Write Layer 2 partitions + update rolling parquet

backfill:
    - Use start_date_override as window start
    - Fetch fresh FRED DGS10 for the full window
    - Write one raw DGS10 partition per market day under current run_id
    - Load warm-up from raw DGS10 partitions before the window
    - Write Layer 2 partitions + update rolling parquet (safe deduplication)

replay:
    - Load the LATEST raw DGS10 partition for each requested market day
      (latest = highest S3 LastModified — same strategy as commodity pipeline)
    - NO live FRED API calls — fully deterministic
    - Load warm-up from raw DGS10 partitions before the window (same layer)
    - Write a NEW Layer 2 partition under a new run_id
    - Update rolling parquet (safe deduplication)
    - Original raw partitions are never modified
    - Missing date partitions are skipped with a warning, NOT a pipeline abort
      (mirrors commodity pipeline behaviour exactly)

DGS10 warm-up
-------------
Rolling(20) for DGS10_ma requires 20 prior rows.  The pipeline:
  1. Scans raw/dgs10 partitions for dates before start_date.
  2. Loads the latest file per date, takes the last N rows.
  3. Prepends them as explicit rows BEFORE the window spine.
  4. Computes rolling(20) over the combined (warmup + window) series.
  5. Strips warm-up rows before writing (build_dgs10_series does this).
  This works for ALL modes — incremental, backfill, and replay.

Bug fixes in this revision (vs prior version)
---------------------------------------------
FIX 1  build_dgs10_series: warmup rows now prepended as actual spine rows
       before rolling calculation so they genuinely contribute to DGS10_ma
       and dgs10_anom.  Previously the spine was built from window dates only
       and warmup values entered only via a left-merge, meaning rolling(20)
       saw at most 1 "prior" row (the merged-in value) instead of N rows.

FIX 2  Replay no longer aborts when some date partitions are missing.
       Mirrors commodity pipeline: load all available dates, log missing
       dates as warnings, continue processing on whatever data exists.

FIX 3  combined_fred deduplication in build_dgs10_series changed to
       keep="last" so new window observations win over warmup rows on
       identical dates (e.g. today's date present in both).

FIX 4  merge_macro_data changed from how="inner" to how="left" so bond
       rows are never silently dropped due to macro coverage gaps.
       Missing macro columns are filled with NaN and logged.

FIX 5  load_dgs10_warmup lookback multiplier increased from 3x to 4x
       (80 calendar days) to reliably cover any holiday cluster.

FIX 6  _read_raw_dgs10_layer now deduplicates by date after concatenating
       all partition frames, preventing duplicate date rows from flowing
       into build_dgs10_series.

FIX 7  update_rolling_layer: date column normalised to datetime64 on both
       sides before concat+dedup so type mismatches do not silently produce
       duplicate rows.

FIX 8  STARTED metadata sentinel no longer crashes when start_str is empty;
       the sentinel is now written after start_str is resolved.

FIX 9  Market-holiday skip no longer fires send_critical_alert (INFO level
       only, matching commodity pipeline behaviour).

Airflow notes
-------------
- Entry point: update_bonds_pipeline(start_date_override, replay_from_raw)
- Recommended: retries=2, retry_delay=timedelta(minutes=5)
- max_active_runs=1 (S3 rolling write is not concurrent-safe)
- DAG order: run_bonds_update >> run_bonds_processing
"""

import os
import uuid
import json
import time
import pandas as pd
import numpy as np
import boto3
import requests
from botocore.exceptions import ClientError
from datetime import datetime, timedelta, timezone, date as date_type
from io import BytesIO
from fredapi import Fred
import pendulum
import pandas_market_calendars as mcal

# =============================================================
# CONFIG  —  single source of truth.  No magic numbers in code.
# =============================================================

CONFIG: dict = {
    # FRED fetch
    "max_retries":          3,
    "retry_backoff_s":      5,         # seconds; multiplied by attempt number

    # DGS10 rolling warm-up
    # FIX 5: lookback multiplier increased from 3 to 4 (80 cal-days)
    # to reliably cover any holiday cluster when hunting for 20 market days.
    "dgs10_history_rows":   20,        # rows loaded to warm rolling(20)
    "dgs10_warmup_lookback_multiplier": 4,   # calendar-day multiplier
    "dgs10_rolling_window": 20,        # window for DGS10_ma

    # Rolling serving layer
    "window_days":          30,        # market days retained in rolling parquet

    # S3 bucket
    "s3_bucket": "yeeshu-bond-bucket",

    # S3 reference file paths (at bucket root)
    "base_bond_key": "synthetic_bond.csv",
    "macro_key":     "macro_data.csv",

    # S3 partitioned path prefixes
    # SINGLE raw DGS10 layer — replaces raw/fred + raw/dgs10_history
    "raw_dgs10_prefix":    "historical-bonds/raw/dgs10/",
    "features_prefix":     "historical-bonds/features/",
    "rolling_key":         "historical-bonds/rolling/bonds_features_30d.parquet",
    "run_metadata_prefix": "historical-bonds/run_metadata/",

    # Data quality thresholds
    "max_null_pct":       0.02,
    "max_negative_pct":   0.01,

    # Anomaly thresholds
    "dgs10_zscore_limit":  4.0,
    "spread_zscore_limit": 4.0,

    # Lineage — bump transformation when feature engineering logic changes
    "pipeline_name":  "bonds_update_pipeline",
    "data_source":    "fred+synthetic+macro",
    "input_source":   "synthetic_bond+fred+macro",
    "transformation": "bond_features_v1",
}

# Volatility mapping by credit rating — unchanged from original pipeline
VOL_MAP: dict[str, int] = {
    "AAA":  2,  "AA+":  3, "AA":  4, "A+":  5,
    "A":    6,  "A-":   8,
    "BBB+": 10, "BBB": 12, "BBB-": 15,
    "BB+":  20, "BB":  25, "B":   35, "CCC": 50,
}

# Layer 2 output columns — contract between first and second pipelines.
# run_mode is written here for replay traceability. The second pipeline
# will rename pipeline_* to source_* and add its own lineage columns.
LAYER2_OUTPUT_COLS: list[str] = [
    "bond_id", "ticker", "sector", "industry", "credit_rating",
    "coupon_rate", "issue_date", "maturity_date", "maturity_years",
    "date", "vol",
    "benchmark_yield", "corporate_yield", "credit_spread",
    "bond_price", "yield_to_maturity", "implied_hazard",
    "implied_pd_annual", "implied_pd_multi_year", "implied_rating",
    "market_synthetic_score",
    "issue_size",
    "units_issued",
    "units_outstanding",
    "market_value",
    "outstanding_pct",
    "DGS10", "DGS10_ma", "dgs10_anom", "fill_method_flag",
    "gdp", "unrate", "fedfunds", "cpi",
    # First pipeline lineage (second pipeline renames these to source_*)
    "pipeline_name", "pipeline_run_id", "data_source",
    "input_source", "transformation", "record_created_at",
    "run_mode",   # informational; second pipeline knows which mode produced this
]


# =============================================================
# CUSTOM EXCEPTIONS
# =============================================================

class BondPipelineError(Exception):
    """Raised for unrecoverable first-pipeline failures."""


class DataValidationError(Exception):
    """Raised when data validation checks fail."""


# =============================================================
# PRODUCTION-GRADE ALERTING (CRITICAL ONLY)
# =============================================================

def send_critical_alert(message: str, context: dict = None) -> None:
    """
    Send CRITICAL alert to Slack only for unrecoverable pipeline failures.

    Triggers for:
    - FRED fetch failure (after all retries)
    - S3 write failure for Layer 2 partitions or rolling parquet
    - Data validation failure
    - Any unhandled pipeline exception

    Does NOT trigger for:
    - Individual retry attempts (only on final exhaustion)
    - Rolling parquet not existing on first run (expected initial state)
    - Market holiday skips  (FIX 9: INFO-level only, not CRITICAL)
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


def send_info_alert(message: str, context: dict = None) -> None:
    """
    Send INFO-level alert (stdout only; no Slack page).
    Used for expected operational events like market-holiday skips
    and missing-partition warnings during replay.
    Matches commodity pipeline INFO-level alerting philosophy.
    """
    payload = {
        "level":     "INFO",
        "pipeline":  CONFIG["pipeline_name"],
        "message":   message,
        "context":   context or {},
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    print(f"[INFO] {message} | context={payload['context']}")


def send_warning_alert(message: str, context: dict = None) -> None:
    """
    Send WARNING-level alert (stdout + optional Slack).
    Used for recoverable issues: missing replay partitions, macro merge
    failures, partial warmup history, etc.
    """
    payload = {
        "level":     "WARNING",
        "pipeline":  CONFIG["pipeline_name"],
        "message":   message,
        "context":   context or {},
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    print(f"[WARNING] {message} | context={payload['context']}")

    webhook = os.getenv("SLACK_WEBHOOK_URL")
    if webhook:
        try:
            text = (
                f"*[WARNING]* {CONFIG['pipeline_name']}\n"
                f"{message}\n"
                f"context: {json.dumps(payload['context'], default=str)}"
            )
            requests.post(webhook, json={"text": text}, timeout=3)
        except Exception as e:
            print(f"[ALERT ERROR] Slack notification failed: {e}")


# =============================================================
# MARKET DATE HELPERS
# =============================================================

NYSE_CALENDAR = mcal.get_calendar("NYSE")


def get_valid_market_days(start_date, end_date) -> set:
    """Return the set of NYSE trading dates (as date objects) in [start_date, end_date]."""
    return set(
        pd.to_datetime(
            NYSE_CALENDAR.valid_days(
                start_date=start_date,
                end_date=end_date,
            )
        ).date
    )


def is_market_holiday(check_date: date_type) -> bool:
    """
    Return True if check_date is NOT a valid NYSE trading day.
    Covers weekends, national holidays, and exchange holidays.
    """
    schedule = NYSE_CALENDAR.schedule(
        start_date=check_date,
        end_date=check_date,
    )
    return schedule.empty


def get_market_end_date() -> date_type:
    """
    Determine the appropriate end date for market data processing.

    - Before 4:00 PM ET  -> use yesterday
    - At or after 4:00 PM ET -> use today

    This prevents pulling incomplete intraday data before market close.
    """
    eastern      = pendulum.timezone("America/New_York")
    now_et       = datetime.now(eastern)
    current_date = now_et.date()
    market_close = datetime.strptime("16:00", "%H:%M").time()

    if now_et.time() < market_close:
        end_date = current_date - timedelta(days=1)
        print(f"  [MARKET] Before 4:00 PM ET — using yesterday as end_date: {end_date}")
    else:
        end_date = current_date
        print(f"  [MARKET] After 4:00 PM ET — using today as end_date: {end_date}")

    return end_date


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
# TIMEZONE HELPERS
# =============================================================

def utc_now() -> datetime:
    """Current datetime as a tz-aware UTC object."""
    return datetime.now(timezone.utc)


def today_utc() -> date_type:
    """Today's date in UTC."""
    return utc_now().date()


# =============================================================
# S3 HELPERS  —  basic I/O
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
    """Read a CSV from S3. Raises BondPipelineError on failure."""
    try:
        print(f"  [S3 READ CSV]     s3://{bucket}/{key}")
        obj = get_s3().get_object(Bucket=bucket, Key=key)
        return pd.read_csv(BytesIO(obj["Body"].read()))
    except ClientError as exc:
        raise BondPipelineError(f"Failed to read s3://{bucket}/{key}: {exc}") from exc


def read_parquet_from_s3(bucket: str, key: str) -> pd.DataFrame:
    """Read a Parquet from S3. Raises BondPipelineError on failure."""
    try:
        print(f"  [S3 READ PARQUET] s3://{bucket}/{key}")
        obj = get_s3().get_object(Bucket=bucket, Key=key)
        return pd.read_parquet(BytesIO(obj["Body"].read()))
    except ClientError as exc:
        raise BondPipelineError(f"Failed to read s3://{bucket}/{key}: {exc}") from exc


def write_parquet_to_s3(df: pd.DataFrame, bucket: str, key: str) -> None:
    """Write a DataFrame as Parquet to S3. Raises BondPipelineError on failure."""
    try:
        print(f"  [S3 WRITE PARQUET] s3://{bucket}/{key}  rows={len(df):,}")
        buf = BytesIO()
        df.to_parquet(buf, index=False)
        buf.seek(0)
        get_s3().put_object(Bucket=bucket, Key=key, Body=buf.getvalue())
    except ClientError as exc:
        raise BondPipelineError(f"Failed to write s3://{bucket}/{key}: {exc}") from exc


# =============================================================
# S3 PARTITION PATH HELPERS  —  deterministic key construction
# =============================================================

def _raw_dgs10_key(target_date, run_id: str) -> str:
    """
    Construct the S3 key for a raw DGS10 partition.

    Partitioned by market date, versioned by run_id.
    One file per market day per pipeline run.

    Example:
        historical-bonds/raw/dgs10/year=2024/month=01/day=15/run_id=<id>/data.parquet
    """
    d = pd.Timestamp(target_date)
    return (
        f"{CONFIG['raw_dgs10_prefix']}"
        f"year={d.year}/month={d.month:02d}/day={d.day:02d}/"
        f"run_id={run_id}/data.parquet"
    )


def _feature_partition_key(target_date, run_id: str) -> str:
    """
    Construct the S3 key for a Layer 2 feature partition.

    One file per date per run_id.

    Example:
        historical-bonds/features/year=2024/month=01/day=15/run_id=<id>/data.parquet
    """
    d = pd.Timestamp(target_date)
    return (
        f"{CONFIG['features_prefix']}"
        f"year={d.year}/month={d.month:02d}/day={d.day:02d}/"
        f"run_id={run_id}/data.parquet"
    )


# =============================================================
# RAW DGS10 LAYER HELPERS
# =============================================================

def _get_latest_dgs10_keys(
    bucket:     str,
    start_date: date_type,
    end_date:   date_type,
) -> dict[date_type, str]:
    """
    Scan raw/dgs10 partitions and return the most recent S3 key for each
    market day in [start_date, end_date].

    Selection rule: highest S3 LastModified timestamp within each date
    prefix — identical to the commodity pipeline's get_latest_raw_keys().
    No run_id lookup required.

    Args:
        bucket:     S3 bucket name.
        start_date: Inclusive window start.
        end_date:   Inclusive window end.

    Returns:
        Dict mapping date -> S3 key (latest file for that date).
        Dates with no partition at all are omitted (not an error).
    """
    s3_client = get_s3()
    paginator = s3_client.get_paginator("list_objects_v2")
    result:   dict[date_type, str] = {}

    valid_days = get_valid_market_days(start_date, end_date)

    for market_day in valid_days:
        date_prefix = (
            f"{CONFIG['raw_dgs10_prefix']}"
            f"year={market_day.year}/"
            f"month={market_day.month:02d}/"
            f"day={market_day.day:02d}/"
        )

        latest_key:      str | None      = None
        latest_modified: datetime | None = None

        for page in paginator.paginate(Bucket=bucket, Prefix=date_prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if not key.endswith("data.parquet"):
                    continue
                if latest_modified is None or obj["LastModified"] > latest_modified:
                    latest_key      = key
                    latest_modified = obj["LastModified"]

        if latest_key:
            result[market_day] = latest_key

    return result


def _read_raw_dgs10_layer(
    bucket:     str,
    start_date: date_type,
    end_date:   date_type,
) -> pd.DataFrame:
    """
    Load the latest raw DGS10 file for every market day in [start_date, end_date].

    Used by:
    - Replay mode    : loads the window to be reprocessed.
    - All modes      : loads warm-up history (dates before the window start).

    Missing date partitions are logged as INFO and skipped — NOT an error.
    This mirrors commodity pipeline behaviour exactly.

    FIX 6: After concatenating all partition frames, deduplicates by date
    (keep="last") to prevent duplicate rows when a single FRED response
    covered multiple days and was stored in one partition file.

    Returns a DataFrame with columns [date, DGS10], sorted ascending.
    Returns an empty DataFrame if no partitions exist.
    """
    valid_days  = get_valid_market_days(start_date, end_date)
    latest_keys = _get_latest_dgs10_keys(bucket, start_date, end_date)

    # Log any market days that have no partition (not an error)
    missing_days = sorted(d for d in valid_days if d not in latest_keys)
    if missing_days:
        send_info_alert(
            f"[RAW DGS10] {len(missing_days)} market day(s) have no raw partition "
            f"in [{start_date}, {end_date}] — skipped.",
            context={"missing_days": [str(d) for d in missing_days[:10]]},
        )

    if not latest_keys:
        print(
            f"  [RAW DGS10] No partitions found for window "
            f"[{start_date}, {end_date}]."
        )
        return pd.DataFrame(columns=["date", "DGS10"])

    frames: list[pd.DataFrame] = []
    for market_day, key in sorted(latest_keys.items()):
        print(f"  [RAW DGS10] Loading latest for {market_day}: {key}")
        part = read_parquet_from_s3(bucket, key)
        frames.append(part)

    combined          = pd.concat(frames, ignore_index=True)
    combined["date"]  = pd.to_datetime(combined["date"])
    combined["DGS10"] = pd.to_numeric(combined["DGS10"], errors="coerce")

    # FIX 6: deduplicate by date — keep last row per date so that if a
    # partition file stored a multi-day FRED response, only one row per
    # date survives into warmup and window computations.
    combined = (
        combined
        .sort_values("date")
        .drop_duplicates(subset=["date"], keep="last")
        .reset_index(drop=True)
    )

    print(
        f"  [RAW DGS10] {len(latest_keys)} partition(s) loaded, "
        f"{len(combined):,} unique date rows."
    )
    return combined


def _write_raw_dgs10_partitions(
    dgs10_df: pd.DataFrame,
    bucket:   str,
    run_id:   str,
) -> dict[str, str]:
    """
    Write one raw DGS10 parquet per market day under the current run_id.

    Called after every successful live FRED fetch (incremental and backfill).
    Creates the immutable audit trail that replay reads later.

    Args:
        dgs10_df: DataFrame with columns [date, DGS10].
                  May contain non-market-day rows; they are written as-is
                  since FRED occasionally reports on non-NYSE days (e.g.
                  Columbus Day). Replay reads by market-day prefix so these
                  rows are harmlessly stored but never loaded by replay.
        bucket:   S3 bucket name.
        run_id:   Current pipeline run identifier.

    Returns:
        Dict mapping date_str -> S3 key for every partition written.
    """
    df          = dgs10_df.copy()
    df["date"]  = pd.to_datetime(df["date"])
    df["_date"] = df["date"].dt.date
    partition_log: dict[str, str] = {}

    for market_day, group in df.groupby("_date"):
        key = _raw_dgs10_key(market_day, run_id)
        write_parquet_to_s3(
            group.drop(columns=["_date"], errors="ignore"),
            bucket,
            key,
        )
        partition_log[str(market_day)] = key

    print(
        f"  [RAW DGS10] Wrote {len(partition_log)} raw partition(s) "
        f"for run_id={run_id}."
    )
    return partition_log


# =============================================================
# RUN METADATA  —  observability only (NOT used for replay)
# =============================================================

def _save_run_metadata(
    bucket:            str,
    run_id:            str,
    mode:              str,
    start_date,
    end_date:          date_type,
    nrows:             int,
    run_ts:            datetime,
    airflow_metadata:  dict  = None,
    status:            str   = "SUCCESS",
    processing_time_s: float = 0,
    partition_count:   int   = 0,
    rolling_updated:   bool  = False,
    error:             str   = "",
) -> None:
    """
    Persist run metadata to S3 for observability and audit trail.

    This file is written on every run regardless of outcome.  It is
    intentionally NOT used for replay data resolution — replay reads
    raw DGS10 partitions directly using LastModified ordering, so there
    is no dependency on this metadata at replay time.

    Non-fatal on failure — never blocks the pipeline.

    start_date accepts None (used for the STARTED sentinel written before
    the window is fully resolved) or any date-like value.
    """
    key = f"{CONFIG['run_metadata_prefix']}{run_id}.json"

    payload = {
        "run_id":            run_id,
        "status":            status,
        "mode":              mode,
        "start_date":        str(start_date) if start_date is not None else None,
        "end_date":          str(end_date),
        "rows":              nrows,
        "processing_time_s": processing_time_s,
        "partition_count":   partition_count,
        "rolling_updated":   rolling_updated,
        "timestamp":         run_ts.isoformat(),
        "error":             error,
        "airflow":           airflow_metadata,
    }

    try:
        get_s3().put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(payload, indent=2, default=str).encode("utf-8"),
        )
        print(f"  [METADATA] {status} metadata written: s3://{bucket}/{key}")
    except Exception as exc:
        print(f"  [METADATA][WARN] Could not save metadata: {exc}")


# =============================================================
# STAGE 1 — load_base_bond_data
# =============================================================

def load_base_bond_data(bucket: str) -> pd.DataFrame:
    """
    Load the static synthetic bond universe from S3.
    This is the base dataset expanded to one row per market day.
    """
    base = read_csv_from_s3(bucket, CONFIG["base_bond_key"])

    if base.empty:
        raise DataValidationError(
            "Base bond file is empty — cannot generate daily rows."
        )

    print(f"  [STAGE 1] Loaded {len(base):,} base bond records.")
    return base


# =============================================================
# STAGE 2 — generate_daily_rows
# =============================================================

def generate_daily_rows(
    base:       pd.DataFrame,
    start_date: str,
    end_date:   str,
) -> pd.DataFrame:
    """
    Expand the base bond universe to one row per bond per NYSE market day.
    Excludes weekends AND NYSE holidays.
    """
    valid_days = get_valid_market_days(start_date, end_date)

    if not valid_days:
        print(f"  [STAGE 2] No market days in range [{start_date}, {end_date}] — returning empty.")
        return pd.DataFrame()

    dates  = pd.DatetimeIndex(sorted(valid_days))
    n_days = len(dates)

    daily         = base.loc[base.index.repeat(n_days)].copy()
    daily["date"] = np.tile(dates, len(base))
    daily["vol"]  = daily["credit_rating"].map(VOL_MAP).fillna(10) / 100

    print(f"  [STAGE 2] Generated {len(daily):,} daily rows across {n_days} market days.")
    return daily


# =============================================================
# STAGE 3 — load_dgs10_warmup
# =============================================================

def load_dgs10_warmup(
    bucket:     str,
    start_date: date_type,
) -> pd.DataFrame:
    """
    Load the last N DGS10 rows strictly before start_date for rolling warm-up.

    Source: raw/dgs10 partitions — the SINGLE source of truth for ALL modes.

    Strategy:
      1. Look back up to (N * MULTIPLIER) calendar days before start_date.
         FIX 5: multiplier is now 4 (was 3) — 80 calendar days to find
         20 market days, safely covering any extended holiday cluster.
      2. Load the latest partition for each found date.
      3. Return the last N rows sorted ascending.

    Empty DataFrame is valid (rolling computation uses min_periods=1).

    Args:
        bucket:     S3 bucket name.
        start_date: Processing window start — warm-up rows must be BEFORE this.

    Returns:
        DataFrame with columns [date, DGS10] sorted ascending.
    """
    n            = CONFIG["dgs10_history_rows"]
    multiplier   = CONFIG["dgs10_warmup_lookback_multiplier"]
    lookback     = n * multiplier
    warmup_end   = pd.Timestamp(start_date).date() - timedelta(days=1)
    warmup_start = warmup_end - timedelta(days=lookback)

    warmup_df = _read_raw_dgs10_layer(bucket, warmup_start, warmup_end)

    if warmup_df.empty:
        send_warning_alert(
            "[STAGE 3] No DGS10 warm-up history found in raw layer — "
            "proceeding without warm-up. DGS10_ma will use min_periods=1.",
            context={"start_date": str(start_date), "lookback_days": lookback},
        )
        return pd.DataFrame(columns=["date", "DGS10"])

    # Keep only [date, DGS10], deduplicate, take last N rows.
    # _read_raw_dgs10_layer already deduplicates by date (FIX 6) but we
    # sort+tail here for safety.
    hist = (
        warmup_df[["date", "DGS10"]]
        .drop_duplicates(subset=["date"])
        .sort_values("date")
        .tail(n)
        .reset_index(drop=True)
    )

    print(
        f"  [STAGE 3] Loaded {len(hist)} DGS10 warm-up row(s) from raw layer "
        f"({hist['date'].min().date()} -> {hist['date'].max().date()})."
    )
    return hist


# =============================================================
# STAGE 4 — fetch_dgs10_from_fred  (incremental / backfill only)
# =============================================================

def fetch_dgs10_from_fred(
    start_date: str,
    end_date:   str,
    run_id:     str,
    bucket:     str,
) -> pd.DataFrame:
    """
    Fetch DGS10 (10-year Treasury yield) from the live FRED API.

    Called ONLY for incremental and backfill modes.
    Replay mode reads directly from the raw DGS10 layer — this function
    is never called during replay.

    After a successful fetch:
      - Writes one raw DGS10 parquet per market day under run_id.
      - Returns the full fetched DataFrame for the window.

    If FRED returns empty data for the range, writes empty partitions
    so replay has a consistent partition structure.

    Raises BondPipelineError if the FRED fetch fails after all retries.
    """
    fred_api_key = os.getenv("FRED_API_KEY")
    if not fred_api_key:
        send_critical_alert(
            "FRED_API_KEY environment variable not set",
            context={"run_id": run_id},
        )
        raise BondPipelineError("FRED_API_KEY not configured.")

    last_exc = None

    for attempt in range(1, CONFIG["max_retries"] + 1):
        try:
            fred   = Fred(api_key=fred_api_key)
            series = fred.get_series(
                "DGS10",
                observation_start=start_date,
                observation_end=end_date,
            )

            if series is None or series.empty:
                # No data for this range — write empty partition per market day
                # so replay has a consistent (empty) partition to load.
                empty_df   = pd.DataFrame(columns=["date", "DGS10"])
                valid_days = get_valid_market_days(start_date, end_date)
                for market_day in valid_days:
                    write_parquet_to_s3(
                        empty_df,
                        bucket,
                        _raw_dgs10_key(market_day, run_id),
                    )
                print(
                    f"  [STAGE 4] FRED returned empty series — "
                    f"wrote {len(valid_days)} empty raw partition(s)."
                )
                return empty_df

            df          = (
                series.to_frame("DGS10")
                .reset_index()
                .rename(columns={"index": "date"})
            )
            df["date"]  = pd.to_datetime(df["date"])
            df["DGS10"] = pd.to_numeric(df["DGS10"], errors="coerce")

            # Write one raw partition per market day
            written = _write_raw_dgs10_partitions(df, bucket, run_id)

            print(
                f"  [STAGE 4] Fetched {len(df)} DGS10 observation(s) from FRED "
                f"and wrote {len(written)} raw partition(s)."
            )
            return df

        except Exception as exc:
            last_exc = exc
            print(
                f"  [STAGE 4] FRED fetch attempt {attempt}/{CONFIG['max_retries']} "
                f"failed: {exc}"
            )
            if attempt < CONFIG["max_retries"]:
                time.sleep(CONFIG["retry_backoff_s"] * attempt)

    send_critical_alert(
        f"FRED DGS10 fetch failed after {CONFIG['max_retries']} attempts",
        context={
            "run_id":     run_id,
            "start_date": start_date,
            "end_date":   end_date,
            "error":      str(last_exc),
        },
    )
    raise BondPipelineError(
        f"FRED DGS10 fetch failed after {CONFIG['max_retries']} attempts: {last_exc}"
    )


# =============================================================
# STAGE 5 — build_dgs10_series
# =============================================================

def build_dgs10_series(
    history_df:  pd.DataFrame,
    new_fred_df: pd.DataFrame,
    start_date:  str,
    end_date:    str,
) -> pd.DataFrame:
    """
    Combine warm-up history with new DGS10 data, align to NYSE market days,
    apply forward/backward fill with fill_method_flag tracking, and compute
    DGS10_ma (rolling 20-day mean) and dgs10_anom.

    Uses NYSE calendar to exclude market holidays as well as weekends,
    matching the bond row generation logic exactly.

    fill_method_flag values:
      "REAL"            — FRED observed value for that market day
      "FORWARD_FILLED"  — no FRED value; filled forward from prior observation
      "BACKWARD_FILLED" — no FRED value and no prior; filled backward

    Returns DataFrame with columns:
        date, DGS10, fill_method_flag, DGS10_ma, dgs10_anom
    (new-date rows only — warm-up history rows are stripped after rolling)

    -----------------------------------------------------------------------
    FIX 1 — WARMUP ROWS NOW ACTUALLY CONTRIBUTE TO ROLLING CALCULATIONS
    -----------------------------------------------------------------------
    Previous bug: the spine was built from valid_market_days (window dates
    only).  history_df values entered only via a left-merge, giving rolling(20)
    at most 1 "prior" row per window date instead of N warmup rows.  This
    caused DGS10_ma to be wrong for the first ~20 days of every window and
    made incremental != replay for those rows.

    Fix: build a combined spine that includes BOTH the warmup rows (dates
    strictly before start_date) AND the window rows (dates >= start_date),
    then compute rolling over the full combined spine, then strip the warmup
    rows before returning.  This is identical to the commodity pipeline's
    buffer_days approach in generate_commodity_features().

    FIX 3 — DEDUPLICATION KEEPS LAST (NEW WINDOW WINS OVER WARMUP)
    -----------------------------------------------------------------------
    If the same date appears in both history_df and new_fred_df (e.g. today
    is at the boundary), keep="last" ensures the new observation wins.
    -----------------------------------------------------------------------
    """
    valid_window_days = get_valid_market_days(start_date, end_date)

    if not valid_window_days:
        print(f"  [STAGE 5] No market days in range [{start_date}, {end_date}] — returning empty.")
        return pd.DataFrame(columns=["date", "DGS10", "fill_method_flag", "DGS10_ma", "dgs10_anom"])

    start_ts = pd.Timestamp(start_date)

    # ── Step 1: Build complete observation set (warmup + window) ────────
    # FIX 3: keep="last" so new window observations beat warmup rows on
    # duplicate dates (boundary case).
    combined_obs = pd.DataFrame(columns=["date", "DGS10"])

    if not history_df.empty and "DGS10" in history_df.columns:
        combined_obs = pd.concat(
            [combined_obs, history_df[["date", "DGS10"]]],
            ignore_index=True,
        )

    if not new_fred_df.empty and "DGS10" in new_fred_df.columns:
        combined_obs = pd.concat(
            [combined_obs, new_fred_df[["date", "DGS10"]]],
            ignore_index=True,
        )

    combined_obs["date"]  = pd.to_datetime(combined_obs["date"])
    combined_obs["DGS10"] = pd.to_numeric(combined_obs["DGS10"], errors="coerce")

    # FIX 3: keep="last" — new window data wins over warmup on same date
    combined_obs = (
        combined_obs
        .sort_values("date")
        .drop_duplicates(subset=["date"], keep="last")
        .reset_index(drop=True)
    )

    # ── Step 2: Build the spine covering warmup dates + window dates ─────
    # Warmup dates: all dates in history_df that are before start_ts
    warmup_dates: list[pd.Timestamp] = []
    if not history_df.empty:
        history_df_copy = history_df.copy()
        history_df_copy["date"] = pd.to_datetime(history_df_copy["date"])
        warmup_dates = sorted(
            history_df_copy.loc[history_df_copy["date"] < start_ts, "date"].tolist()
        )

    # Window dates: the valid NYSE market days for the requested window
    window_dates = sorted(pd.Timestamp(d) for d in valid_window_days)

    # Combined spine: warmup first, then window, no duplicates
    all_spine_dates = sorted(set(warmup_dates) | set(window_dates))
    spine = pd.DataFrame({"date": pd.DatetimeIndex(all_spine_dates)})

    # ── Step 3: Merge observations onto the full spine ───────────────────
    spine = spine.merge(combined_obs, on="date", how="left")

    # ── Step 4: Fill method tracking ────────────────────────────────────
    real_mask                    = spine["DGS10"].notna()
    spine["fill_method_flag"]    = np.where(real_mask, "REAL", pd.NA)

    # Forward fill
    spine["_dgs10_before_ffill"] = spine["DGS10"].copy()
    spine["DGS10"]               = spine["DGS10"].ffill()
    ffill_mask = spine["_dgs10_before_ffill"].isna() & spine["DGS10"].notna()
    spine.loc[ffill_mask, "fill_method_flag"] = "FORWARD_FILLED"

    # Backward fill
    spine["_dgs10_before_bfill"] = spine["DGS10"].copy()
    spine["DGS10"]               = spine["DGS10"].bfill()
    bfill_mask = spine["_dgs10_before_bfill"].isna() & spine["DGS10"].notna()
    spine.loc[bfill_mask, "fill_method_flag"] = "BACKWARD_FILLED"

    # Final fill flag cleanup
    spine["fill_method_flag"] = spine["fill_method_flag"].fillna("BACKWARD_FILLED")
    spine = spine.drop(columns=["_dgs10_before_ffill", "_dgs10_before_bfill"])

    # ── Step 5: Ultimate fallback — entire spine still null ──────────────
    if spine["DGS10"].isna().all():
        if not history_df.empty and history_df["DGS10"].notna().any():
            last_known = history_df["DGS10"].dropna().iloc[-1]
            spine["DGS10"]            = last_known
            spine["fill_method_flag"] = "BACKWARD_FILLED"
            print(f"  [STAGE 5] No DGS10 in window — using last known value {last_known}")
        else:
            spine["DGS10"]            = 0.0
            spine["fill_method_flag"] = "BACKWARD_FILLED"
            print("  [STAGE 5] No DGS10 in history — using 0 as fallback")

    # ── Step 6: Rolling features over the FULL spine (warmup + window) ───
    # FIX 1: rolling(20) is now computed over the complete spine which
    # includes the warmup rows, so the first window date benefits from up
    # to N prior rows.  This makes DGS10_ma identical across incremental,
    # backfill, and replay for the same date range.
    spine["DGS10_ma"]   = spine["DGS10"].rolling(
        CONFIG["dgs10_rolling_window"], min_periods=1
    ).mean()
    spine["dgs10_anom"] = spine["DGS10"] - spine["DGS10_ma"]

    # ── Step 7: Strip warmup rows — return only the requested window ─────
    spine = spine[spine["date"] >= start_ts].reset_index(drop=True)

    real_count  = int((spine["fill_method_flag"] == "REAL").sum())
    ffill_count = int((spine["fill_method_flag"] == "FORWARD_FILLED").sum())
    bfill_count = int((spine["fill_method_flag"] == "BACKWARD_FILLED").sum())

    print(
        f"  [STAGE 5] DGS10 series built — "
        f"REAL={real_count}, FORWARD_FILLED={ffill_count}, "
        f"BACKWARD_FILLED={bfill_count}  ({len(spine)} total rows in window)."
    )
    return spine


# =============================================================
# STAGE 6 — validate_data
# =============================================================

def validate_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Hard data-quality validation.

    Removes invalid rows. Critical threshold breaches raise DataValidationError.

    Returns:
        Cleaned DataFrame (bad rows are discarded, not returned).
    """
    original_rows = len(df)

    required_cols = ["bond_id", "ticker", "date", "bond_price", "DGS10"]

    missing_cols = [c for c in required_cols if c not in df.columns]
    if missing_cols:
        raise DataValidationError(f"Missing required columns: {missing_cols}")

    # Null filtering
    null_mask = df[required_cols].isnull().any(axis=1)
    null_rows = int(null_mask.sum())
    null_pct  = null_rows / len(df) if len(df) else 0

    # Invalid numeric filtering
    invalid_mask = (
        (df["coupon_rate"]    < 0) |
        (df["maturity_years"] < 0) |
        (df["bond_price"]     <= 0) |
        (df["DGS10"]          <= 0)
    )
    invalid_rows = int(invalid_mask.sum())
    invalid_pct  = invalid_rows / len(df) if len(df) else 0

    # Threshold enforcement
    if null_pct > CONFIG["max_null_pct"]:
        raise DataValidationError(f"Null percentage too high: {null_pct:.2%}")

    if invalid_pct > CONFIG["max_negative_pct"]:
        raise DataValidationError(f"Invalid numeric percentage too high: {invalid_pct:.2%}")

    # Remove bad rows
    bad_mask = null_mask | invalid_mask
    cleaned  = df[~bad_mask].copy().reset_index(drop=True)

    print(
        f"  [VALIDATE] "
        f"rows_before={original_rows:,} "
        f"rows_after={len(cleaned):,} "
        f"rows_dropped={bad_mask.sum():,}"
    )
    return cleaned


# =============================================================
# STAGE 7 — detect_anomalies
# =============================================================

def detect_anomalies(df: pd.DataFrame) -> pd.DataFrame:
    """
    Soft statistical anomaly detection.
    Flags suspicious values but does NOT fail the pipeline.
    """
    df = df.copy()

    # DGS10 anomaly
    dgs10_std = df["DGS10"].std()
    if dgs10_std and not pd.isna(dgs10_std):
        dgs10_z = (df["DGS10"] - df["DGS10"].mean()) / dgs10_std
        df["dgs10_anomaly_flag"] = (dgs10_z.abs() > CONFIG["dgs10_zscore_limit"])
    else:
        df["dgs10_anomaly_flag"] = False

    # Credit spread anomaly
    spread_std = df["credit_spread"].std() if "credit_spread" in df.columns else None
    if spread_std and not pd.isna(spread_std):
        spread_z = (df["credit_spread"] - df["credit_spread"].mean()) / spread_std
        df["spread_anomaly_flag"] = (spread_z.abs() > CONFIG["spread_zscore_limit"])
    else:
        df["spread_anomaly_flag"] = False

    dgs10_flags  = int(df["dgs10_anomaly_flag"].sum())
    spread_flags = int(df["spread_anomaly_flag"].sum())

    print(
        f"  [ANOMALIES] "
        f"dgs10_flags={dgs10_flags:,} "
        f"spread_flags={spread_flags:,}"
    )
    return df


# =============================================================
# STAGE 8 — merge_macro_data
# =============================================================

def merge_macro_data(daily: pd.DataFrame, bucket: str) -> pd.DataFrame:
    """
    Merge macro features (GDP, UNRATE, FEDFUNDS, CPI) by month-year key.

    FIX 4: Changed from how="inner" to how="left" so bond rows are NEVER
    silently dropped due to macro coverage gaps.  Rows without a matching
    macro month receive NaN for macro columns and a warning is issued.
    This matches the commodity pipeline's non-fatal merge pattern.
    """
    try:
        macro          = read_csv_from_s3(bucket, CONFIG["macro_key"])
        macro          = macro.copy()
        macro["date"]  = pd.to_datetime(macro["date"])
        macro["mm_yy"] = macro["date"].dt.strftime("%m-%y")

        daily          = daily.copy()
        daily["mm_yy"] = pd.to_datetime(daily["date"]).dt.strftime("%m-%y")

        # FIX 4: how="left" instead of how="inner" — never drop bond rows
        merged = daily.merge(
            macro.drop(columns=["date"]),
            on="mm_yy",
            how="left",
        ).drop(columns=["mm_yy"])

        # Warn if macro was missing for any bond rows
        macro_cols = [c for c in ["gdp", "unrate", "fedfunds", "cpi"] if c in merged.columns]
        if macro_cols:
            null_macro = merged[macro_cols].isnull().any(axis=1).sum()
            if null_macro > 0:
                send_warning_alert(
                    f"[STAGE 8] {null_macro:,} bond row(s) have no matching macro month — "
                    "macro columns are NaN for those rows.",
                    context={"null_macro_rows": int(null_macro)},
                )

        print(f"  [STAGE 8] Macro merge (left): {len(daily):,} bond rows -> {len(merged):,} rows.")
        return merged

    except Exception as exc:
        send_warning_alert(
            f"[STAGE 8] Macro merge failed — proceeding without macro: {exc}"
        )
        return daily.drop(columns=["mm_yy"], errors="ignore")


# =============================================================
# LAYER 2 — write_layer2_features
# =============================================================

def write_layer2_features(
    df:     pd.DataFrame,
    bucket: str,
    run_id: str,
) -> dict:
    """
    Write one Parquet per calendar date to the Layer 2 feature store.
    Also updates features/latest.json pointer.
    """
    df_copy           = df.copy()
    df_copy["_date_"] = pd.to_datetime(df_copy["date"]).dt.date
    partition_log     = {}

    for date_val, group in df_copy.groupby("_date_"):
        clean_group = group.drop(columns=["_date_"])
        key         = _feature_partition_key(date_val, run_id)
        write_parquet_to_s3(clean_group, bucket, key)
        partition_log[str(date_val)] = {"key": key, "rows": len(clean_group)}

    # Update latest.json pointer
    latest_key     = f"{CONFIG['features_prefix']}latest.json"
    latest_payload = {
        "run_id":          run_id,
        "updated_at":      utc_now().isoformat(),
        "feature_version": "v1",
        "schema_version":  "1.0",
        "dates_written":   list(partition_log.keys()),
    }
    try:
        get_s3().put_object(
            Bucket=bucket,
            Key=latest_key,
            Body=json.dumps(latest_payload, default=str, indent=2),
        )
        print(f"  [LAYER 2] Updated latest.json: {latest_key}")
    except Exception as exc:
        print(f"  [LAYER 2][WARN] Could not write latest.json: {exc}")

    print(
        f"  [LAYER 2] Written {len(partition_log)} date partition(s) "
        f"for run_id={run_id}."
    )
    return partition_log


# =============================================================
# ROLLING LAYER — load + update
# =============================================================

def load_rolling_layer(bucket: str) -> pd.DataFrame | None:
    """
    Load the current 30-day rolling serving parquet.

    Returns None if the file does not exist (expected on first ever run).
    Raises BondPipelineError if the key exists but cannot be read.
    """
    key = CONFIG["rolling_key"]
    if not s3_key_exists(bucket, key):
        print(
            f"  [ROLLING] Rolling parquet not found: s3://{bucket}/{key} "
            "(expected on first run)."
        )
        return None

    df         = read_parquet_from_s3(bucket, key)
    # FIX 7: normalise date to datetime64 immediately on load so downstream
    # comparisons are always type-consistent.
    df["date"] = pd.to_datetime(df["date"])
    print(
        f"  [ROLLING] Loaded {len(df):,} rows from rolling parquet "
        f"(date range: {df['date'].min().date()} -> {df['date'].max().date()})."
    )
    return df


def update_rolling_layer(
    new_features:   pd.DataFrame,
    old_rolling_df: pd.DataFrame | None,
    bucket:         str,
    window_days:    int = 30,
) -> pd.DataFrame:
    """
    Merge new features into the rolling serving parquet.

    Merge semantics:
    - New rows take precedence over old rows for the same (bond_id, date).
    - Keeps only the last N NYSE market days.
    - Safe for replay and backfill — dedup is key-based, not positional.

    FIX 7: Both sides are normalised to datetime64 before concat+dedup to
    prevent silent type-mismatch duplicates when one side has Python date
    objects and the other has datetime64.
    """
    # FIX 7: normalise date columns on both sides before any operation
    new_df = new_features.copy()
    new_df["date"] = pd.to_datetime(new_df["date"])

    if old_rolling_df is not None and not old_rolling_df.empty:
        old_df         = old_rolling_df.copy()
        old_df["date"] = pd.to_datetime(old_df["date"])
        combined       = pd.concat([old_df, new_df], ignore_index=True)
    else:
        combined = new_df

    combined["date"] = pd.to_datetime(combined["date"])

    # Deduplicate — new run takes precedence (new rows were appended last)
    combined = (
        combined
        .sort_values(["bond_id", "date"])
        .drop_duplicates(subset=["bond_id", "date"], keep="last")
        .reset_index(drop=True)
    )

    # Keep last N market days (NYSE trading days only)
    unique_dates      = combined["date"].drop_duplicates().sort_values()
    market_day_dates  = [
        d for d in unique_dates
        if not is_market_holiday(d.date() if hasattr(d, "date") else d)
    ]
    last_market_dates = pd.Series(market_day_dates).tail(window_days)
    rolling           = combined[combined["date"].isin(last_market_dates)]
    rolling           = rolling.sort_values(["bond_id", "date"]).reset_index(drop=True)

    write_parquet_to_s3(rolling, bucket, CONFIG["rolling_key"])
    print(
        f"  [ROLLING] Updated rolling layer: {len(rolling):,} rows "
        f"(market_days_retained={len(last_market_dates)})."
    )
    return rolling


# =============================================================
# LINEAGE — first pipeline metadata stamp
# =============================================================

def _add_pipeline_metadata(
    df:     pd.DataFrame,
    run_id: str,
    run_ts: datetime,
    mode:   str,
) -> pd.DataFrame:
    """
    Stamp every output row with first-pipeline lineage.

    These columns are renamed to source_* by the second pipeline so
    both pipeline lineage sets coexist in the final Snowflake/Postgres output.
    run_mode is informational for Layer 2 consumers.
    """
    df = df.copy()
    df["pipeline_name"]     = CONFIG["pipeline_name"]
    df["pipeline_run_id"]   = run_id
    df["data_source"]       = CONFIG["data_source"]
    df["input_source"]      = CONFIG["input_source"]
    df["transformation"]    = CONFIG["transformation"]
    df["record_created_at"] = run_ts.isoformat()
    df["run_mode"]          = mode
    return df


# =============================================================
# MAIN PIPELINE  —  update_bonds_pipeline  (FIRST PIPELINE)
# =============================================================

def update_bonds_pipeline(
    start_date_override: str  = None,
    replay_from_raw:     bool = False,
    airflow_metadata:    dict = None,
) -> str:
    """
    FIRST PIPELINE — Bond ingestion and feature engineering.

    Mode detection (derived from parameters ONLY — never from data columns):
    ┌──────────────────────────────────┬──────────────┐
    │ Condition                        │ mode         │
    ├──────────────────────────────────┼──────────────┤
    │ replay_from_raw = True           │ "replay"     │
    │ start_date_override provided     │ "backfill"   │
    │ default (neither)                │ "incremental"│
    └──────────────────────────────────┴──────────────┘

    Raw DGS10 layer:
        historical-bonds/raw/dgs10/year=Y/month=MM/day=DD/run_id=<id>/data.parquet
        One file per market day per run_id.
        Incremental + backfill: write after every successful FRED fetch.
        Replay: read latest file per date (by S3 LastModified) — no run_id lookup.
        Warm-up: read dates before start_date from the same layer, all modes.

    Replay missing-partition behaviour (FIX 2):
        If some market-day partitions are absent in the raw DGS10 layer,
        replay loads all available dates, logs a WARNING for each missing
        date, and continues processing.  It does NOT abort.  This mirrors
        the commodity pipeline exactly.  Replay only aborts if ZERO
        partitions are found for the entire requested window.

    Outputs (S3 only — NO Snowflake or Postgres writes):
    - Raw DGS10 partitions     (incremental / backfill only)
    - Layer 2 feature partitions (one per date per run_id)
    - Updated 30-day rolling serving parquet
    - Run metadata JSON (observability only)

    Args:
        start_date_override: "YYYY-MM-DD" string. Required for replay mode.
                             If provided without replay_from_raw=True -> backfill.
        replay_from_raw:     True -> replay mode. Reads raw DGS10 partitions
                             instead of calling FRED — deterministic.
        airflow_metadata:    Airflow execution context passed for observability.

    Returns:
        str: Status string for Airflow XCom
             ("SKIPPED_MARKET_HOLIDAY" | "ALREADY_CURRENT" | "NO_NEW_ROWS" |
              "SUCCESS_<n>_ROWS")

    Raises:
        BondPipelineError:   For unrecoverable pipeline failures.
        DataValidationError: For output data contract violations.
    """
    pipeline_start = time.time()
    run_ts         = utc_now()
    today          = today_utc()
    bucket         = CONFIG["s3_bucket"]

    # ─────────────────────────────────────────────────────────
    # MODE DETECTION — single, explicit decision
    # ─────────────────────────────────────────────────────────
    if replay_from_raw:
        mode = "replay"
    elif start_date_override:
        mode = "backfill"
    else:
        mode = "incremental"

    if mode == "replay" and not start_date_override:
        raise BondPipelineError("Replay mode requires start_date_override.")

    end_date = get_market_end_date()

    # FIX 9: Market holiday skip uses INFO-level alert, not CRITICAL.
    # A closed market is a normal operational event, not a pipeline failure.
    if is_market_holiday(end_date):
        run_id = f"skipped_{run_ts.strftime('%Y%m%d_%H%M%S')}"
        msg    = f"NYSE closed on {end_date} (holiday/weekend). Skipping pipeline."
        print(f"  [MARKET CLOSED] {msg}")
        send_info_alert(msg, context={"run_id": run_id, "date": str(end_date)})
        _save_run_metadata(
            bucket=bucket, run_id=run_id, mode="skipped",
            start_date=end_date, end_date=end_date, nrows=0,
            run_ts=run_ts, airflow_metadata=airflow_metadata,
            status="SKIPPED", error="market_holiday",
        )
        return "SKIPPED_MARKET_HOLIDAY"

    print(f"\n{'=' * 66}")
    print(f"  BOND UPDATE PIPELINE (FIRST) START")
    print(f"  mode                : {mode}")
    print(f"  start_date_override : {start_date_override}")
    print(f"  replay_from_raw     : {replay_from_raw}")
    print(f"  end_date            : {end_date}")
    print(f"{'=' * 66}\n")

    # run_id initialised here so it is always available in the except block.
    run_id    = f"run_{run_ts.strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
    start_str = ""   # resolved in STEP 2; guarded in except block

    try:
        # ══════════════════════════════════════════════════════════════
        # STEP 1 — Load rolling layer (watermark source for incremental)
        # Non-critical: None is acceptable on first ever run.
        # ══════════════════════════════════════════════════════════════
        print("  [STEP 1] Loading rolling layer...")
        old_rolling_df = load_rolling_layer(bucket)

        # ══════════════════════════════════════════════════════════════
        # STEP 2 — Determine processing window
        # ══════════════════════════════════════════════════════════════
        print("\n  [STEP 2] Determining processing window...")

        if mode in ("replay", "backfill"):
            start_date = pd.Timestamp(start_date_override).date()
            print(
                f"  [{mode.upper()}] Processing window: "
                f"{start_date} -> {end_date}"
            )

        else:  # incremental
            if old_rolling_df is not None and not old_rolling_df.empty:
                last_date = pd.to_datetime(old_rolling_df["date"]).dt.date.max()
            else:
                last_date = today - timedelta(days=CONFIG["window_days"])
                print(
                    f"  [INCREMENTAL] No rolling parquet found — "
                    f"defaulting start to {last_date} ({CONFIG['window_days']} days ago)."
                )

            print(f"  [INCREMENTAL] Last processed date: {last_date}")
            print(f"  [INCREMENTAL] Today              : {today}")

            if last_date >= today:
                print("  [INCREMENTAL] Rolling parquet is current — nothing to process.")
                return "ALREADY_CURRENT"

            start_date = last_date

        # String forms used for FRED API, generate_daily_rows, build_dgs10_series
        start_str = (
            pd.Timestamp(start_date + timedelta(days=1)).strftime("%Y-%m-%d")
            if mode == "incremental"
            else pd.Timestamp(start_date).strftime("%Y-%m-%d")
        )
        end_str = end_date.strftime("%Y-%m-%d")

        print(f"  output_run_id      : {run_id}")
        print(f"  processing window  : {start_str} -> {end_str}")

        # FIX 8: Write STARTED sentinel AFTER start_str is resolved so
        # pd.Timestamp(start_str).date() is always valid (non-empty string).
        _save_run_metadata(
            bucket=bucket, run_id=run_id, mode=mode,
            start_date=pd.Timestamp(start_str).date(), end_date=end_date,
            nrows=0, run_ts=run_ts, airflow_metadata=airflow_metadata,
            status="STARTED",
        )

        # ══════════════════════════════════════════════════════════════
        # STEP 3 — Load base bond data from S3
        # ══════════════════════════════════════════════════════════════
        print("\n  [STEP 3] Loading base bond data from S3...")
        base = load_base_bond_data(bucket)

        # ══════════════════════════════════════════════════════════════
        # STEP 4 — Generate daily bond rows (market day expansion)
        # ══════════════════════════════════════════════════════════════
        print("\n  [STEP 4] Generating daily bond rows...")
        daily = generate_daily_rows(base, start_str, end_str)

        if daily.empty:
            print("  No market days in processing window — pipeline complete.")
            return "NO_NEW_ROWS"

        print(f"  Generated {len(daily):,} rows for window [{start_str}, {end_str}]")

        # ══════════════════════════════════════════════════════════════
        # STEP 5 — Fetch / load DGS10 observations for the window
        #
        # Incremental / Backfill:
        #   Call live FRED API, then write one raw partition per market day.
        #
        # Replay (FIX 2):
        #   Read the latest raw DGS10 partition for each market day directly
        #   from the raw/dgs10 layer — no FRED call, fully deterministic.
        #   If some date partitions are missing, log a WARNING and continue
        #   with whatever data is available.  Only abort if ZERO partitions
        #   exist for the entire requested window.
        # ══════════════════════════════════════════════════════════════
        if mode == "replay":
            print("\n  [STEP 5] Loading DGS10 from raw layer (replay mode)...")
            replay_start_date = pd.Timestamp(start_str).date()
            dgs10_new = _read_raw_dgs10_layer(bucket, replay_start_date, end_date)

            # FIX 2: Only abort if NO partitions at all were found for the
            # entire window.  Partial availability (some dates missing) is
            # a WARNING, not a fatal error — mirrors commodity pipeline.
            if dgs10_new.empty:
                raise BondPipelineError(
                    f"Replay aborted — no raw DGS10 partitions found for "
                    f"the entire window [{start_str}, {end_str}]. "
                    "Ensure at least one incremental/backfill run has been "
                    "executed for this date range first."
                )

            # Check for partially missing dates and warn (non-fatal)
            valid_days_in_window = get_valid_market_days(start_str, end_str)
            loaded_dates = set(dgs10_new["date"].dt.date.tolist())
            missing_window_dates = sorted(
                d for d in valid_days_in_window if d not in loaded_dates
            )
            if missing_window_dates:
                send_warning_alert(
                    f"[STEP 5] Replay: {len(missing_window_dates)} market day(s) "
                    f"have no raw DGS10 partition — those dates will have "
                    "FORWARD_FILLED or BACKWARD_FILLED DGS10 values.",
                    context={
                        "missing_count": len(missing_window_dates),
                        "missing_dates": [str(d) for d in missing_window_dates[:10]],
                    },
                )

            print(
                f"  [STEP 5] Loaded {len(dgs10_new):,} DGS10 row(s) "
                f"from raw layer for replay."
            )
        else:
            print("\n  [STEP 5] Fetching DGS10 from FRED...")
            dgs10_new = fetch_dgs10_from_fred(
                start_date=start_str,
                end_date=end_str,
                run_id=run_id,
                bucket=bucket,
            )

        # ══════════════════════════════════════════════════════════════
        # STEP 6 — Load DGS10 warm-up history from raw layer
        #
        # All modes use the SAME raw/dgs10 layer as the source.
        # Warm-up rows are the latest raw partitions strictly before
        # start_str.  Empty is valid — rolling uses min_periods=1.
        # ══════════════════════════════════════════════════════════════
        print("\n  [STEP 6] Loading DGS10 warm-up history from raw layer...")
        dgs10_history = load_dgs10_warmup(
            bucket=bucket,
            start_date=pd.Timestamp(start_str).date(),
        )

        # ══════════════════════════════════════════════════════════════
        # STEP 7 — Build DGS10 series with fill flags + rolling features
        #
        # FIX 1 (in build_dgs10_series): warmup rows are now prepended as
        # actual spine rows so rolling(20) genuinely sees N prior values.
        # ══════════════════════════════════════════════════════════════
        print("\n  [STEP 7] Building DGS10 series with fill method flags...")
        dgs10_series = build_dgs10_series(
            history_df  = dgs10_history,
            new_fred_df = dgs10_new,
            start_date  = start_str,
            end_date    = end_str,
        )

        # Compute derived bond columns
        daily["market_value"] = daily["bond_price"] * daily["units_outstanding"]
        daily["outstanding_pct"] = np.where(
            daily["units_issued"] > 0,
            (daily["units_outstanding"] / daily["units_issued"]) * 100,
            np.nan,
        )

        # Normalise daily["date"] to datetime64 for the merge
        daily["date"] = pd.to_datetime(daily["date"])

        # Merge DGS10 enrichment into daily bond rows
        daily = daily.merge(dgs10_series, on="date", how="left")

        # ══════════════════════════════════════════════════════════════
        # STEP 8 — Merge macro data
        # FIX 4: left join — never drops bond rows on macro coverage gap
        # ══════════════════════════════════════════════════════════════
        print("\n  [STEP 8] Merging macro data...")
        daily = merge_macro_data(daily, bucket)

        if daily.empty:
            raise DataValidationError(
                "DataFrame is empty after macro merge — "
                "check macro_data.csv date coverage."
            )

        # ══════════════════════════════════════════════════════════════
        # STEP 9 — Stamp first-pipeline lineage metadata
        # ══════════════════════════════════════════════════════════════
        print("\n  [STEP 9] Stamping first-pipeline lineage metadata...")
        daily = _add_pipeline_metadata(daily, run_id, run_ts, mode)

        # Convert date to Python date for consistent partition logic
        daily["date"] = pd.to_datetime(daily["date"]).dt.date

        # Select output columns — only those present in the DataFrame
        df_output = daily[
            [c for c in LAYER2_OUTPUT_COLS if c in daily.columns]
        ].copy()

        # ══════════════════════════════════════════════════════════════
        # DATA VALIDATION
        # ══════════════════════════════════════════════════════════════
        df_output = validate_data(df_output)

        # ══════════════════════════════════════════════════════════════
        # ANOMALY DETECTION
        # ══════════════════════════════════════════════════════════════
        df_output = detect_anomalies(df_output)

        output_rows = len(df_output)
        print(f"\n  Output rows: {output_rows:,}")

        # ══════════════════════════════════════════════════════════════
        # STEP 10 — Write Layer 2 feature partitions
        # One parquet per date per run_id.
        # Raises BondPipelineError on S3 write failure.
        # ══════════════════════════════════════════════════════════════
        print(f"\n  [STEP 10] Writing Layer 2 feature partitions...")
        partition_log = retry_with_backoff(
            lambda: write_layer2_features(df_output, bucket, run_id),
            retries=3,
            critical_name="Layer 2 feature partition write",
            run_id=run_id,
        )

        # ══════════════════════════════════════════════════════════════
        # STEP 11 — Update 30-day rolling serving parquet
        # Safe deduplication ensures replay/backfill does not corrupt it.
        # ══════════════════════════════════════════════════════════════
        print("\n  [STEP 11] Updating rolling serving layer...")
        df_for_rolling         = df_output.copy()
        df_for_rolling["date"] = pd.to_datetime(df_for_rolling["date"])

        retry_with_backoff(
            lambda: update_rolling_layer(
                new_features   = df_for_rolling,
                old_rolling_df = old_rolling_df,
                bucket         = bucket,
                window_days    = CONFIG["window_days"],
            ),
            retries=3,
            critical_name="Rolling layer S3 write",
            run_id=run_id,
        )

        # ══════════════════════════════════════════════════════════════
        # STEP 12 — Save SUCCESS run metadata
        # ══════════════════════════════════════════════════════════════
        processing_time = round(time.time() - pipeline_start, 2)

        _save_run_metadata(
            bucket=bucket,
            run_id=run_id,
            mode=mode,
            start_date=pd.Timestamp(start_str).date(),
            end_date=end_date,
            nrows=output_rows,
            run_ts=run_ts,
            airflow_metadata=airflow_metadata,
            status="SUCCESS",
            processing_time_s=processing_time,
            partition_count=len(partition_log),
            rolling_updated=True,
        )

        # ══════════════════════════════════════════════════════════════
        # STEP 13 — Pipeline success summary
        # ══════════════════════════════════════════════════════════════
        fill_summary = (
            df_output["fill_method_flag"].value_counts().to_dict()
            if "fill_method_flag" in df_output.columns else {}
        )

        print(f"\n{'=' * 66}")
        print(f"  BOND UPDATE PIPELINE SUCCESS")
        print(f"  run_id             : {run_id}")
        print(f"  mode               : {mode}")
        print(f"  rows produced      : {output_rows:,}")
        print(f"  date partitions    : {len(partition_log)}")
        print(f"  window             : {start_str} -> {end_str}")
        print(f"  duration           : {processing_time}s")
        print(f"  fill_flags         : {fill_summary}")
        print(f"  Layer 2            : OK")
        print(f"  Rolling parquet    : OK")
        print(f"{'=' * 66}\n")

        return f"SUCCESS_{output_rows}_ROWS"

    except Exception as exc:
        processing_time = round(time.time() - pipeline_start, 2)

        send_critical_alert(
            "Bond update pipeline failed with unhandled exception",
            context={
                "run_id":   run_id,
                "mode":     mode,
                "duration": f"{processing_time}s",
                "error":    str(exc),
            },
        )

        print(f"\n{'=' * 66}")
        print(f"  BOND UPDATE PIPELINE FAILED")
        print(f"  run_id   : {run_id}")
        print(f"  mode     : {mode}")
        print(f"  error    : {exc}")
        print(f"  duration : {processing_time}s")
        print(f"{'=' * 66}\n")

        # Best-effort FAILED metadata overwrite
        # start_str may be "" if failure happened before STEP 2 completed
        _save_run_metadata(
            bucket=bucket,
            run_id=run_id,
            mode=mode,
            start_date=pd.Timestamp(start_str).date() if start_str else None,
            end_date=end_date,
            nrows=0,
            run_ts=run_ts,
            airflow_metadata=airflow_metadata,
            status="FAILED",
            processing_time_s=processing_time,
            error=str(exc),
        )

        raise