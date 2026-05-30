"""
bonds_update_pipeline.py
=========================
FIRST PIPELINE — Ingestion + Feature Engineering

Responsibilities
----------------
- Load static base bond universe from S3
- Generate one row per bond per business day
- Fetch FRED DGS10 (live API, or load S3 raw snapshot for replay)
- Load DGS10 warm-up history (rolling parquet, or S3 raw snapshot for replay)
- Build DGS10 rolling features (DGS10_ma, dgs10_anom, fill_method_flag)
- Merge macro data (GDP, UNRATE, FEDFUNDS, CPI)
- Write Layer 2 feature partitions (one parquet per date per run_id)
- Maintain 30-day rolling serving parquet
- Save run metadata for replay resolution
- NO ML predictions
- NO Snowflake writes
- NO Postgres writes

S3 storage layout
-----------------
RAW SNAPSHOTS (for deterministic replay):
    s3://<bucket>/historical-bonds/raw/fred/
        year=Y/month=MM/day=DD/run_id=<id>/data.parquet

    s3://<bucket>/historical-bonds/raw/dgs10_history/
        year=Y/month=MM/day=DD/run_id=<id>/data.parquet

LAYER 2 FEATURES (versioned per run_id, partitioned by date):
    s3://<bucket>/historical-bonds/features/
        year=Y/month=MM/day=DD/run_id=<id>/data.parquet

ROLLING SERVING LAYER (mutable, last 30 calendar days):
    s3://<bucket>/historical-bonds/rolling/bonds_features_30d.parquet

RUN METADATA (for replay run_id resolution by start_date):
    s3://<bucket>/historical-bonds/run_metadata/<run_id>.json

Mode detection
--------------
Derived exclusively from function parameters — never from data columns.
    replay=True                   <- mode = "replay"
    start_date_override provided  <- mode = "backfill"
    default                       <- mode = "incremental"

Mode behaviour
--------------
incremental:
    - Watermark = max(date) from rolling parquet (or today-30d if absent)
    - Fetch fresh FRED DGS10 and generate rows from watermark+1 <- today
    - Save raw FRED snapshot + DGS10 history snapshot
    - Write Layer 2 partitions + update rolling parquet

backfill:
    - Use start_date_override as window start
    - Fetch fresh FRED DGS10 and generate rows for the full window
    - Save raw FRED snapshot + DGS10 history snapshot
    - Write Layer 2 partitions + update rolling parquet (safe deduplication)

replay:
    - Load raw FRED snapshot + DGS10 history snapshot from S3 by run_id
    - Rebuild features deterministically — no live API calls
    - Write a NEW Layer 2 partition under a new run_id
    - Update rolling parquet (safe deduplication)
    - Original run's snapshots are never modified

DGS10 warm-up
-------------
Rolling(20) for DGS10_ma requires 20 prior rows. The pipeline:
  1. Loads the last 20 DGS10 rows from the rolling parquet (before start_date).
  2. Prepends them to the new FRED observations.
  3. Computes rolling(20) over the combined series.
  4. Strips warm-up rows before writing (build_dgs10_series does this).

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
    "dgs10_history_rows":   20,        # rows loaded to warm rolling(20)
    "dgs10_rolling_window": 20,        # window for DGS10_ma

    # Rolling serving layer
    "window_days":          30,        # calendar days retained in rolling parquet

    # S3 bucket
    "s3_bucket": "yeeshu-bond-bucket",

    # S3 reference file paths (at bucket root)
    "base_bond_key": "synthetic_bond.csv",
    "macro_key":     "macro_data.csv",

    # S3 partitioned path prefixes
    "raw_fred_prefix":     "historical-bonds/raw/fred/",
    "raw_dgs10_prefix":    "historical-bonds/raw/dgs10_history/",
    "features_prefix":     "historical-bonds/features/",
    "rolling_key":         "historical-bonds/rolling/bonds_features_30d.parquet",
    "run_metadata_prefix": "historical-bonds/run_metadata/",


    # Data quality thresholds
    "max_null_pct":       0.02,
    "max_negative_pct":   0.01,

    # Anomaly thresholds
    "dgs10_zscore_limit": 4.0,
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
# MARKET DATE HELPERS
# =============================================================


NYSE_CALENDAR = mcal.get_calendar("NYSE")


def get_valid_market_days(start_date, end_date):

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
    Returns True if the given date is NOT a valid NYSE trading day.
    Covers:
    - weekends
    - national holidays
    - exchange holidays
    """


    schedule = NYSE_CALENDAR.schedule(
        start_date=check_date,
        end_date=check_date
    )

    return schedule.empty

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

def _raw_fred_key(start_date, run_id: str) -> str:
    """
    Construct the S3 key for a FRED DGS10 raw snapshot.
    Partitioned by start_date so replay can locate it by window start.

    Example:
        historical-bonds/raw/fred/year=2024/month=01/day=15/run_id=<id>/data.parquet
    """
    d = pd.Timestamp(start_date)
    return (
        f"{CONFIG['raw_fred_prefix']}"
        f"year={d.year}/month={d.month:02d}/day={d.day:02d}/"
        f"run_id={run_id}/data.parquet"
    )


def _raw_dgs10_key(start_date, run_id: str) -> str:
    """
    Construct the S3 key for a DGS10 warm-up history raw snapshot.
    Partitioned by start_date so replay can locate the exact history rows.

    Example:
        historical-bonds/raw/dgs10_history/year=2024/month=01/day=15/run_id=<id>/data.parquet
    """
    d = pd.Timestamp(start_date)
    return (
        f"{CONFIG['raw_dgs10_prefix']}"
        f"year={d.year}/month={d.month:02d}/day={d.day:02d}/"
        f"run_id={run_id}/data.parquet"
    )


def _feature_partition_key(date, run_id: str) -> str:
    """
    Construct the S3 key for a Layer 2 feature partition.
    One file per date per run_id.

    Example:
        historical-bonds/features/year=2024/month=01/day=15/run_id=<id>/data.parquet
    """
    d = pd.Timestamp(date)
    return (
        f"{CONFIG['features_prefix']}"
        f"year={d.year}/month={d.month:02d}/day={d.day:02d}/"
        f"run_id={run_id}/data.parquet"
    )


# =============================================================
# RUN METADATA  —  persistence for replay run_id resolution
# =============================================================

def _save_run_metadata(
    bucket: str,
    run_id: str,
    mode: str,
    start_date: date_type,
    end_date: date_type,
    nrows: int,
    run_ts: datetime,
    airflow_metadata: dict = None,
    status: str = "SUCCESS",
    processing_time_s: float = 0,
    partition_count: int = 0,
    rolling_updated: bool = False,
    error: str = "",
) -> None:
    """
    Persist run metadata to S3 for observability and audit trail.
    
    NOT used for replay resolution - raw snapshots are located by run_id
    passed explicitly or resolved from the original run's metadata.
    
    Non-fatal on failure — never blocks the pipeline.
    """
    key = f"{CONFIG['run_metadata_prefix']}{run_id}.json"
    
    payload = {
        "run_id": run_id,
        "status": status,
        "mode": mode,
        "start_date": str(start_date),
        "end_date": str(end_date),
        "rows": nrows,
        "processing_time_s": processing_time_s,
        "partition_count": partition_count,
        "rolling_updated": rolling_updated,
        "timestamp": run_ts.isoformat(),
        "error": error,
        "airflow": airflow_metadata,
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

def _resolve_run_id_for_date(bucket: str, target_date: date_type) -> str:
    """Find the most recent run_id that wrote this specific date."""
    prefix = CONFIG["run_metadata_prefix"]
    latest_run = None
    latest_ts = None
    
    try:
        paginator = get_s3().get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
        
        for page in pages:
            for obj in page.get("Contents", []):
                try:
                    raw = get_s3().get_object(Bucket=bucket, Key=obj["Key"])["Body"].read()
                    data = json.loads(raw)
                    
                    start = pd.Timestamp(data["start_date"]).date()
                    end = pd.Timestamp(data["end_date"]).date()
                    
                    if start <= target_date <= end and data["status"] == "SUCCESS":
                        ts = pd.Timestamp(data["timestamp"])
                        if latest_ts is None or ts > latest_ts:
                            latest_ts = ts
                            latest_run = data["run_id"]
                except Exception:
                    continue
        
        if not latest_run:
            raise BondPipelineError(f"No run found for date {target_date}")
        
        return latest_run
        
    except ClientError as exc:
        raise BondPipelineError(f"Failed to list metadata: {exc}") from exc

# =============================================================
# STAGE 1 — load_base_bond_data  (UNCHANGED)
# =============================================================

def load_base_bond_data(bucket: str) -> pd.DataFrame:
    """
    Load the static synthetic bond universe from S3.
    This is the base dataset expanded to one row per business day.
    Unchanged from original pipeline.
    """
    base = read_csv_from_s3(bucket, CONFIG["base_bond_key"])

    if base.empty:
        raise DataValidationError(
            "Base bond file is empty — cannot generate daily rows."
        )

    print(f"  [STAGE 1] Loaded {len(base):,} base bond records.")
    return base


# =============================================================
# STAGE 2 — generate_daily_rows  (UNCHANGED)
# =============================================================

def generate_daily_rows(
    base:       pd.DataFrame,
    start_date: str,
    end_date:   str,
) -> pd.DataFrame:
    """
    Expand the base bond universe to one row per bond per MARKET DAY only.
    Excludes weekends AND NYSE holidays.
    """
    # Get valid NYSE trading days (excludes weekends AND holidays)
    valid_days = get_valid_market_days(start_date, end_date)
    
    if not valid_days:
        print(f"  [STAGE 2] No market days in range [{start_date}, {end_date}] - returning empty.")
        return pd.DataFrame()
    
    dates = pd.DatetimeIndex(sorted(valid_days))
    n_days = len(dates)

    daily = base.loc[base.index.repeat(n_days)].copy()
    daily["date"] = np.tile(dates, len(base))
    daily["vol"] = daily["credit_rating"].map(VOL_MAP).fillna(10) / 100

    print(f"  [STAGE 2] Generated {len(daily):,} daily rows across {n_days} market days.")
    return daily

# =============================================================
# STAGE 3 — load_dgs10_history  (warm-up rows for rolling window)
# =============================================================

def load_dgs10_history(
    start_date:   date_type,
    run_id:       str,
    mode:         str,
    bucket:       str,
    old_rolling:  pd.DataFrame | None = None,
) -> pd.DataFrame:
    """
    Load the last N DGS10 rows strictly before start_date for rolling warm-up.

    Replay mode:
        Loads the raw snapshot written by the original run.
        Path: historical-bonds/raw/dgs10_history/year=Y/month=MM/day=DD/run_id=<id>/data.parquet
        No Snowflake or rolling parquet access — fully deterministic.

    Normal / Backfill mode:
        - FIRST: Try to extract from rolling parquet (if available)
        - SECOND: If rolling parquet has no data (first run), fetch directly from FRED
        - ALWAYS saves a snapshot to S3 for future replayability

    Returns DataFrame with columns [date, DGS10] sorted ascending.
    Empty DataFrame is valid (rolling computation uses min_periods=1).

    Args:
        start_date:  Processing window start date.
        run_id:      Current pipeline run identifier.
        mode:        "replay" | "backfill" | "incremental".
        bucket:      S3 bucket name.
        old_rolling: Rolling parquet DataFrame (None if first ever run).
    """
    n           = CONFIG["dgs10_history_rows"]
    snapshot_key = _raw_dgs10_key(start_date, run_id)

    # =============================================================
    # REPLAY MODE - Load from existing snapshot only
    # =============================================================
    if mode == "replay":
        if not s3_key_exists(bucket, snapshot_key):
            raise BondPipelineError(
                f"DGS10 history snapshot not found for replay: "
                f"s3://{bucket}/{snapshot_key}"
            )
        history          = read_parquet_from_s3(bucket, snapshot_key)
        history["date"]  = pd.to_datetime(history["date"])
        history["DGS10"] = pd.to_numeric(history["DGS10"], errors="coerce")
        history          = history.sort_values("date").reset_index(drop=True)
        print(
            f"  [STAGE 3] Loaded {len(history)} DGS10 history rows from S3 snapshot "
            f"(replay mode)."
        )
        return history

    # =============================================================
    # NORMAL / BACKFILL MODE - Try to get warm-up data
    # =============================================================
    hist_rows = pd.DataFrame(columns=["date", "DGS10"])
    start_ts = pd.Timestamp(start_date)
    
    # Option 1: Extract from existing rolling parquet (incremental mode, or subsequent backfill)
    if old_rolling is not None and not old_rolling.empty:
        hist_rows = (
            old_rolling[
                (old_rolling["date"] < start_ts) &
                (old_rolling["DGS10"].notna())
            ]
            [["date", "DGS10"]]
            .sort_values("date")
            .drop_duplicates()
            .tail(n)
            .reset_index(drop=True)
        )
        
        if not hist_rows.empty:
            print(
                f"  [STAGE 3] Found {len(hist_rows)} DGS10 history rows in rolling parquet "
                f"({hist_rows['date'].min().date()} <- {hist_rows['date'].max().date()})."
            )
    
    # Option 2: If no history found (first backfill run), fetch directly from FRED
    if hist_rows.empty and mode in ("backfill", "incremental"):
        print(
            f"  [STAGE 3] No DGS10 history in rolling parquet - "
            f"fetching last {n} days from FRED for warm-up..."
        )
        
        # Calculate date range for warm-up (need n days before start_date)
        warmup_end = start_ts - timedelta(days=1)
        warmup_start = warmup_end - timedelta(days=n * 2)  # Buffer to ensure we get n days
        
        fred_api_key = os.getenv("FRED_API_KEY")
        if not fred_api_key:
            print(f"  [STAGE 3][WARN] FRED_API_KEY not set - cannot fetch warm-up data")
        else:
            try:
                fred = Fred(api_key=fred_api_key)
                series = fred.get_series(
                    "DGS10",
                    observation_start=warmup_start.strftime("%Y-%m-%d"),
                    observation_end=warmup_end.strftime("%Y-%m-%d"),
                )
                
                if series is not None and not series.empty:
                    fred_df = (
                        series.to_frame("DGS10")
                        .reset_index()
                        .rename(columns={"index": "date"})
                    )
                    fred_df["date"] = pd.to_datetime(fred_df["date"])
                    fred_df["DGS10"] = pd.to_numeric(fred_df["DGS10"], errors="coerce")
                    
                    # Take only business days and last n rows
                    # Filter to business days (Monday-Friday)
                    fred_df = fred_df[fred_df["date"].dt.dayofweek < 5]
                    hist_rows = fred_df.tail(n).reset_index(drop=True)
                    
                    print(
                        f"  [STAGE 3] Fetched {len(hist_rows)} DGS10 history rows from FRED "
                        f"({hist_rows['date'].min().date()} <- {hist_rows['date'].max().date()})."
                    )
                else:
                    print(f"  [STAGE 3] FRED returned no data for warm-up period")
                    
            except Exception as exc:
                print(f"  [STAGE 3][WARN] Failed to fetch warm-up from FRED: {exc}")

    # =============================================================
    # ALWAYS SAVE SNAPSHOT if we have data (for future replayability)
    # This ensures backfill runs can be replayed deterministically
    # =============================================================
    if not hist_rows.empty:
        try:
            write_parquet_to_s3(hist_rows, bucket, snapshot_key)
            print(
                f"  [STAGE 3] DGS10 history snapshot saved: "
                f"s3://{bucket}/{snapshot_key} ({len(hist_rows)} rows)"
            )
        except Exception as exc:
            # Non-fatal — snapshot is best-effort, but we log it
            print(f"  [STAGE 3][WARN] Could not save DGS10 history snapshot: {exc}")
    else:
        print(
            "  [STAGE 3] No prior DGS10 history found — proceeding without warm-up. "
            "DGS10_ma will rely on min_periods=1 for the first runs."
        )

    return hist_rows


# =============================================================
# STAGE 4 — fetch_dgs10_from_fred  (UNCHANGED logic, new S3 paths)
# =============================================================

def fetch_dgs10_from_fred(
    start_date: str,
    end_date:   str,
    run_id:     str,
    mode:       str,
    bucket:     str,
) -> pd.DataFrame:
    """
    Fetch DGS10 (10-year Treasury yield) from FRED for the date range.

    Normal / Backfill mode:
        Calls the live FRED API with retry + backoff.
        Saves a raw Parquet snapshot keyed by (start_date, run_id).

    Replay mode:
        Loads the snapshot written by the original run — no FRED API call.
        Same run_id <- always same FRED observations <- deterministic output.

    Raises BondPipelineError if the FRED fetch fails after all retries
    (normal/backfill only).
    """
    snapshot_key = _raw_fred_key(start_date, run_id)

    if mode == "replay":
        frames = []
        current = pd.Timestamp(start_date)
        end = pd.Timestamp(end_date)
        
        valid_days = get_valid_market_days(start_date, end_date)
        
        while current <= end:
            current_date = current.date()
            if current_date in valid_days:
                date_run_id = _resolve_run_id_for_date(bucket, current_date)
                key = _raw_fred_key(current_date, date_run_id)
                if s3_key_exists(bucket, key):
                    frames.append(read_parquet_from_s3(bucket, key))
            current += timedelta(days=1)
        
        if not frames:
            raise BondPipelineError(f"No FRED partitions found")
        
        df = pd.concat(frames, ignore_index=True)
        df["date"] = pd.to_datetime(df["date"])
        df["DGS10"] = pd.to_numeric(df["DGS10"], errors="coerce")
        return df

    # Normal / Backfill — call live FRED API
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

                empty_df = pd.DataFrame(
                    columns=["date", "DGS10"]
                )

                raw_key = _raw_fred_key(
                    end_date,
                    run_id
                )

                write_parquet_to_s3(
                    empty_df,
                    bucket,
                    raw_key
                )

                print(
                    f"[STAGE 4] Empty FRED raw partition written: "
                    f"s3://{bucket}/{raw_key}"
                )

                return empty_df
            df = (
                series.to_frame("DGS10")
                .reset_index()
                .rename(columns={"index": "date"})
            )

            df["date"]  = pd.to_datetime(df["date"])
            df["DGS10"] = pd.to_numeric(df["DGS10"], errors="coerce")
            # =========================================================
            # WRITE ONE RAW PARQUET PER BUSINESS DATE
            # =========================================================

            df["date_only"] = pd.to_datetime(df["date"]).dt.date

            written_dates = 0

            valid_market_days = get_valid_market_days(start_date, end_date)
            
            for date_val, group in df.groupby("date_only"):
                if date_val not in valid_market_days:
                    continue

                raw_key = _raw_fred_key(date_val, run_id)

                write_parquet_to_s3(
                        group.drop(columns=["date_only"]),
                        bucket,
                        raw_key
                )

                written_dates += 1

            print(
                f"  [STAGE 4] Fetched {len(df)} DGS10 observations from FRED "
                f"and wrote {written_dates} raw business-date partitions."
            )

            return df.drop(columns=["date_only"])

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
    Combine history with new FRED data, align to MARKET DAYS (NYSE trading days),
    apply forward/backward fill with fill_method_flag tracking, and compute
    DGS10_ma (rolling 20-day mean) and dgs10_anom.

    This function uses NYSE calendar to exclude market holidays (e.g., Christmas,
    July 4th) - not just weekends. This ensures alignment with bond rows which
    are also generated only for market days.

    fill_method_flag values:
      "REAL"             — FRED observed value for that market day
      "FORWARD_FILLED"   — no FRED value; filled forward from prior observation
      "BACKWARD_FILLED"  — no FRED value and no prior; filled backward

    Returns DataFrame with columns:
        date, DGS10, fill_method_flag, DGS10_ma, dgs10_anom
    (new-date rows only — warm-up history is stripped after rolling computation)

    Args:
        history_df:  Warm-up DGS10 rows (dates before start_date)
        new_fred_df: Fresh FRED DGS10 observations for the window
        start_date:  Start of processing window (YYYY-MM-DD)
        end_date:    End of processing window (YYYY-MM-DD)

    Returns:
        DataFrame with DGS10 series aligned to market days only
    """
    # Get valid NYSE trading days (excludes weekends AND holidays)
    valid_market_days = get_valid_market_days(start_date, end_date)
    
    # If no market days in range, return empty DataFrame
    if not valid_market_days:
        print(f"  [STAGE 5] No market days in range [{start_date}, {end_date}] - returning empty.")
        return pd.DataFrame(columns=["date", "DGS10", "fill_method_flag", "DGS10_ma", "dgs10_anom"])
    
    # Create spine with market days only (not just business days)
    spine = pd.DataFrame({"date": pd.DatetimeIndex(sorted(valid_market_days))})

    # Combine warm-up history + new FRED rows
    combined_fred = pd.concat(
        [history_df[["date", "DGS10"]], new_fred_df[["date", "DGS10"]]],
        ignore_index=True,
    ).drop_duplicates(subset=["date"]).sort_values("date").reset_index(drop=True)

    # Merge FRED data onto market day spine
    spine = spine.merge(combined_fred, on="date", how="left")

    # ── Fill method tracking ──────────────────────────────────────────────
    # Track which rows have REAL FRED observations
    real_mask                    = spine["DGS10"].notna()
    spine["fill_method_flag"]    = np.where(real_mask, "REAL", pd.NA)

    # Forward fill missing DGS10 values (e.g., holidays with no FRED data)
    spine["_dgs10_before_ffill"] = spine["DGS10"].copy()
    spine["DGS10"]               = spine["DGS10"].ffill()
    ffill_mask = spine["_dgs10_before_ffill"].isna() & spine["DGS10"].notna()
    spine.loc[ffill_mask, "fill_method_flag"] = "FORWARD_FILLED"

    # Backward fill any remaining missing values (e.g., first day of window)
    spine["_dgs10_before_bfill"] = spine["DGS10"].copy()
    spine["DGS10"]               = spine["DGS10"].bfill()
    bfill_mask = spine["_dgs10_before_bfill"].isna() & spine["DGS10"].notna()
    spine.loc[bfill_mask, "fill_method_flag"] = "BACKWARD_FILLED"

    # Any remaining NULLs get BACKWARD_FILLED as fallback
    spine["fill_method_flag"] = spine["fill_method_flag"].fillna("BACKWARD_FILLED")
    spine = spine.drop(columns=["_dgs10_before_ffill", "_dgs10_before_bfill"])

    # ── FINAL FALLBACK: If still no DGS10 values, use last known from history ──
    if spine["DGS10"].isna().all():
        # Get last non-null DGS10 from warm-up history
        if not history_df.empty and history_df["DGS10"].notna().any():
            last_known = history_df["DGS10"].dropna().iloc[-1]
            spine["DGS10"] = last_known
            spine["fill_method_flag"] = "BACKWARD_FILLED"
            print(f"  [STAGE 5] No DGS10 in window - using last known value {last_known}")
        else:
            # Ultimate fallback - use 0 (should never happen in production)
            spine["DGS10"] = 0.0
            spine["fill_method_flag"] = "BACKWARD_FILLED"
            print(f"  [STAGE 5] No DGS10 in history - using 0 as fallback")

    # ── Rolling features over combined series (warm-up + new) ────────────
    # Calculate 20-day rolling average and anomaly (deviation from average)
    spine["DGS10_ma"]   = spine["DGS10"].rolling(
        CONFIG["dgs10_rolling_window"], min_periods=1
    ).mean()
    spine["dgs10_anom"] = spine["DGS10"] - spine["DGS10_ma"]

    # ── Strip warm-up rows — return only new-date rows ───────────────────
    # Remove warm-up history rows that were only needed for rolling calculation
    start_ts = pd.Timestamp(start_date)
    spine     = spine[spine["date"] >= start_ts].reset_index(drop=True)

    # Log fill method statistics for observability
    real_count  = int((spine["fill_method_flag"] == "REAL").sum())
    ffill_count = int((spine["fill_method_flag"] == "FORWARD_FILLED").sum())
    bfill_count = int((spine["fill_method_flag"] == "BACKWARD_FILLED").sum())

    print(
        f"  [STAGE 5] DGS10 series built — "
        f"REAL={real_count}, FORWARD_FILLED={ffill_count}, "
        f"BACKWARD_FILLED={bfill_count}  ({len(spine)} total rows)."
    )
    return spine



# =============================================================
# STAGE 6 — validate_data
# =============================================================
def validate_data(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Hard data-quality validation.
    
    Removes invalid rows.
    Critical threshold breaches raise DataValidationError.
    
    Returns:
        cleaned DataFrame (bad rows are discarded, not returned)
    """

    original_rows = len(df)

    required_cols = [
        "bond_id",
        "ticker",
        "date",
        "bond_price",
        "DGS10",
    ]

    # =========================================================
    # Required-column validation
    # =========================================================

    missing_cols = [
        c for c in required_cols
        if c not in df.columns
    ]

    if missing_cols:
        raise DataValidationError(
            f"Missing required columns: {missing_cols}"
        )

    # =========================================================
    # Null filtering
    # =========================================================

    null_mask = df[required_cols].isnull().any(axis=1)

    null_rows = int(null_mask.sum())

    null_pct = (
        null_rows / len(df)
        if len(df) else 0
    )

    # =========================================================
    # Invalid numeric filtering
    # =========================================================

    invalid_mask = (
        (df["coupon_rate"] < 0) |
        (df["maturity_years"] < 0) |
        (df["bond_price"] <= 0) |
        (df["DGS10"] <= 0)
    )

    invalid_rows = int(invalid_mask.sum())

    invalid_pct = (
        invalid_rows / len(df)
        if len(df) else 0
    )

    # =========================================================
    # Threshold enforcement
    # =========================================================

    if null_pct > CONFIG["max_null_pct"]:
        raise DataValidationError(
            f"Null percentage too high: {null_pct:.2%}"
        )

    if invalid_pct > CONFIG["max_negative_pct"]:
        raise DataValidationError(
            f"Invalid numeric percentage too high: {invalid_pct:.2%}"
        )

    # =========================================================
    # Remove bad rows (discard them - no DLQ)
    # =========================================================

    bad_mask = null_mask | invalid_mask

    cleaned = (
        df[~bad_mask]
        .copy()
        .reset_index(drop=True)
    )

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

def detect_anomalies(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Soft statistical anomaly detection.

    Flags suspicious values but does NOT fail pipeline.
    """

    df = df.copy()

    # =========================================================
    # DGS10 anomaly
    # =========================================================

    dgs10_std = df["DGS10"].std()

    if dgs10_std and not pd.isna(dgs10_std):

        dgs10_z = (
            (df["DGS10"] - df["DGS10"].mean())
            / dgs10_std
        )

        df["dgs10_anomaly_flag"] = (
            dgs10_z.abs()
            > CONFIG["dgs10_zscore_limit"]
        )

    else:

        df["dgs10_anomaly_flag"] = False

    # =========================================================
    # Credit spread anomaly
    # =========================================================

    spread_std = df["credit_spread"].std()

    if spread_std and not pd.isna(spread_std):

        spread_z = (
            (
                df["credit_spread"]
                - df["credit_spread"].mean()
            )
            / spread_std
        )

        df["spread_anomaly_flag"] = (
            spread_z.abs()
            > CONFIG["spread_zscore_limit"]
        )

    else:

        df["spread_anomaly_flag"] = False

    # =========================================================
    # Summary
    # =========================================================

    dgs10_flags = int(
        df["dgs10_anomaly_flag"].sum()
    )

    spread_flags = int(
        df["spread_anomaly_flag"].sum()
    )

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
    Preserves original pipeline logic exactly.
    Non-fatal on failure — bond rows without macro are still written.
    """
    try:
        macro          = read_csv_from_s3(bucket, CONFIG["macro_key"])
        macro          = macro.copy()
        macro["date"]  = pd.to_datetime(macro["date"])
        macro["mm_yy"] = macro["date"].dt.strftime("%m-%y")

        daily          = daily.copy()
        daily["mm_yy"] = daily["date"].dt.strftime("%m-%y")

        merged = daily.merge(
            macro.drop(columns=["date"]),
            on="mm_yy",
            how="inner",
        ).drop(columns=["mm_yy"])

        print(f"  [STAGE 8] Macro merge: {len(daily):,} <- {len(merged):,} rows.")
        return merged

    except Exception as exc:
        print(f"  [STAGE 8][WARN] Macro merge failed — proceeding without macro: {exc}")
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
    """
    df_copy           = df.copy()
    df_copy["_date_"] = pd.to_datetime(df_copy["date"]).dt.date
    partition_log     = {}

    for date_val, group in df_copy.groupby("_date_"):
        clean_group = group.drop(columns=["_date_"])
        key         = _feature_partition_key(date_val, run_id)
        write_parquet_to_s3(clean_group, bucket, key)
        partition_log[str(date_val)] = {"key": key, "rows": len(clean_group)}

    # =========================================================
    # ADD THIS: Write latest.json after all partitions
    # =========================================================
    latest_key = f"{CONFIG['features_prefix']}latest.json"
    latest_payload = {
        "run_id": run_id,
        "updated_at": utc_now().isoformat(),
        "feature_version": "v1",
        "schema_version": "1.0",
        "dates_written": list(partition_log.keys()),
    }
    
    try:
        s3 = get_s3()
        s3.put_object(
            Bucket=bucket,
            Key=latest_key,
            Body=json.dumps(latest_payload, default=str, indent=2),
        )
        print(f"  [LAYER 2] Updated latest.json: {latest_key}")
    except Exception as e:
        print(f"  [LAYER 2][WARN] Could not write latest.json: {e}")

    print(
        f"  [LAYER 2] Written {len(partition_log)} date partitions "
        f"for run_id={run_id}"
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
            f"(expected on first run)."
        )
        return None

    df          = read_parquet_from_s3(bucket, key)
    df["date"]  = pd.to_datetime(df["date"])
    print(
        f"  [ROLLING] Loaded {len(df):,} rows from rolling parquet "
        f"(date range: {df['date'].min().date()} <- {df['date'].max().date()})."
    )
    return df


def update_rolling_layer(
    new_features:   pd.DataFrame,
    old_rolling_df: pd.DataFrame | None,
    bucket:         str,
    window_days:    int = 30,
) -> pd.DataFrame:
    """
    Merge new features into the rolling serving parquet and trim to window_days.

    Safe for replay and backfill — new rows overwrite existing rows for the
    same (bond_id, date) pair (keep="last" after sort). This ensures the
    rolling parquet always reflects the latest recomputed state.

    Keeps ONLY last N MARKET DAYS (NYSE trading days, excludes holidays).

    Args:
        new_features:   Fully featured DataFrame for the processing window.
        old_rolling_df: Existing rolling parquet (None if first run).
        bucket:         S3 bucket name.
        window_days:    Market days to retain (default 30).

    Returns:
        Updated rolling DataFrame after write.
    """
    if old_rolling_df is not None and not old_rolling_df.empty:
        combined = pd.concat([old_rolling_df, new_features], ignore_index=True)
    else:
        combined = new_features.copy()

    combined["date"] = pd.to_datetime(combined["date"])

    # Deduplicate — new run takes precedence over prior rolling data
    combined = (
        combined
        .sort_values(["bond_id", "date"])
        .drop_duplicates(subset=["bond_id", "date"], keep="last")
        .reset_index(drop=True)
    )

    # =========================================================
    # KEEP LAST N MARKET DAYS (NYSE TRADING DAYS ONLY)
    # This excludes weekends AND holidays automatically
    # =========================================================

    # Get unique dates from combined data
    unique_dates = combined["date"].drop_duplicates().sort_values()
    
    # Convert to date objects and filter to valid market days
    market_day_dates = []
    for d in unique_dates:
        d_as_date = d.date() if hasattr(d, 'date') else d
        if not is_market_holiday(d_as_date):
            market_day_dates.append(d)
    
    # Take last N market days
    last_market_dates = pd.Series(market_day_dates).tail(window_days)
    
    # Filter combined DataFrame to only market days
    rolling = combined[combined["date"].isin(last_market_dates)]
    
    rolling = rolling.sort_values(["bond_id", "date"]).reset_index(drop=True)

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
    df:      pd.DataFrame,
    run_id:  str,
    run_ts:  datetime,
    mode:    str,
) -> pd.DataFrame:
    """
    Stamp every output row with first-pipeline lineage.

    These columns will be renamed to source_* by the second pipeline so
    both pipeline lineage sets coexist in the final Snowflake/Postgres output.

    run_mode is stamped here so Layer 2 consumers know which mode produced
    a given partition (informational — does not affect second-pipeline mode).
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
    start_date_override: str = None,
    replay_from_raw: bool = False,
    airflow_metadata: dict = None,
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

    Outputs (written to S3 — NO Snowflake or Postgres writes):
    - Layer 2 feature partitions (one per date per run_id)
    - Updated 30-day rolling serving parquet
    - Run metadata JSON
    - Raw FRED snapshot + DGS10 history snapshot (normal/backfill only)

    Args:
        start_date_override: "YYYY-MM-DD" string. Required for replay mode.
                             If provided without replay_from_raw=True <- backfill mode.
        replay_from_raw:     True <- replay mode. Loads raw S3 snapshots
                             instead of calling live APIs — deterministic.

    Returns:
        str: Status string for Airflow XCom
             ("ALREADY_CURRENT" | "NO_NEW_ROWS" | "SUCCESS_<n>_ROWS")

    Raises:
        BondPipelineError:   For unrecoverable pipeline failures.
        DataValidationError: For output data contract violations.
    """
    pipeline_start = time.time()
    run_ts         = utc_now()
    today          = today_utc()
    bucket         = CONFIG["s3_bucket"]

    # ──────────────────────────────────────────
    # MODE DETECTION — single, explicit decision
    # ──────────────────────────────────────────
    if replay_from_raw:
        mode = "replay"
    elif start_date_override:
        mode = "backfill"
    else:
        mode = "incremental"

    if mode == "replay" and not start_date_override:
        raise BondPipelineError("Replay mode requires start_date_override.")

    end_date = get_market_end_date()


    if is_market_holiday(end_date):
        run_id = f"skipped_{run_ts.strftime('%Y%m%d_%H%M%S')}"
        msg = f"NYSE closed on {end_date} (holiday/weekend). Skipping pipeline."
        print(f"  [MARKET CLOSED] {msg}")
        
        send_critical_alert(
            msg,
            context={"run_id": run_id, "date": str(end_date)}
        )
        
        _save_run_metadata(
            bucket=bucket, run_id=run_id, mode="skipped",
            start_date=end_date, end_date=end_date, nrows=0,
            run_ts=run_ts, airflow_metadata=airflow_metadata,
            status="SKIPPED", error="market_holiday"
        )
        return "SKIPPED_MARKET_HOLIDAY"

    print(f"\n{'=' * 66}")
    print(f"  BOND UPDATE PIPELINE (FIRST) START")
    print(f"  mode                : {mode}")
    print(f"  start_date_override : {start_date_override}")
    print(f"  replay_from_raw     : {replay_from_raw}")
    print(f"  end_date            : {end_date}")
    print(f"{'=' * 66}\n")

    try:
        # ══════════════════════════════════════════════════════════════
        # STEP 1 — Load rolling layer (needed for watermark + DGS10 history)
        # Non-critical: None is acceptable on first ever run.
        # ══════════════════════════════════════════════════════════════
        print("  [STEP 1] Loading rolling layer...")
        old_rolling_df = load_rolling_layer(bucket)

        # ══════════════════════════════════════════════════════════════
        # STEP 2 — Determine processing window + run_id
        # ══════════════════════════════════════════════════════════════
        print("\n  [STEP 2] Determining processing window...")

        if mode in ("replay", "backfill"):
            start_date = pd.Timestamp(start_date_override).date()
            print(
                f"  [{mode.upper()}] Processing window: "
                f"{start_date} <- {end_date}"
            )

        else:  # incremental
            # Watermark from rolling parquet — self-contained, no Snowflake needed
            if old_rolling_df is not None and not old_rolling_df.empty:
                last_date = pd.to_datetime(old_rolling_df["date"]).dt.date.max()
            else:
                # First ever run — default to window_days ago
                last_date = today - timedelta(days=CONFIG["window_days"])
                print(
                    f"  [INCREMENTAL] No rolling parquet found — "
                    f"defaulting start to {last_date} ({CONFIG['window_days']} days ago)."
                )

            print(f"  [INCREMENTAL] Last processed date: {last_date}")
            print(f"  [INCREMENTAL] Today              : {today}")

            if last_date >= today:
                print(
                    "  [INCREMENTAL] Rolling parquet is current — no new data to process."
                )
                return "ALREADY_CURRENT"

            start_date = last_date  # business days from last_date+1 generated below


        run_id = f"run_{run_ts.strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
        print(f"  output_run_id: {run_id}")

        # STARTED metadata sentinel for operational observability
        _save_run_metadata(
            bucket=bucket,

            run_id=run_id,

            mode=mode,

            start_date=start_date,

            end_date=end_date,

            nrows=0,

            run_ts=run_ts,

            airflow_metadata=airflow_metadata,

            status="STARTED",
        )

        # String forms for pd.date_range and FRED API calls
        start_str = (
            pd.Timestamp(start_date + timedelta(days=1)).strftime("%Y-%m-%d")
            if mode == "incremental"
            else pd.Timestamp(start_date).strftime("%Y-%m-%d")
        )
        end_str = end_date.strftime("%Y-%m-%d")

        # ══════════════════════════════════════════════════════════════
        # STEP 3 — Load base bond data from S3
        # ══════════════════════════════════════════════════════════════
        print("\n  [STEP 3] Loading base bond data from S3...")
        base = load_base_bond_data(bucket)

        # ══════════════════════════════════════════════════════════════
        # STEP 4 — Generate daily bond rows (business day expansion)
        # ══════════════════════════════════════════════════════════════
        print("\n  [STEP 4] Generating daily bond rows...")
        daily = generate_daily_rows(base, start_str, end_str)

        if daily.empty:
            print("  No business days in processing window — pipeline complete.")
            return "NO_NEW_ROWS"

        print(f"  Generated {len(daily):,} rows for window [{start_str}, {end_str}]")

        # ══════════════════════════════════════════════════════════════
        # STEP 5 — Load DGS10 warm-up history
        # ══════════════════════════════════════════════════════════════
        print("\n  [STEP 5] Loading DGS10 history for rolling warm-up...")
        dgs10_history = load_dgs10_history(
            start_date  = pd.Timestamp(start_str).date(),
            run_id      = run_id,
            mode        = mode,
            bucket      = bucket,
            old_rolling = old_rolling_df,
        )

        # ══════════════════════════════════════════════════════════════
        # STEP 6 — Fetch DGS10 from FRED (or load S3 snapshot for replay)
        # ══════════════════════════════════════════════════════════════
        print("\n  [STEP 6] Fetching DGS10 data...")
        dgs10_new = fetch_dgs10_from_fred(
            start_date = start_str,
            end_date   = end_str,
            run_id     = run_id,
            mode       = mode,
            bucket     = bucket,
        )

        # ══════════════════════════════════════════════════════════════
        # STEP 7 — Build DGS10 series with fill flags + rolling features
        # ══════════════════════════════════════════════════════════════
        print("\n  [STEP 7] Building DGS10 series with fill method flags...")
        dgs10_series = build_dgs10_series(
            history_df  = dgs10_history,
            new_fred_df = dgs10_new,
            start_date  = start_str,
            end_date    = end_str,
        )
        daily["market_value"] = (
            daily["bond_price"] * daily["units_outstanding"]
        )
        daily["outstanding_pct"] = np.where(
            daily["units_issued"] > 0,
            (daily["units_outstanding"] / daily["units_issued"]) * 100,
            np.nan
        )
        # Merge DGS10 enrichment into daily bond rows
        daily = daily.merge(dgs10_series, on="date", how="left")

        # ══════════════════════════════════════════════════════════════
        # STEP 8 — Merge macro data
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
        
        # ══════════════════════════════════════════════════════
        # DATA VALIDATION
        # ══════════════════════════════════════════════════════

        df_output = validate_data(df_output)

        # ══════════════════════════════════════════════════════
        # ANOMALY DETECTION
        # ══════════════════════════════════════════════════════

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
        df_for_rolling    = df_output.copy()
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
        # STEP 12 — Save Success run metadata for observability
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
        # STEP 13 — Pipeline success
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
        print(f"  window             : {start_str} <- {end_str}")
        print(f"  duration           : {processing_time}s")
        print(f"  fill_flags         : {fill_summary}")
        print(f"  Layer 2            : OK")
        print(f"  Rolling parquet    : OK")
        print(f"{'=' * 66}\n")


        return f"SUCCESS_{output_rows}_ROWS"


    except Exception as exc:

        processing_time = round(
            time.time() - pipeline_start,
            2,
        )

        send_critical_alert(
            "Bond update pipeline failed with unhandled exception",
            context={
                "mode": mode,
                "duration": f"{processing_time}s",
                "error": str(exc),
            },
        )

        print(f"\n{'=' * 66}")
        print(f"  BOND UPDATE PIPELINE FAILED")
        print(f"  mode     : {mode}")
        print(f"  error    : {exc}")
        print(f"  duration : {processing_time}s")
        print(f"{'=' * 66}\n")

        # FAILED metadata overwrite
        _save_run_metadata(
            bucket=bucket,

            run_id=run_id,

            mode=mode,

            start_date=(
                pd.Timestamp(start_str).date()
                if "start_str" in locals()
                else None
            ),

            end_date=end_date,

            nrows=0,

            run_ts=run_ts,

            airflow_metadata=airflow_metadata,

            status="FAILED",

            processing_time_s=processing_time,

            error=str(exc),
        )

        raise