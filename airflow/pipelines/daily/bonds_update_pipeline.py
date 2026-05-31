"""
bonds_update_pipeline.py
=========================
FIRST PIPELINE — Bond Ingestion and Feature Engineering

Responsibilities
----------------
- Load the static synthetic bond universe from S3
- Generate one row per bond per NYSE market day
- Fetch FRED DGS10 for incremental and backfill modes; read raw partitions
  for replay
- Build DGS10 rolling features: DGS10_ma, dgs10_anom, fill_method_flag
- Write Layer 2 feature partitions (one parquet per date per run_id)
- Maintain the 30-day rolling serving parquet
- Save run metadata for observability
- NO macro data (GDP, CPI, UNRATE, FEDFUNDS)
- NO ML predictions
- NO Snowflake writes
- NO Postgres writes

S3 storage layout
-----------------
RAW (immutable, append-only, partitioned by date):
    s3://<bucket>/historical-bonds/raw/
        year=YYYY/month=MM/day=DD/run_id=<run_id>/data.parquet

    One file per market day per run_id.
    Replay selects the latest file per date by S3 LastModified — no
    run_id lookup is ever required.

FEATURE LAYER (versioned per run_id, partitioned by date):
    s3://<bucket>/historical-bonds/features/
        year=YYYY/month=MM/day=DD/run_id=<run_id>/data.parquet
    s3://<bucket>/historical-bonds/features/latest.json

ROLLING SERVING LAYER (mutable, last 30 market days):
    s3://<bucket>/historical-bonds/rolling/bonds_features_30d.parquet

RUN METADATA (observability only — never used for replay resolution):
    s3://<bucket>/historical-bonds/run_metadata/<run_id>.json

Mode detection
--------------
Derived exclusively from function parameters — never from data columns.
    replay_from_raw=True          -> mode = "replay"
    start_date_override provided  -> mode = "backfill"
    default (neither)             -> mode = "incremental"

Mode behaviour
--------------
incremental:
    - Watermark = max(date) from rolling parquet; if absent, today - window_days
    - Warm-up rows sourced from the rolling parquet tail
    - Fetch fresh FRED DGS10 for watermark+1 -> today
    - Write raw partitions, Layer 2 partitions, update rolling parquet

backfill:
    - start_date = start_date_override
    - Warm-up rows sourced from the rolling parquet tail
    - Fetch fresh FRED DGS10 for the full backfill window
    - Write raw partitions, Layer 2 partitions, update rolling parquet

replay:
    - Load the LATEST raw partition for each market day by S3 LastModified
    - Warm-up reconstructed from raw partitions strictly before start_date
    - NO FRED API calls — fully deterministic and reproducible
    - Write a new Layer 2 partition under a new run_id
    - Update rolling parquet with safe key-based deduplication
    - Missing date partitions are logged and skipped — replay never aborts
      solely because a subset of partitions is absent

DGS10 warm-up
-------------
rolling(20) for DGS10_ma requires up to 20 prior market-day observations.
The pipeline builds a combined spine of [warmup_dates + window_dates],
computes rolling statistics over the full spine, then strips the warmup
rows before writing.  This guarantees that the first rows of any window
have the same DGS10_ma value regardless of mode.

Airflow notes
-------------
- Entry point : update_bonds_pipeline(start_date_override, replay_from_raw)
- Retries     : retries=2, retry_delay=timedelta(minutes=5)
- Concurrency : max_active_runs=1  (rolling parquet write is not concurrent-safe)
- DAG order   : run_bonds_update >> run_bonds_processing
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
# CONFIG  —  single source of truth
# =============================================================

CONFIG: dict = {
    # FRED fetch
    "max_retries":     3,
    "retry_backoff_s": 5,   # base seconds; multiplied by attempt index

    # DGS10 rolling warm-up
    "dgs10_history_rows":              20,  # market-day rows prepended before rolling
    "dgs10_warmup_lookback_multiplier": 4,  # calendar-day multiplier: 4 * 20 = 80 days
    "dgs10_rolling_window":            20,  # window size for DGS10_ma

    # Rolling serving layer
    "window_days": 30,   # market days retained in bonds_features_30d.parquet

    # S3 bucket
    "s3_bucket": "yeeshu-bond-bucket",

    # S3 reference files
    "base_bond_key": "synthetic_bond.csv",

    # S3 path prefixes
    "raw_prefix":          "historical-bonds/raw/",
    "features_prefix":     "historical-bonds/features/",
    "rolling_key":         "historical-bonds/rolling/bonds_features_30d.parquet",
    "run_metadata_prefix": "historical-bonds/run_metadata/",

    # Data quality thresholds
    "max_null_pct":     0.02,
    "max_negative_pct": 0.01,

    # Anomaly detection thresholds
    "dgs10_zscore_limit":  4.0,
    "spread_zscore_limit": 4.0,

    # Lineage — increment transformation version when feature logic changes
    "pipeline_name":  "bonds_update_pipeline",
    "data_source":    "fred+synthetic",
    "input_source":   "synthetic_bond+fred",
    "transformation": "bond_features_v1",
}

# Volatility basis points by credit rating, expressed as a decimal fraction
VOL_MAP: dict[str, int] = {
    "AAA":  2,  "AA+":  3, "AA":  4, "A+":  5,
    "A":    6,  "A-":   8,
    "BBB+": 10, "BBB": 12, "BBB-": 15,
    "BB+":  20, "BB":  25, "B":   35, "CCC": 50,
}

# Output column contract between this pipeline and the downstream processing
# pipeline.  The downstream pipeline renames pipeline_* columns to source_*.
LAYER2_OUTPUT_COLS: list[str] = [
    "bond_id", "ticker", "sector", "industry", "credit_rating",
    "coupon_rate", "issue_date", "maturity_date", "maturity_years",
    "date", "vol",
    "benchmark_yield", "corporate_yield", "credit_spread",
    "bond_price", "yield_to_maturity", "implied_hazard",
    "implied_pd_annual", "implied_pd_multi_year", "implied_rating",
    "market_synthetic_score",
    "issue_size", "units_issued", "units_outstanding",
    "market_value", "outstanding_pct",
    "DGS10", "DGS10_ma", "dgs10_anom", "fill_method_flag",
    "pipeline_name", "pipeline_run_id", "data_source",
    "input_source", "transformation", "record_created_at",
    "run_mode",
]


# =============================================================
# CUSTOM EXCEPTIONS
# =============================================================

class BondPipelineError(Exception):
    """Raised for unrecoverable pipeline failures."""


class DataValidationError(Exception):
    """Raised when output data fails quality or schema checks."""


# =============================================================
# ALERTING
# =============================================================

def send_alert(message: str, level: str = "INFO", context: dict = None) -> None:
    """
    Structured alert dispatcher.

    Levels: INFO | WARNING | CRITICAL

    INFO and WARNING are written to stdout only.
    CRITICAL additionally posts to the Slack webhook configured in the
    SLACK_WEBHOOK_URL environment variable, if present.  Webhook failures
    are non-fatal — the pipeline continues.
    """
    payload = {
        "level":    level,
        "pipeline": CONFIG["pipeline_name"],
        "message":  message,
        "context":  context or {},
        "ts":       datetime.now(timezone.utc).isoformat(),
    }
    print(f"[{level}] {message} | context={json.dumps(payload['context'], default=str)}")

    if level == "CRITICAL":
        webhook = os.getenv("SLACK_WEBHOOK_URL")
        if webhook:
            try:
                run_id    = (context or {}).get("run_id", "unknown")
                truncated = message[:500] + "..." if len(message) > 500 else message
                requests.post(
                    webhook,
                    json={"text": f"*[CRITICAL]* {CONFIG['pipeline_name']}\n{truncated}\nrun_id: {run_id}"},
                    timeout=3,
                )
            except Exception as exc:
                print(f"[ALERT ERROR] Slack notification failed: {exc}")


# =============================================================
# MARKET CALENDAR HELPERS
# =============================================================

NYSE_CALENDAR = mcal.get_calendar("NYSE")


def get_valid_market_days(start_date, end_date) -> set:
    """Return NYSE trading dates (as Python date objects) in [start_date, end_date]."""
    return set(
        pd.to_datetime(
            NYSE_CALENDAR.valid_days(start_date=start_date, end_date=end_date)
        ).date
    )


def is_market_holiday(check_date: date_type) -> bool:
    """Return True when check_date is not a valid NYSE trading session."""
    return NYSE_CALENDAR.schedule(start_date=check_date, end_date=check_date).empty


def get_market_end_date() -> date_type:
    """
    Return the appropriate processing end date relative to NYSE market close.

    Before 4:00 PM ET  — returns yesterday (avoid pulling incomplete session data).
    At or after 4:00 PM ET — returns today.
    """
    eastern      = pendulum.timezone("America/New_York")
    now_et       = datetime.now(eastern)
    market_close = datetime.strptime("16:00", "%H:%M").time()

    if now_et.time() < market_close:
        end_date = now_et.date() - timedelta(days=1)
        print(f"  [MARKET] Before 4:00 PM ET — end_date={end_date}")
    else:
        end_date = now_et.date()
        print(f"  [MARKET] After 4:00 PM ET — end_date={end_date}")

    return end_date


# =============================================================
# TIMEZONE HELPERS
# =============================================================

def utc_now() -> datetime:
    """Current datetime, tz-aware UTC."""
    return datetime.now(timezone.utc)


def today_utc() -> date_type:
    """Today's date in UTC."""
    return utc_now().date()


# =============================================================
# RETRY WITH EXPONENTIAL BACKOFF
# =============================================================

def retry_with_backoff(
    func,
    retries:        int   = 3,
    backoff_factor: int   = 2,
    exceptions:     tuple = (Exception,),
    critical_name:  str   = None,
    run_id:         str   = None,
):
    """
    Execute func() with exponential backoff on failure.

    A CRITICAL alert is sent only when all retries are exhausted and
    critical_name is provided.  Individual retry attempts are logged to
    stdout only so on-call noise is minimised.

    Args:
        func:           Zero-argument callable to execute.
        retries:        Maximum number of additional attempts after the first.
        backoff_factor: Wait seconds = backoff_factor ** attempt_index.
        exceptions:     Exception types that trigger a retry.
        critical_name:  Operation label used in the final-failure alert.
        run_id:         Pipeline run identifier for alert context.

    Returns:
        The return value of func() on success.

    Raises:
        The last caught exception after all retries are exhausted.
    """
    last_exc = None
    for attempt in range(retries + 1):
        try:
            return func()
        except exceptions as exc:
            last_exc = exc
            if attempt < retries:
                wait = backoff_factor ** attempt
                print(f"  Retry {attempt + 1}/{retries} in {wait}s: {exc}")
                time.sleep(wait)
            else:
                print(f"  All {retries} retries exhausted: {exc}")

    if critical_name:
        send_alert(
            f"{critical_name} failed after {retries} retries",
            level="CRITICAL",
            context={"run_id": run_id, "error": str(last_exc)},
        )
    raise last_exc


# =============================================================
# S3 HELPERS — basic I/O
# =============================================================

def get_s3():
    """Return a boto3 S3 client.  Called per-operation to avoid stale sessions."""
    return boto3.client("s3")


def s3_key_exists(bucket: str, key: str) -> bool:
    """Return True if the S3 key exists."""
    try:
        get_s3().head_object(Bucket=bucket, Key=key)
        return True
    except ClientError:
        return False


def read_csv_from_s3(bucket: str, key: str) -> pd.DataFrame:
    """Read a CSV file from S3. Raises BondPipelineError on failure."""
    try:
        print(f"  [S3 READ CSV]     s3://{bucket}/{key}")
        obj = get_s3().get_object(Bucket=bucket, Key=key)
        return pd.read_csv(BytesIO(obj["Body"].read()))
    except ClientError as exc:
        raise BondPipelineError(f"Failed to read s3://{bucket}/{key}: {exc}") from exc


def read_parquet_from_s3(bucket: str, key: str) -> pd.DataFrame:
    """Read a Parquet file from S3. Raises BondPipelineError on failure."""
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
# S3 PARTITION PATH BUILDERS
# =============================================================

def _raw_partition_key(target_date, run_id: str) -> str:
    """
    Return the S3 key for a raw bond data partition.

    Layout:  historical-bonds/raw/year=YYYY/month=MM/day=DD/run_id=<id>/data.parquet
    """
    d = pd.Timestamp(target_date)
    return (
        f"{CONFIG['raw_prefix']}"
        f"year={d.year}/month={d.month:02d}/day={d.day:02d}/"
        f"run_id={run_id}/data.parquet"
    )


def _feature_partition_key(target_date, run_id: str) -> str:
    """
    Return the S3 key for a Layer 2 feature partition.

    Layout:  historical-bonds/features/year=YYYY/month=MM/day=DD/run_id=<id>/data.parquet
    """
    d = pd.Timestamp(target_date)
    return (
        f"{CONFIG['features_prefix']}"
        f"year={d.year}/month={d.month:02d}/day={d.day:02d}/"
        f"run_id={run_id}/data.parquet"
    )


# =============================================================
# RAW LAYER PARTITION DISCOVERY
# =============================================================

def _get_latest_raw_keys(
    bucket:     str,
    start_date: date_type,
    end_date:   date_type,
) -> dict[date_type, str]:
    """
    Scan raw partitions under historical-bonds/raw/ and return the most
    recently written S3 key for each NYSE market day in [start_date, end_date].

    Selection rule: highest S3 LastModified within each date prefix.
    No run_id lookup is performed.  Dates with no partition are omitted
    from the result; callers decide whether absence is acceptable.

    Args:
        bucket:     S3 bucket name.
        start_date: Inclusive scan start.
        end_date:   Inclusive scan end.

    Returns:
        Dict mapping date -> latest S3 key for that date.
    """
    s3_client = get_s3()
    paginator = s3_client.get_paginator("list_objects_v2")
    result:   dict[date_type, str] = {}

    for market_day in get_valid_market_days(start_date, end_date):
        prefix = (
            f"{CONFIG['raw_prefix']}"
            f"year={market_day.year}/"
            f"month={market_day.month:02d}/"
            f"day={market_day.day:02d}/"
        )
        latest_key:      str | None      = None
        latest_modified: datetime | None = None

        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
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


def _read_raw_layer(
    bucket:     str,
    start_date: date_type,
    end_date:   date_type,
) -> pd.DataFrame:
    """
    Load the latest raw partition for every NYSE market day in
    [start_date, end_date].

    Called by:
    - Replay mode  : loads the window to be reprocessed.
    - All modes    : loads DGS10 warm-up history before the window start.

    Missing date partitions are logged as WARNING and skipped; processing
    continues on whatever data is available.  The caller receives an empty
    DataFrame only when no partitions exist at all for the requested range.

    After loading, the result is deduplicated by date (keep="last") so that
    a partition file storing a multi-day FRED response contributes at most
    one row per date into downstream calculations.

    Returns:
        DataFrame with at minimum columns [date, DGS10], sorted ascending.
        Returns an empty DataFrame when no partitions are found.
    """
    valid_days  = get_valid_market_days(start_date, end_date)
    latest_keys = _get_latest_raw_keys(bucket, start_date, end_date)

    missing = sorted(d for d in valid_days if d not in latest_keys)
    if missing:
        send_alert(
            f"{len(missing)} market day(s) have no raw partition in "
            f"[{start_date}, {end_date}] — those dates are skipped.",
            level="WARNING",
            context={"missing_dates": [str(d) for d in missing[:10]]},
        )

    if not latest_keys:
        print(f"  [RAW] No partitions found for [{start_date}, {end_date}].")
        return pd.DataFrame(columns=["date", "DGS10"])

    frames: list[pd.DataFrame] = []
    for market_day, key in sorted(latest_keys.items()):
        print(f"  [RAW] Loading latest partition for {market_day}: {key}")
        frames.append(read_parquet_from_s3(bucket, key))

    combined          = pd.concat(frames, ignore_index=True)
    combined["date"]  = pd.to_datetime(combined["date"])
    combined["DGS10"] = pd.to_numeric(combined["DGS10"], errors="coerce")

    combined = (
        combined
        .sort_values("date")
        .drop_duplicates(subset=["date"], keep="last")
        .reset_index(drop=True)
    )

    print(
        f"  [RAW] {len(latest_keys)} partition(s) loaded — "
        f"{len(combined):,} unique date rows."
    )
    return combined


def _write_raw_partitions(
    dgs10_df: pd.DataFrame,
    bucket:   str,
    run_id:   str,
) -> dict[str, str]:
    """
    Write one raw parquet per market day under the current run_id.

    Called after every successful live FRED fetch (incremental and backfill
    modes only).  Each file becomes part of the immutable audit trail that
    replay reads in future runs.

    Args:
        dgs10_df: DataFrame with columns [date, DGS10].
        bucket:   S3 bucket name.
        run_id:   Current pipeline run identifier.

    Returns:
        Dict mapping date string -> S3 key for each partition written.
    """
    df          = dgs10_df.copy()
    df["date"]  = pd.to_datetime(df["date"])
    df["_date"] = df["date"].dt.date
    written:    dict[str, str] = {}

    for market_day, group in df.groupby("_date"):
        key = _raw_partition_key(market_day, run_id)
        write_parquet_to_s3(group.drop(columns=["_date"], errors="ignore"), bucket, key)
        written[str(market_day)] = key

    print(f"  [RAW] Wrote {len(written)} partition(s) for run_id={run_id}.")
    return written


# =============================================================
# RUN METADATA — observability only
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
    processing_time_s: float = 0.0,
    partition_count:   int   = 0,
    rolling_updated:   bool  = False,
    error:             str   = "",
) -> None:
    """
    Persist run summary JSON to S3 for observability and audit purposes.

    This file is written on every run — including failures — and is never
    read back by the pipeline itself.  Replay resolution uses raw partition
    LastModified ordering, not metadata files.

    Non-fatal: a write failure here is logged and ignored.

    start_date may be None when this function is called before the
    processing window has been fully resolved.
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
        print(f"  [METADATA] {status} written: s3://{bucket}/{key}")
    except Exception as exc:
        print(f"  [METADATA][WARN] Could not write metadata: {exc}")


# =============================================================
# STAGE 1 — Base bond universe
# =============================================================

def load_base_bond_data(bucket: str) -> pd.DataFrame:
    """
    Load the static synthetic bond universe CSV from S3.

    This file defines the set of bonds whose daily rows are generated by
    generate_daily_rows().  It is loaded once per pipeline run.

    Raises:
        DataValidationError: when the file is empty.
        BondPipelineError:   when the S3 read fails.
    """
    base = read_csv_from_s3(bucket, CONFIG["base_bond_key"])
    if base.empty:
        raise DataValidationError("Base bond file is empty — cannot generate daily rows.")
    print(f"  [STAGE 1] Loaded {len(base):,} bond universe records.")
    return base


# =============================================================
# STAGE 2 — Daily row expansion
# =============================================================

def generate_daily_rows(
    base:       pd.DataFrame,
    start_date: str,
    end_date:   str,
) -> pd.DataFrame:
    """
    Cross-join the bond universe with every NYSE trading day in
    [start_date, end_date] to produce one row per bond per market day.

    Weekend and NYSE holiday dates are excluded.  The vol column is derived
    from the credit_rating -> VOL_MAP lookup, scaled to a decimal fraction.

    Args:
        base:       Bond universe DataFrame from load_base_bond_data().
        start_date: Inclusive window start as "YYYY-MM-DD".
        end_date:   Inclusive window end as "YYYY-MM-DD".

    Returns:
        DataFrame with one row per (bond, market_day).  Empty when no
        trading days exist in the window.
    """
    valid_days = get_valid_market_days(start_date, end_date)
    if not valid_days:
        print(f"  [STAGE 2] No market days in [{start_date}, {end_date}] — returning empty.")
        return pd.DataFrame()

    dates  = pd.DatetimeIndex(sorted(valid_days))
    n_days = len(dates)

    daily         = base.loc[base.index.repeat(n_days)].copy()
    daily["date"] = np.tile(dates, len(base))
    daily["vol"]  = daily["credit_rating"].map(VOL_MAP).fillna(10) / 100

    print(f"  [STAGE 2] Generated {len(daily):,} daily rows across {n_days} market days.")
    return daily


# =============================================================
# STAGE 3 — DGS10 warm-up history
# =============================================================

def load_dgs10_warmup_from_rolling(
    rolling_df: pd.DataFrame | None,
) -> pd.DataFrame:
    """
    Extract the last N DGS10 observations from the rolling serving parquet
    for use as rolling-window warm-up in incremental and backfill modes.

    Rolling parquet is the warm-up source for incremental and backfill because
    it is always available, already deduplicated, and already sorted.  Raw
    partition scanning is reserved for replay where the rolling parquet must
    not be consulted.

    Args:
        rolling_df: The current rolling parquet DataFrame, or None on first run.

    Returns:
        DataFrame with columns [date, DGS10], containing at most
        dgs10_history_rows rows sorted ascending.
        Returns an empty DataFrame when rolling_df is None or has no DGS10 data.
    """
    n = CONFIG["dgs10_history_rows"]

    if rolling_df is None or rolling_df.empty:
        send_alert(
            "Rolling parquet unavailable — proceeding without DGS10 warm-up. "
            "DGS10_ma will use min_periods=1 for the first window.",
            level="WARNING",
        )
        return pd.DataFrame(columns=["date", "DGS10"])

    if "DGS10" not in rolling_df.columns:
        send_alert(
            "Rolling parquet does not contain a DGS10 column — "
            "warm-up skipped.",
            level="WARNING",
        )
        return pd.DataFrame(columns=["date", "DGS10"])

    hist = (
        rolling_df[["date", "DGS10"]]
        .copy()
        .assign(date=lambda df: pd.to_datetime(df["date"]))
        .drop_duplicates(subset=["date"])
        .sort_values("date")
        .tail(n)
        .reset_index(drop=True)
    )

    if hist.empty:
        return pd.DataFrame(columns=["date", "DGS10"])

    print(
        f"  [STAGE 3] {len(hist)} DGS10 warm-up row(s) from rolling parquet "
        f"({hist['date'].min().date()} -> {hist['date'].max().date()})."
    )
    return hist


def load_dgs10_warmup_from_raw(
    bucket:     str,
    start_date: date_type,
) -> pd.DataFrame:
    """
    Load the last N DGS10 observations strictly before start_date from raw
    partitions for use as rolling-window warm-up in replay mode.

    Scans up to (dgs10_history_rows * dgs10_warmup_lookback_multiplier)
    calendar days before start_date, which is 80 days by default.  This
    window safely covers any extended holiday cluster while remaining bounded.

    Args:
        bucket:     S3 bucket name.
        start_date: Processing window start — warm-up rows must precede this.

    Returns:
        DataFrame with columns [date, DGS10] sorted ascending, at most N rows.
        Returns an empty DataFrame when no prior raw partitions exist.
    """
    n            = CONFIG["dgs10_history_rows"]
    multiplier   = CONFIG["dgs10_warmup_lookback_multiplier"]
    warmup_end   = pd.Timestamp(start_date).date() - timedelta(days=1)
    warmup_start = warmup_end - timedelta(days=n * multiplier)

    raw_df = _read_raw_layer(bucket, warmup_start, warmup_end)

    if raw_df.empty:
        send_alert(
            "No DGS10 raw partitions found before the replay window — "
            "proceeding without warm-up.  DGS10_ma will use min_periods=1.",
            level="WARNING",
            context={"start_date": str(start_date), "lookback_days": n * multiplier},
        )
        return pd.DataFrame(columns=["date", "DGS10"])

    hist = (
        raw_df[["date", "DGS10"]]
        .drop_duplicates(subset=["date"])
        .sort_values("date")
        .tail(n)
        .reset_index(drop=True)
    )

    print(
        f"  [STAGE 3] {len(hist)} DGS10 warm-up row(s) from raw partitions "
        f"({hist['date'].min().date()} -> {hist['date'].max().date()})."
    )
    return hist


# =============================================================
# STAGE 4 — FRED fetch  (incremental and backfill only)
# =============================================================

def fetch_dgs10_from_fred(
    start_date: str,
    end_date:   str,
    run_id:     str,
    bucket:     str,
) -> pd.DataFrame:
    """
    Fetch the DGS10 (10-year Treasury yield) time series from the live FRED
    API for the requested date range and persist raw partitions ONLY for
    NYSE market days that have real FRED data.

    CRITICAL BEHAVIOR:
    - FRED API returns data for ALL calendar dates (including weekends/holidays)
    - Raw partitions are written ONLY for NYSE market days (trading days)
    - Non-market days (weekends, NYSE holidays) NEVER receive raw partitions
    - NO empty partitions are ever written - missing data is handled by forward/backward fill
    - All dates are handled as naive datetime (no timezone) to match the rest of the pipeline

    Called only during incremental and backfill runs.  Replay mode reads
    existing raw partitions and never invokes this function.

    Args:
        start_date: Inclusive fetch start as "YYYY-MM-DD".
        end_date:   Inclusive fetch end as "YYYY-MM-DD".
        run_id:     Current pipeline run identifier, used for partition naming.
        bucket:     S3 bucket name.

    Returns:
        DataFrame with columns [date, DGS10] covering ONLY NYSE market days
        in the requested range (weekends/holidays are filtered out).

    Raises:
        BondPipelineError: when FRED_API_KEY is not set or all fetch
                           attempts are exhausted.
    """
    fred_api_key = os.getenv("FRED_API_KEY")
    if not fred_api_key:
        send_alert("FRED_API_KEY environment variable is not set.", level="CRITICAL",
                   context={"run_id": run_id})
        raise BondPipelineError("FRED_API_KEY not configured.")

    last_exc = None
    for attempt in range(1, CONFIG["max_retries"] + 1):
        try:
            fred = Fred(api_key=fred_api_key)
            series = fred.get_series(
                "DGS10",
                observation_start=start_date,
                observation_end=end_date,
            )

            # Get valid NYSE market days (excludes weekends and holidays)
            valid_market_days = get_valid_market_days(start_date, end_date)

            if series is None or series.empty:
                # FRED returned no data - write nothing to raw
                # Feature layer will fill missing dates via forward/backward fill
                print(
                    f"  [STAGE 4] FRED returned no observations for [{start_date}, {end_date}] — "
                    f"no raw partitions written. Feature layer will handle missing dates."
                )
                return pd.DataFrame(columns=["date", "DGS10"])

            # Convert FRED series to DataFrame
            df = series.to_frame("DGS10").reset_index().rename(columns={"index": "date"})
            df["date"] = pd.to_datetime(df["date"])
            df["DGS10"] = pd.to_numeric(df["DGS10"], errors="coerce")

            # Filter to market days ONLY (remove weekends and holidays like Memorial Day)
            # Use .date() comparison without timezone - all dates are naive
            df_market_only = df[df["date"].dt.date.isin(valid_market_days)].copy()

            if df_market_only.empty:
                # FRED has data but only for non-market days (holidays/weekends)
                # Write nothing to raw - feature layer will handle filling
                print(
                    f"  [STAGE 4] FRED returned data only for non-market days — "
                    f"no raw partitions written. Feature layer will handle market days."
                )
                return pd.DataFrame(columns=["date", "DGS10"])

            # Write raw partitions ONLY for market days that have real FRED data
            written = _write_raw_partitions(df_market_only, bucket, run_id)

            print(
                f"  [STAGE 4] Fetched {len(df)} FRED observation(s), "
                f"filtered to {len(df_market_only)} market day(s), "
                f"wrote {len(written)} raw partition(s)."
            )

            return df_market_only

        except Exception as exc:
            last_exc = exc
            print(f"  [STAGE 4] FRED attempt {attempt}/{CONFIG['max_retries']} failed: {exc}")
            if attempt < CONFIG["max_retries"]:
                time.sleep(CONFIG["retry_backoff_s"] * attempt)

    send_alert(
        f"FRED DGS10 fetch failed after {CONFIG['max_retries']} attempts.",
        level="CRITICAL",
        context={"run_id": run_id, "start": start_date, "end": end_date,
                 "error": str(last_exc)},
    )
    raise BondPipelineError(
        f"FRED DGS10 fetch failed after {CONFIG['max_retries']} attempts: {last_exc}"
    )

    
# =============================================================
# STAGE 5 — DGS10 series construction
# =============================================================

def build_dgs10_series(
    history_df:  pd.DataFrame,
    new_fred_df: pd.DataFrame,
    start_date:  str,
    end_date:    str,
) -> pd.DataFrame:
    """
    Produce a complete, gap-filled DGS10 time series aligned to NYSE market
    days for [start_date, end_date], with rolling statistics attached.

    Approach
    --------
    A combined spine is built from two sets of dates:

    1. Warm-up dates  — the dates present in history_df (strictly before
       start_date).  These rows participate in the rolling calculation but
       are stripped from the return value so they never appear in output.

    2. Window dates   — all valid NYSE market days in [start_date, end_date].
       These are the rows returned to the caller.

    FRED observations from history_df and new_fred_df are merged onto this
    combined spine.  When the same date appears in both, the new observation
    (from new_fred_df) takes precedence (keep="last" after sort).

    Gap filling
    -----------
    After merging, any market day with no FRED observation is filled:
      - Forward fill first (carry the most recent prior observation).
      - Backward fill for any remaining leading nulls.
      - Whole-spine null fallback: use the last known value from history_df,
        or 0.0 if history is also empty.

    Each row is assigned a fill_method_flag:
      "REAL"            — FRED observation present for that market day.
      "FORWARD_FILLED"  — no direct observation; filled from a prior date.
      "BACKWARD_FILLED" — no prior observation available; filled backward.

    Rolling statistics
    ------------------
    DGS10_ma   = rolling(20).mean() with min_periods=1.
    dgs10_anom = DGS10 - DGS10_ma.

    Both are computed over the full combined spine (warm-up + window) so that
    the first window rows benefit from up to 20 prior observations.

    Args:
        history_df:  Warm-up DataFrame with columns [date, DGS10].
        new_fred_df: New observations with columns [date, DGS10].
        start_date:  Inclusive window start as "YYYY-MM-DD".
        end_date:    Inclusive window end as "YYYY-MM-DD".

    Returns:
        DataFrame with columns [date, DGS10, fill_method_flag, DGS10_ma,
        dgs10_anom] covering only the window dates.
    """
    valid_window_days = get_valid_market_days(start_date, end_date)
    empty_result = pd.DataFrame(
        columns=["date", "DGS10", "fill_method_flag", "DGS10_ma", "dgs10_anom"]
    )

    if not valid_window_days:
        print(f"  [STAGE 5] No market days in [{start_date}, {end_date}] — returning empty.")
        return empty_result

    start_ts = pd.Timestamp(start_date)

    # ── Build unified observation set ───────────────────────────────────
    # new_fred_df is appended after history_df so keep="last" retains the
    # fresh observation when both cover the same date.
    parts = []
    if not history_df.empty and "DGS10" in history_df.columns:
        parts.append(history_df[["date", "DGS10"]])
    if not new_fred_df.empty and "DGS10" in new_fred_df.columns:
        parts.append(new_fred_df[["date", "DGS10"]])

    if parts:
        obs = pd.concat(parts, ignore_index=True)
    else:
        obs = pd.DataFrame(columns=["date", "DGS10"])

    obs["date"]  = pd.to_datetime(obs["date"])
    obs["DGS10"] = pd.to_numeric(obs["DGS10"], errors="coerce")
    obs = (
        obs.sort_values("date")
           .drop_duplicates(subset=["date"], keep="last")
           .reset_index(drop=True)
    )

    # ── Build combined spine: warmup dates + window dates ───────────────
    warmup_dates: list[pd.Timestamp] = []
    if not history_df.empty:
        h = history_df.copy()
        h["date"] = pd.to_datetime(h["date"])
        warmup_dates = sorted(h.loc[h["date"] < start_ts, "date"].tolist())

    window_dates = sorted(pd.Timestamp(d) for d in valid_window_days)
    all_dates    = sorted(set(warmup_dates) | set(window_dates))

    spine = pd.DataFrame({"date": pd.DatetimeIndex(all_dates)})
    spine = spine.merge(obs, on="date", how="left")

    # ── Gap filling with fill_method_flag ────────────────────────────────
    spine["fill_method_flag"] = np.where(spine["DGS10"].notna(), "REAL", pd.NA)

    spine["_before_ffill"] = spine["DGS10"].copy()
    spine["DGS10"]         = spine["DGS10"].ffill()
    ffill_mask = spine["_before_ffill"].isna() & spine["DGS10"].notna()
    spine.loc[ffill_mask, "fill_method_flag"] = "FORWARD_FILLED"

    spine["_before_bfill"] = spine["DGS10"].copy()
    spine["DGS10"]         = spine["DGS10"].bfill()
    bfill_mask = spine["_before_bfill"].isna() & spine["DGS10"].notna()
    spine.loc[bfill_mask, "fill_method_flag"] = "BACKWARD_FILLED"

    spine["fill_method_flag"] = spine["fill_method_flag"].fillna("BACKWARD_FILLED")
    spine = spine.drop(columns=["_before_ffill", "_before_bfill"])

    # ── Whole-spine null fallback ────────────────────────────────────────
    if spine["DGS10"].isna().all():
        if not history_df.empty and history_df["DGS10"].notna().any():
            last_val = history_df["DGS10"].dropna().iloc[-1]
            spine["DGS10"]            = last_val
            spine["fill_method_flag"] = "BACKWARD_FILLED"
            print(f"  [STAGE 5] Entire window has no FRED data — using last known value {last_val}.")
        else:
            spine["DGS10"]            = 0.0
            spine["fill_method_flag"] = "BACKWARD_FILLED"
            print("  [STAGE 5] No FRED history available — DGS10 set to 0.0 as emergency fallback.")

    # ── Rolling statistics over full spine (warmup + window) ────────────
    spine["DGS10_ma"]   = spine["DGS10"].rolling(CONFIG["dgs10_rolling_window"], min_periods=1).mean()
    spine["dgs10_anom"] = spine["DGS10"] - spine["DGS10_ma"]

    # ── Strip warm-up rows, return window only ───────────────────────────
    result = spine[spine["date"] >= start_ts].reset_index(drop=True)

    real  = int((result["fill_method_flag"] == "REAL").sum())
    fwd   = int((result["fill_method_flag"] == "FORWARD_FILLED").sum())
    bwd   = int((result["fill_method_flag"] == "BACKWARD_FILLED").sum())
    print(
        f"  [STAGE 5] DGS10 series built: REAL={real}, "
        f"FORWARD_FILLED={fwd}, BACKWARD_FILLED={bwd} "
        f"({len(result)} window rows)."
    )
    return result


# =============================================================
# STAGE 6 — Data validation
# =============================================================

def validate_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply hard data-quality rules to the output DataFrame.

    Rows with null values in required columns or structurally invalid numeric
    values are dropped.  When the proportion of bad rows exceeds the configured
    thresholds, DataValidationError is raised so the pipeline fails loudly
    rather than writing corrupt feature partitions.

    Args:
        df: Output DataFrame prior to writing.

    Returns:
        Cleaned DataFrame with invalid rows removed.

    Raises:
        DataValidationError: when missing-column or threshold checks fail.
    """
    original_rows = len(df)
    required_cols = ["bond_id", "ticker", "date", "bond_price", "DGS10"]

    missing_cols = [c for c in required_cols if c not in df.columns]
    if missing_cols:
        raise DataValidationError(f"Missing required columns: {missing_cols}")

    null_mask    = df[required_cols].isnull().any(axis=1)
    null_pct     = null_mask.sum() / len(df) if len(df) else 0.0

    invalid_mask = (
        (df["coupon_rate"]    < 0) |
        (df["maturity_years"] < 0) |
        (df["bond_price"]     <= 0) |
        (df["DGS10"]          <= 0)
    )
    invalid_pct = invalid_mask.sum() / len(df) if len(df) else 0.0

    if null_pct > CONFIG["max_null_pct"]:
        raise DataValidationError(
            f"Null rate {null_pct:.2%} exceeds threshold {CONFIG['max_null_pct']:.2%}."
        )
    if invalid_pct > CONFIG["max_negative_pct"]:
        raise DataValidationError(
            f"Invalid numeric rate {invalid_pct:.2%} exceeds threshold "
            f"{CONFIG['max_negative_pct']:.2%}."
        )

    cleaned = df[~(null_mask | invalid_mask)].copy().reset_index(drop=True)
    print(
        f"  [VALIDATE] rows_before={original_rows:,}  "
        f"rows_after={len(cleaned):,}  "
        f"rows_dropped={(original_rows - len(cleaned)):,}"
    )
    return cleaned


# =============================================================
# STAGE 7 — Anomaly detection
# =============================================================

def detect_anomalies(df: pd.DataFrame) -> pd.DataFrame:
    """
    Flag statistically unusual values without dropping rows.

    Two soft anomaly flags are computed per row:
      dgs10_anomaly_flag  — DGS10 z-score exceeds dgs10_zscore_limit.
      spread_anomaly_flag — credit_spread z-score exceeds spread_zscore_limit.

    These flags are informational.  The pipeline always continues regardless
    of flag counts.  Downstream consumers decide whether to exclude flagged rows.
    """
    df = df.copy()

    dgs10_std = df["DGS10"].std()
    if dgs10_std and not pd.isna(dgs10_std):
        dgs10_z = (df["DGS10"] - df["DGS10"].mean()) / dgs10_std
        df["dgs10_anomaly_flag"] = dgs10_z.abs() > CONFIG["dgs10_zscore_limit"]
    else:
        df["dgs10_anomaly_flag"] = False

    if "credit_spread" in df.columns:
        spread_std = df["credit_spread"].std()
        if spread_std and not pd.isna(spread_std):
            spread_z = (df["credit_spread"] - df["credit_spread"].mean()) / spread_std
            df["spread_anomaly_flag"] = spread_z.abs() > CONFIG["spread_zscore_limit"]
        else:
            df["spread_anomaly_flag"] = False
    else:
        df["spread_anomaly_flag"] = False

    print(
        f"  [ANOMALY] dgs10_flags={int(df['dgs10_anomaly_flag'].sum()):,}  "
        f"spread_flags={int(df['spread_anomaly_flag'].sum()):,}"
    )
    return df


# =============================================================
# LAYER 2 — Feature partition writer
# =============================================================

def write_layer2_features(
    df:     pd.DataFrame,
    bucket: str,
    run_id: str,
) -> dict:
    """
    Write one Parquet file per calendar date to the Layer 2 feature store,
    versioned under the current run_id.  Also updates the features/latest.json
    pointer so downstream consumers can discover the most recent run.

    Args:
        df:     Output DataFrame; must contain a date column.
        bucket: S3 bucket name.
        run_id: Current pipeline run identifier.

    Returns:
        Dict mapping date string -> {"key": s3_key, "rows": row_count}.
    """
    df_copy           = df.copy()
    df_copy["_date_"] = pd.to_datetime(df_copy["date"]).dt.date
    partition_log     = {}

    for date_val, group in df_copy.groupby("_date_"):
        key = _feature_partition_key(date_val, run_id)
        write_parquet_to_s3(group.drop(columns=["_date_"]), bucket, key)
        partition_log[str(date_val)] = {"key": key, "rows": len(group)}

    latest_key = f"{CONFIG['features_prefix']}latest.json"
    try:
        get_s3().put_object(
            Bucket=bucket,
            Key=latest_key,
            Body=json.dumps(
                {
                    "run_id":          run_id,
                    "updated_at":      utc_now().isoformat(),
                    "feature_version": "v1",
                    "schema_version":  "1.0",
                    "dates_written":   list(partition_log.keys()),
                },
                default=str,
                indent=2,
            ),
        )
        print(f"  [LAYER 2] Updated latest.json: s3://{bucket}/{latest_key}")
    except Exception as exc:
        print(f"  [LAYER 2][WARN] Could not update latest.json: {exc}")

    print(f"  [LAYER 2] {len(partition_log)} partition(s) written for run_id={run_id}.")
    return partition_log


# =============================================================
# ROLLING LAYER — load and update
# =============================================================

def load_rolling_layer(bucket: str) -> pd.DataFrame | None:
    """
    Load the 30-day rolling serving parquet from S3.

    Returns None when the file does not yet exist (expected on the first
    pipeline run).  Raises BondPipelineError when the key exists but cannot
    be read.  The date column is normalised to datetime64 on load.
    """
    key = CONFIG["rolling_key"]
    if not s3_key_exists(bucket, key):
        print(f"  [ROLLING] Rolling parquet not found at s3://{bucket}/{key} "
              "(expected on first run).")
        return None

    df         = read_parquet_from_s3(bucket, key)
    df["date"] = pd.to_datetime(df["date"])
    print(
        f"  [ROLLING] Loaded {len(df):,} rows "
        f"({df['date'].min().date()} -> {df['date'].max().date()})."
    )
    return df


def update_rolling_layer(
    new_features:   pd.DataFrame,
    old_rolling_df: pd.DataFrame | None,
    bucket:         str,
    window_days:    int = 30,
) -> pd.DataFrame:
    """
    Merge new feature rows into the rolling serving parquet and trim to the
    last window_days NYSE trading days.

    Merge semantics:
    - When the same (bond_id, date) appears in both old and new data,
      the new row takes precedence (keep="last" after sort+concat).
    - Window trimming is based on NYSE trading days only; calendar weekends
      and exchange holidays are not counted toward the window.

    Both sides are normalised to datetime64 before concat to prevent silent
    type-mismatch duplicates when one side carries Python date objects.

    Args:
        new_features:   Feature DataFrame from the current run.
        old_rolling_df: Existing rolling parquet, or None on first run.
        bucket:         S3 bucket name.
        window_days:    Number of NYSE market days to retain.

    Returns:
        The updated rolling DataFrame after writing to S3.
    """
    new_df         = new_features.copy()
    new_df["date"] = pd.to_datetime(new_df["date"])

    if old_rolling_df is not None and not old_rolling_df.empty:
        old_df         = old_rolling_df.copy()
        old_df["date"] = pd.to_datetime(old_df["date"])
        combined       = pd.concat([old_df, new_df], ignore_index=True)
    else:
        combined = new_df.copy()

    combined["date"] = pd.to_datetime(combined["date"])
    combined = (
        combined
        .sort_values(["bond_id", "date"])
        .drop_duplicates(subset=["bond_id", "date"], keep="last")
        .reset_index(drop=True)
    )

    # Retain only the last window_days NYSE trading days
    unique_dates     = combined["date"].drop_duplicates().sort_values()
    market_days      = [
        d for d in unique_dates
        if not is_market_holiday(d.date() if hasattr(d, "date") else d)
    ]
    tail_dates = pd.Series(market_days).tail(window_days)
    rolling    = combined[combined["date"].isin(tail_dates)]
    rolling    = rolling.sort_values(["bond_id", "date"]).reset_index(drop=True)

    write_parquet_to_s3(rolling, bucket, CONFIG["rolling_key"])
    print(
        f"  [ROLLING] Updated: {len(rolling):,} rows, "
        f"{len(tail_dates)} market days retained."
    )
    return rolling


# =============================================================
# LINEAGE — pipeline metadata stamp
# =============================================================

def _add_pipeline_metadata(
    df:     pd.DataFrame,
    run_id: str,
    run_ts: datetime,
    mode:   str,
) -> pd.DataFrame:
    """
    Stamp every output row with first-pipeline lineage columns.

    The downstream processing pipeline renames these pipeline_* columns
    to source_* so that both pipeline lineage sets coexist in the final
    output without collision.

    run_mode carries the execution mode string so downstream consumers can
    distinguish live-ingested rows from replayed rows.
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
# MAIN PIPELINE — update_bonds_pipeline
# =============================================================

def update_bonds_pipeline(
    start_date_override: str  = None,
    replay_from_raw:     bool = False,
    airflow_metadata:    dict = None,
) -> str:
    """
    First pipeline entry point: bond ingestion and feature engineering.

    Mode selection (derived from parameters only — never from data):
    ┌────────────────────────────────────┬──────────────┐
    │ Condition                          │ mode         │
    ├────────────────────────────────────┼──────────────┤
    │ replay_from_raw = True             │ "replay"     │
    │ start_date_override provided       │ "backfill"   │
    │ neither                            │ "incremental"│
    └────────────────────────────────────┴──────────────┘

    Incremental and backfill:
    - Warm-up sourced from the rolling parquet tail.
    - New DGS10 data fetched from FRED and written as raw partitions.
    - Layer 2 partitions and rolling parquet updated.

    Replay:
    - Warm-up and window data sourced exclusively from raw partitions.
    - Latest raw partition per date selected by S3 LastModified — no
      run_id lookup.
    - Missing date partitions are logged and skipped; replay never aborts
      unless the entire requested window has zero raw partitions.
    - Layer 2 partitions written under a new run_id.
    - Rolling parquet updated with safe key-based deduplication.

    Window start resolution (incremental):
    - If the rolling parquet exists: start = max(date) in rolling parquet.
    - If the rolling parquet does not exist:
      start = today - timedelta(days=window_days).

    Args:
        start_date_override: "YYYY-MM-DD" start for backfill or replay.
                             Required when replay_from_raw=True.
        replay_from_raw:     When True, reads raw partitions instead of FRED.
        airflow_metadata:    Airflow execution context for observability.

    Returns:
        Status string for Airflow XCom:
            "ALREADY_CURRENT"  — rolling parquet already covers today.
            "NO_NEW_ROWS"      — window contains no NYSE trading days.
            "SUCCESS_<n>_ROWS" — pipeline completed; n feature rows written.

    Raises:
        BondPipelineError:   Unrecoverable infrastructure failure.
        DataValidationError: Output data fails quality or schema checks.
    """
    pipeline_start = time.time()
    run_ts         = utc_now()
    today          = today_utc()
    bucket         = CONFIG["s3_bucket"]

    # ── Mode resolution ──────────────────────────────────────────────────
    if replay_from_raw:
        mode = "replay"
    elif start_date_override:
        mode = "backfill"
    else:
        mode = "incremental"

    if mode == "replay" and not start_date_override:
        raise BondPipelineError("Replay mode requires start_date_override.")

    end_date = get_market_end_date()

    print(f"\n{'=' * 66}")
    print(f"  BOND UPDATE PIPELINE START")
    print(f"  mode                : {mode}")
    print(f"  start_date_override : {start_date_override}")
    print(f"  replay_from_raw     : {replay_from_raw}")
    print(f"  end_date            : {end_date}")
    print(f"{'=' * 66}\n")

    run_id    = f"run_{run_ts.strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
    start_str = ""   # resolved in STEP 2; used in the except block

    try:
        # ── STEP 1 : Load rolling layer ──────────────────────────────────
        print("  [STEP 1] Loading rolling serving layer...")
        old_rolling_df = load_rolling_layer(bucket)

        # ── STEP 2 : Resolve processing window ──────────────────────────
        print("\n  [STEP 2] Resolving processing window...")

        if mode in ("replay", "backfill"):
            start_date = pd.Timestamp(start_date_override).date()

        else:  # incremental
            if old_rolling_df is not None and not old_rolling_df.empty:
                start_date = pd.to_datetime(old_rolling_df["date"]).dt.date.max()
            else:
                extra_days = 15
                start_date = today - timedelta(days=CONFIG["window_days"] + extra_days)
                print(
                    f"  [INCREMENTAL] No rolling parquet found — "
                    f"defaulting start_date to today - {CONFIG['window_days']}d = {start_date}."
                )

            print(f"  [INCREMENTAL] last processed date : {start_date}")
            print(f"  [INCREMENTAL] today               : {today}")

            if start_date >= today:
                print("  [INCREMENTAL] Rolling parquet is already current — nothing to process.")
                return "ALREADY_CURRENT"

        # For incremental mode, start_str advances one day past the last
        # processed date.  For backfill and replay, start_str equals start_date.
        start_str = (
            pd.Timestamp(start_date + timedelta(days=1)).strftime("%Y-%m-%d")
            if mode == "incremental"
            else pd.Timestamp(start_date).strftime("%Y-%m-%d")
        )
        end_str = end_date.strftime("%Y-%m-%d")

        print(f"  run_id             : {run_id}")
        print(f"  processing window  : {start_str} -> {end_str}")

        # STARTED sentinel — written after start_str is resolved so the
        # date field is always a valid ISO string.
        _save_run_metadata(
            bucket=bucket, run_id=run_id, mode=mode,
            start_date=pd.Timestamp(start_str).date(), end_date=end_date,
            nrows=0, run_ts=run_ts, airflow_metadata=airflow_metadata,
            status="STARTED",
        )

        # ── STEP 3 : Load bond universe ──────────────────────────────────
        print("\n  [STEP 3] Loading bond universe...")
        base = load_base_bond_data(bucket)

        # ── STEP 4 : Generate daily bond rows ────────────────────────────
        print("\n  [STEP 4] Generating daily bond rows...")
        daily = generate_daily_rows(base, start_str, end_str)
        if daily.empty:
            print("  No market days in the processing window — pipeline complete.")
            return "NO_NEW_ROWS"

        print(f"  Generated {len(daily):,} rows for [{start_str}, {end_str}].")

        # ── STEP 5 : Obtain DGS10 observations for the window ────────────
        # Incremental / backfill: fetch from live FRED, persist raw partitions.
        # Replay: load latest raw partition per date, never call FRED.
        if mode == "replay":
            print("\n  [STEP 5] Loading DGS10 from raw partitions (replay)...")
            replay_start = pd.Timestamp(start_str).date()
            dgs10_new    = _read_raw_layer(bucket, replay_start, end_date)

            if dgs10_new.empty:
                print(f"  [STEP 5] No raw partitions found for window [{start_str}, {end_str}] — "
                    f"will use forward/backward fill only.")
                dgs10_new = pd.DataFrame(columns=["date", "DGS10"])

            # Log partially missing dates as a warning — replay continues
            valid_window = get_valid_market_days(start_str, end_str)
            loaded_dates = set(dgs10_new["date"].dt.date.tolist())
            missing      = sorted(d for d in valid_window if d not in loaded_dates)
            if missing:
                send_alert(
                    f"{len(missing)} market day(s) have no raw partition in the replay "
                    "window — those dates will receive FORWARD_FILLED or BACKWARD_FILLED "
                    "DGS10 values.",
                    level="WARNING",
                    context={"missing_count": len(missing),
                             "examples": [str(d) for d in missing[:10]]},
                )
            print(f"  [STEP 5] Loaded {len(dgs10_new):,} DGS10 rows from raw layer.")
        else:
            print("\n  [STEP 5] Fetching DGS10 from FRED...")
            dgs10_new = fetch_dgs10_from_fred(
                start_date=start_str,
                end_date=end_str,
                run_id=run_id,
                bucket=bucket,
            )

        # ── STEP 6 : Load DGS10 warm-up history ─────────────────────────
        # Incremental and backfill source warm-up from the rolling parquet.
        # Replay reconstructs warm-up from raw partitions to remain
        # independent of the rolling parquet state.
        print("\n  [STEP 6] Loading DGS10 warm-up history...")
        if mode == "replay":
            dgs10_history = load_dgs10_warmup_from_raw(
                bucket=bucket,
                start_date=pd.Timestamp(start_str).date(),
            )
        else:
            dgs10_history = load_dgs10_warmup_from_rolling(old_rolling_df)

        # ── STEP 7 : Build DGS10 series with rolling statistics ──────────
        print("\n  [STEP 7] Building DGS10 series...")
        dgs10_series = build_dgs10_series(
            history_df  = dgs10_history,
            new_fred_df = dgs10_new,
            start_date  = start_str,
            end_date    = end_str,
        )

        # ── STEP 8 : Compute derived bond columns ────────────────────────
        daily["market_value"] = daily["bond_price"] * daily["units_outstanding"]
        daily["outstanding_pct"] = np.where(
            daily["units_issued"] > 0,
            (daily["units_outstanding"] / daily["units_issued"]) * 100,
            np.nan,
        )
        daily["date"] = pd.to_datetime(daily["date"])
        daily         = daily.merge(dgs10_series, on="date", how="left")

        if daily.empty:
            raise DataValidationError(
                "DataFrame is empty after DGS10 merge — "
                "check that the DGS10 series covers the processing window."
            )

        # ── STEP 9 : Stamp pipeline lineage ─────────────────────────────
        print("\n  [STEP 9] Stamping pipeline lineage...")
        daily = _add_pipeline_metadata(daily, run_id, run_ts, mode)

        daily["date"] = pd.to_datetime(daily["date"]).dt.date
        df_output = daily[
            [c for c in LAYER2_OUTPUT_COLS if c in daily.columns]
        ].copy()

        # ── STEP 10 : Validate output ────────────────────────────────────
        df_output = validate_data(df_output)
        df_output = detect_anomalies(df_output)

        output_rows = len(df_output)
        print(f"\n  Output rows: {output_rows:,}")

        # ── STEP 11 : Write Layer 2 feature partitions ───────────────────
        print("\n  [STEP 11] Writing Layer 2 feature partitions...")
        partition_log = retry_with_backoff(
            lambda: write_layer2_features(df_output, bucket, run_id),
            retries=3,
            critical_name="Layer 2 feature partition write",
            run_id=run_id,
        )

        # ── STEP 12 : Update rolling serving parquet ─────────────────────
        # Replay also updates the rolling parquet so that the serving layer
        # reflects the recomputed feature values.
        print("\n  [STEP 12] Updating rolling serving layer...")
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

        # ── STEP 13 : Persist SUCCESS run metadata ───────────────────────
        processing_time = round(time.time() - pipeline_start, 2)
        _save_run_metadata(
            bucket=bucket, run_id=run_id, mode=mode,
            start_date=pd.Timestamp(start_str).date(), end_date=end_date,
            nrows=output_rows, run_ts=run_ts, airflow_metadata=airflow_metadata,
            status="SUCCESS", processing_time_s=processing_time,
            partition_count=len(partition_log), rolling_updated=True,
        )

        fill_summary = (
            df_output["fill_method_flag"].value_counts().to_dict()
            if "fill_method_flag" in df_output.columns else {}
        )

        print(f"\n{'=' * 66}")
        print(f"  BOND UPDATE PIPELINE SUCCESS")
        print(f"  run_id          : {run_id}")
        print(f"  mode            : {mode}")
        print(f"  rows produced   : {output_rows:,}")
        print(f"  date partitions : {len(partition_log)}")
        print(f"  window          : {start_str} -> {end_str}")
        print(f"  duration        : {processing_time}s")
        print(f"  fill_flags      : {fill_summary}")
        print(f"{'=' * 66}\n")

        return f"SUCCESS_{output_rows}_ROWS"

    except Exception as exc:
        processing_time = round(time.time() - pipeline_start, 2)

        send_alert(
            "Bond update pipeline failed with unhandled exception",
            level="CRITICAL",
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

        _save_run_metadata(
            bucket=bucket, run_id=run_id, mode=mode,
            start_date=pd.Timestamp(start_str).date() if start_str else None,
            end_date=end_date, nrows=0, run_ts=run_ts,
            airflow_metadata=airflow_metadata,
            status="FAILED", processing_time_s=processing_time, error=str(exc),
        )
        raise