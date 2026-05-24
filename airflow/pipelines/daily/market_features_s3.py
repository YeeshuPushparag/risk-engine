"""
market_features_pipeline.py
============================
Production-grade financial data pipeline — hedge-fund quality.

Storage layout
--------------
Layer 1 — Raw       (immutable, append-only, partitioned by date)
    s3://<bucket>/<prefix>raw/year=Y/month=MM/day=DD/data.parquet

Layer 2 — Features  (versioned per run_id, partitioned by date)
    s3://<bucket>/<prefix>features/year=Y/month=MM/day=DD/run_id=<id>/data.parquet
    s3://<bucket>/<prefix>features/latest.json   <- points to most recent run_id

Layer 3 — Rolling   (mutable serving layer, last N calendar days)
    s3://<bucket>/<prefix>rolling/market_features_30d.parquet

Support files
-------------
Lock     : s3://<bucket>/<prefix>locks/pipeline.lock
DLQ      : s3://<bucket>/<prefix>dlq/run_id=<id>/failed_tickers.json
Metadata : s3://<bucket>/<prefix>metadata/run_id=<id>/run_summary.json

Airflow integration notes
-------------------------
- DAG must set  max_active_runs=1  — S3 locking is an extra safety net,
  not a replacement for Airflow concurrency control.
- Recommended:  retries=2, retry_delay=timedelta(minutes=5)
- Backfill:     pass {"start_date": "2024-01-01"} via Airflow conf
- Raw replay:   pass {"replay_from_raw": true, "start_date": "..."} via conf
  Wire an S3 sensor on the metadata key; trigger when failed_tickers > 0.
- DLQ DAG must also set  max_active_runs=1.

Versioning
----------
- Bump CONFIG["feature_version"] when feature engineering logic changes.
- Bump SCHEMA_VERSION when RAW_SCHEMA columns or types change.
- SCHEMA_HASH is computed automatically from RAW_SCHEMA at import time.
"""

import hashlib
import uuid
import json
import time
import pandas as pd
import numpy as np
import yfinance as yf
import boto3
from botocore.exceptions import ClientError
from tqdm import tqdm
from datetime import datetime, timedelta, timezone, date as date_type
from io import BytesIO
import os
import requests

# =============================================================
# CONFIG  —  single source of truth.  No magic numbers in code.
# =============================================================

CONFIG: dict = {
    # Fetch
    "batch_size":              100,
    "max_retries":             3,
    "retry_backoff_s":         5,

    # Failure handling
    "failure_threshold":       0.1,

    # Feature engineering
    "risk_free_rate":          0.05,
    "buffer_days":             20,

    # Data quality
    "coverage_threshold":      0.9,
    "max_price_jump":          0.5,
    "min_volume":              0,

    # Anomaly detection thresholds
    "volume_anomaly_high":     3.0,
    "volume_anomaly_low":      0.1,
    "volatility_spike_ratio":  2.5,
    "coverage_drop_ratio":     0.8,

    # Rolling serving layer
    "window_days":             30,

    # Lineage
    "pipeline_name":           "market_features_s3",
    "feature_version":         "v1",
    "transformation":          "generate_market_features_v1",
    "data_source":             "yfinance",
}

# =============================================================
# SCHEMA DEFINITIONS
# =============================================================

SCHEMA_VERSION: str = "1.0"

RAW_SCHEMA: dict[str, str] = {
    "date":      "datetime64[ns, UTC]",
    "open":      "float64",
    "high":      "float64",
    "low":       "float64",
    "close":     "float64",
    "volume":    "float64",
    "ticker":    "object",
    "record_id": "object",
}

# ✅ FIXED FEATURE SCHEMA (includes metadata)
FEATURE_SCHEMA: dict[str, str] = {
    # core
    "date":            "datetime64[ns, UTC]",
    "open":            "float64",
    "high":            "float64",
    "low":             "float64",
    "close":           "float64",
    "volume":          "float64",
    "ticker":          "object",
    "record_id":       "object",

    # features
    "daily_return":    "float64",
    "log_return":      "float64",
    "high_low_spread": "float64",
    "close_open_diff": "float64",
    "vol_5d":          "float64",
    "vol_20d":         "float64",
    "volatility_21d":  "float64",
    "ma_5":            "float64",
    "ma_20":           "float64",
    "momentum_5d":     "float64",
    "momentum_20d":    "float64",
    "avg_volume_10d":  "float64",
    "vol_change":      "float64",
    "excess_return":   "float64",
    "downside_risk":   "float64",
    "sharpe_ratio":    "float64",
    "sortino_ratio":   "float64",
    "outlier_flag":    "bool",

    # metadata 
    "pipeline_name":      "object",
    "pipeline_run_id":    "object",
    "data_source":        "object",
    "input_source":       "object",
    "transformation":     "object",
    "record_created_at":  "datetime64[ns, UTC]",
    "data_date":          "object",
    "feature_version":    "object",
}

# Schema hash
SCHEMA_HASH: str = hashlib.md5(
    json.dumps(RAW_SCHEMA, sort_keys=True).encode()
).hexdigest()

FEATURE_COLS: list[str] = [
    "date", "open", "high", "low", "close", "volume", "ticker",
]

# =============================================================
# CUSTOM EXCEPTIONS
# =============================================================

class SchemaError(Exception):
    """Raised when data does not conform to a declared schema."""


class LockError(Exception):
    """Raised when the S3 pipeline lock is already held."""


class InputContractError(Exception):
    """Raised when the input tickers CSV fails validation."""

# =============================================================
# ALERTING  —  structured, level-tagged, context-rich
# =============================================================

def send_alert(
    message: str,
    level:   str  = "ERROR",
    context: dict = None,
) -> None:
    """
    Structured alert dispatcher.

    In production, replace the print stub with one or more of:
      - boto3 SNS:   sns.publish(TopicArn=..., Message=..., Subject=...)
      - Slack hook:  requests.post(SLACK_WEBHOOK_URL, json={"text": message})
      - PagerDuty:   requests.post(PD_URL, json={...})
      - Datadog:     statsd.event(title, message, alert_type=level.lower())

    Levels: DEBUG | INFO | WARNING | ERROR | CRITICAL
    """
    payload = {
        "level":     level,
        "pipeline":  CONFIG["pipeline_name"],
        "message":   message,
        "context":   context or {},
        "timestamp": utc_now().isoformat(),
    }
    # Structured stdout — parseable by any log aggregator (CloudWatch, Datadog, etc.)
    print(f"  [ALERT][{level}] {message} | context={json.dumps(payload['context'], default=str)}")

    # ── Production hook (uncomment and configure) ──────────────────
    if level in ["ERROR", "CRITICAL"]:
        try:
            webhook = os.getenv("SLACK_WEBHOOK_URL")
            if webhook:
                text = f"*[{level}]* {CONFIG['pipeline_name']}\n" \
                    f"message: {message}\n" \
                    f"context: {json.dumps(payload['context'], default=str)}\n" \
                    f"time: {payload['timestamp']}"
                requests.post(webhook, json={"text": text}, timeout=5)
        except Exception as e:
            print(f"[ALERT ERROR] Slack failed: {e}")

# =============================================================
# TIMEZONE HELPERS
# =============================================================

def utc_now() -> datetime:
    """Current datetime as a tz-aware UTC object."""
    return datetime.now(timezone.utc)


def to_utc_timestamp(value) -> pd.Timestamp:
    """Convert any date / datetime / string to a tz-aware UTC Timestamp."""
    return pd.to_datetime(value, utc=True)


def today_utc() -> date_type:
    """Today's date in UTC."""
    return utc_now().date()

# =============================================================
# S3 HELPERS — basic I/O
# =============================================================

def get_s3():
    """Return a boto3 S3 client. Called per-operation to avoid stale sessions."""
    return boto3.client("s3")


def s3_key_exists(bucket: str, key: str) -> bool:
    try:
        get_s3().head_object(Bucket=bucket, Key=key)
        return True
    except ClientError:
        return False


def read_csv_from_s3(bucket: str, key: str) -> pd.DataFrame:
    print(f"  [S3 READ CSV]     s3://{bucket}/{key}")
    obj = get_s3().get_object(Bucket=bucket, Key=key)
    return pd.read_csv(obj["Body"])


def read_parquet_from_s3(bucket: str, key: str) -> pd.DataFrame:
    print(f"  [S3 READ PARQUET] s3://{bucket}/{key}")
    obj = get_s3().get_object(Bucket=bucket, Key=key)
    return pd.read_parquet(BytesIO(obj["Body"].read()))


def write_json_to_s3(payload: dict, bucket: str, key: str) -> None:
    print(f"  [S3 WRITE JSON]   s3://{bucket}/{key}")
    get_s3().put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(payload, default=str, indent=2),
    )

# =============================================================
# ATOMIC WRITES  —  write->temp, copy->final, delete temp
# =============================================================

def atomic_write_parquet_to_s3(
    df:     pd.DataFrame,
    bucket: str,
    key:    str,
) -> None:
    """
    Atomically write a DataFrame as parquet to S3.

    Protocol:
      1. Serialize to an in-memory buffer.
      2. PUT to a temp key  (_temp/<key>).
      3. COPY temp -> final key   (S3-side, zero-copy for same bucket).
      4. DELETE temp key.

    If step 3 or 4 fails, the temp key is cleaned up in the finally block
    so no orphaned objects accumulate.  The final key is never partially
    written — it either exists in full or not at all.
    """
    temp_key = f"_temp/{key}"
    s3 = get_s3()

    # Serialize once
    buf = BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    raw_bytes = buf.getvalue()

    try:
        # Step 1: write to temp
        s3.put_object(Bucket=bucket, Key=temp_key, Body=raw_bytes)

        # Step 2: server-side copy to final destination
        s3.copy_object(
            Bucket=bucket,
            CopySource={"Bucket": bucket, "Key": temp_key},
            Key=key,
        )
        print(f"  [S3 ATOMIC WRITE] s3://{bucket}/{key}  rows={len(df):,}")

    except Exception as exc:
        send_alert(
            f"Atomic write failed for key={key}",
            level="ERROR",
            context={"key": key, "temp_key": temp_key, "error": str(exc)},
        )
        raise

    finally:
        # Step 3: always remove temp — even if copy failed
        try:
            s3.delete_object(Bucket=bucket, Key=temp_key)
        except Exception:
            pass  # temp cleanup failure is non-fatal

# =============================================================
# S3 CONCURRENCY LOCKING
# =============================================================

_LOCK_KEY_SUFFIX = "locks/pipeline.lock"


def acquire_s3_lock(bucket: str, prefix: str, run_id: str) -> None:
    """
    Create an S3 lock file to prevent concurrent pipeline execution.

    S3 does not offer true atomic test-and-set, but for batch pipelines
    that run on a schedule, this pattern is sufficient:
      - Check -> fail if lock exists (with stale-lock detection in prod)
      - Write lock payload immediately
      - Release in a try/finally block in the caller

    For true atomic locking, use DynamoDB conditional writes.
    Comment referencing production upgrade: replace with DynamoDB
    put_item with ConditionExpression="attribute_not_exists(lock_id)".
    """
    lock_key = prefix + _LOCK_KEY_SUFFIX

    if s3_key_exists(bucket, lock_key):
        try:
            raw  = get_s3().get_object(Bucket=bucket, Key=lock_key)["Body"].read()
            info = json.loads(raw)
            holder = info.get("run_id", "unknown")
            since  = info.get("acquired_at", "unknown")
        except Exception:
            holder, since = "unknown", "unknown"

        msg = f"Pipeline locked by run_id={holder} (since {since}). Aborting to prevent corruption."
        send_alert(msg, level="CRITICAL", context={"lock_key": lock_key, "held_by": holder})
        raise LockError(msg)

    write_json_to_s3(
        {"run_id": run_id, "acquired_at": utc_now().isoformat(), "pid": str(uuid.uuid4())},
        bucket,
        lock_key,
    )
    print(f"  [LOCK] acquired -> s3://{bucket}/{lock_key}")


def release_s3_lock(bucket: str, prefix: str) -> None:
    """Release the S3 lock. Called in the finally block — must never raise."""
    lock_key = prefix + _LOCK_KEY_SUFFIX
    try:
        get_s3().delete_object(Bucket=bucket, Key=lock_key)
        print(f"  [LOCK] released -> s3://{bucket}/{lock_key}")
    except Exception as exc:
        # Non-fatal — log and continue.  Stale lock will need manual cleanup.
        msg = f"Could not release S3 lock — manual cleanup required: s3://{bucket}/{lock_key}"
        send_alert(msg, level="WARNING", context={"error": str(exc)})

# =============================================================
# INPUT CONTRACT VALIDATION
# =============================================================

def validate_ticker_input(df: pd.DataFrame) -> list[str]:
    """
    Validate the tickers CSV before any expensive work begins.

    Contract:
    - Must contain a 'ticker' column.
    - Must have at least one non-null row.
    - Ticker values must be non-empty strings.

    Returns a cleaned, deduplicated, upper-cased ticker list.
    Raises InputContractError immediately on any violation.
    """
    required_cols = ["ticker"]
    missing_cols  = [c for c in required_cols if c not in df.columns]
    if missing_cols:
        msg = f"Tickers CSV missing required columns: {missing_cols}. Found: {list(df.columns)}"
        send_alert(msg, level="CRITICAL", context={"missing": missing_cols})
        raise InputContractError(msg)

    raw_tickers = df["ticker"].dropna().astype(str).str.strip()
    raw_tickers = raw_tickers[raw_tickers != ""]

    if raw_tickers.empty:
        msg = "Tickers CSV is empty — no valid ticker values found after cleaning."
        send_alert(msg, level="CRITICAL")
        raise InputContractError(msg)

    tickers = raw_tickers.str.upper().unique().tolist()

    # Sanity check: flag suspiciously long strings (likely data corruption)
    bad = [t for t in tickers if len(t) > 10]
    if bad:
        send_alert(
            f"{len(bad)} ticker(s) exceed 10 characters — possible data corruption.",
            level="WARNING",
            context={"examples": bad[:5]},
        )

    print(f"  [INPUT CONTRACT] OK — {len(tickers):,} unique tickers loaded.")
    return tickers

# =============================================================
# SCHEMA ENFORCEMENT  (intra-stage and inter-stage)
# =============================================================

def enforce_schema(
    df:      pd.DataFrame,
    schema:  dict[str, str],
    label:   str = "unknown",
) -> pd.DataFrame:
    """
    Validate and coerce df to match the declared schema.

    Rules:
    - All schema keys MUST be present -> SchemaError on any missing column.
    - Each column is cast to its declared dtype.
    - All cast failures are collected before raising — you see every problem
      in one error, not just the first.
    - Extra columns not in schema are preserved unchanged.
    - Never silently passes bad data.

    Args:
        df:     DataFrame to validate.
        schema: Dict of {column_name: expected_dtype}.
        label:  Human-readable stage name for error messages.
    """
    missing = [col for col in schema if col not in df.columns]
    if missing:
        msg = (
            f"[{label}] Schema violation — missing columns: {missing}. "
            f"Present: {list(df.columns)}"
        )
        send_alert(msg, level="ERROR", context={"stage": label, "missing_cols": missing})
        raise SchemaError(msg)

    df     = df.copy()
    errors = []

    for col, expected in schema.items():
        current = str(df[col].dtype)
        if current == expected:
            continue
        try:
            if expected == "datetime64[ns, UTC]":
                df[col] = pd.to_datetime(df[col], utc=True)
            elif expected == "float64":
                df[col] = pd.to_numeric(df[col], errors="raise").astype("float64")
            elif expected == "bool":
                df[col] = df[col].astype(bool)
            elif expected == "object":
                df[col] = df[col].astype(str)
            else:
                df[col] = df[col].astype(expected)
        except Exception as exc:
            errors.append(
                f"  col='{col}': cannot cast {current!r} -> {expected!r}: {exc}"
            )

    if errors:
        msg = f"[{label}] Schema enforcement failed:\n" + "\n".join(errors)
        send_alert(msg, level="ERROR", context={"stage": label})
        raise SchemaError(msg)

    print(f"  [SCHEMA][{label}] OK — {len(schema)} columns validated  "
          f"(version={SCHEMA_VERSION}, hash={SCHEMA_HASH[:8]})")
    return df

# =============================================================
# STAGE 1A — fetch_raw_data  (live yfinance mode)
# =============================================================

def fetch_raw_data(
    tickers:    list[str],
    start_date: date_type,
    end_date:   date_type,
    run_id:     str,
    run_ts:     datetime,
) -> tuple[pd.DataFrame, dict]:  # CHANGED: returns dict instead of list
    """
    Download OHLCV data from yfinance in batches with retry + backoff.

    Returns:
        (raw_df, failed_by_date)  # CHANGED: now returns dict per date
        failed_by_date: dict {date_str: [list of failed tickers]}

    Idempotency:
        record_id = "<TICKER>_<YYYY-MM-DD>" is deterministic — replaying the
        same date range always produces the same record_ids, enabling safe dedup.

    Timezone:
        All date values are UTC-aware after enforce_schema is called downstream.
    """
    new_records: list = []
    failed_tickers: list = []  # KEPT for backward compatibility
    failed_by_date: dict = {}  # ADDED: {date_str: [tickers]}

    # ADDED: Initialize failed_by_date for all dates in range
    current_date = start_date
    while current_date <= end_date:
        if current_date.weekday() < 5:  # weekdays only
            date_str = current_date.strftime("%Y-%m-%d")
            failed_by_date[date_str] = []
        current_date += timedelta(days=1)

    total_batches = (
        len(tickers) + CONFIG["batch_size"] - 1
    ) // CONFIG["batch_size"]

    for i in range(0, len(tickers), CONFIG["batch_size"]):

        batch = tickers[i : i + CONFIG["batch_size"]]
        batch_num = i // CONFIG["batch_size"] + 1
        data = None

        # =========================================================
        # DOWNLOAD WITH RETRIES
        # =========================================================

        for attempt in range(1, CONFIG["max_retries"] + 1):

            try:
                data = yf.download(
                    tickers=" ".join(batch),
                    start=str(start_date),
                    end=str(end_date + timedelta(days=1)),
                    interval="1d",
                    group_by="ticker",
                    auto_adjust=False,
                    threads=True,
                    progress=False,
                )

                if data is None or data.empty:
                    raise ValueError("empty response from yfinance")

                break

            except Exception as exc:

                print(
                    f"  [FETCH] batch {batch_num}/{total_batches} "
                    f"attempt {attempt}/{CONFIG['max_retries']} failed: {exc}"
                )

                if attempt < CONFIG["max_retries"]:
                    time.sleep(CONFIG["retry_backoff_s"] * attempt)

                else:
                    print(
                        f"  [FETCH] batch {batch_num} exhausted retries -> DLQ"
                    )
                    failed_tickers.extend(batch)
                    # ADDED: Mark all tickers in batch as failed for all dates
                    for t in batch:
                        for date_str in failed_by_date.keys():
                            if t not in failed_by_date[date_str]:
                                failed_by_date[date_str].append(t)

        # =========================================================
        # SKIP EMPTY RESPONSES
        # =========================================================

        if data is None or data.empty:
            continue

        # =========================================================
        # HANDLE YFINANCE RESPONSE SAFELY
        # =========================================================

        try:

            if isinstance(data.columns, pd.MultiIndex):
                returned_tickers = set(
                    data.columns.get_level_values(0)
                )
            else:
                # malformed / single ticker response
                returned_tickers = set()

        except Exception as exc:

            print(f"  [FETCH][COLUMN ERROR] {exc}")

            failed_tickers.extend(batch)
            # ADDED: Mark all tickers in batch as failed for all dates
            for t in batch:
                for date_str in failed_by_date.keys():
                    if t not in failed_by_date[date_str]:
                        failed_by_date[date_str].append(t)
            continue

        # =========================================================
        # DETECT MISSING TICKERS
        # =========================================================

        missing = [
            t for t in batch
            if t not in returned_tickers
        ]

        if missing:
            print(f"  [FETCH][MISSING] {missing}")
            failed_tickers.extend(missing)
            print(f"  [FETCH] Total failed so far: {len(set(failed_tickers))} unique tickers")
            # ADDED: Mark missing tickers as failed for all dates
            for t in missing:
                for date_str in failed_by_date.keys():
                    if t not in failed_by_date[date_str]:
                        failed_by_date[date_str].append(t)

        # =========================================================
        # PROCESS VALID TICKERS
        # =========================================================

        for t in batch:

            if t not in returned_tickers:
                continue

            try:

                sub = data[t].reset_index().rename(columns={
                    "Date": "date",
                    "Open": "open",
                    "High": "high",
                    "Low": "low",
                    "Close": "close",
                    "Volume": "volume",
                })
             
                if sub["close"].isna().all():
                    print(f"  [FETCH][EMPTY] {t} - no price data")
                    failed_tickers.append(t)
                    continue
             
                # defensive cleanup
                if "Adj Close" in sub.columns:
                    sub = sub.drop(columns=["Adj Close"])

                # keep only expected columns
                sub = sub[
                    [c for c in sub.columns if c in FEATURE_COLS]
                ]

                # skip corrupted empty frames
                if sub.empty:
                    failed_tickers.append(t)
                    # ADDED: Mark ticker as failed for all dates
                    for date_str in failed_by_date.keys():
                        if t not in failed_by_date[date_str]:
                            failed_by_date[date_str].append(t)
                    continue

                sub["ticker"] = t

                sub["record_id"] = (
                    t + "_" + sub["date"].astype(str)
                )

                sub["ingestion_ts"] = run_ts
                sub["pipeline_run_id"] = run_id

                # ADDED: Track which dates actually have data
                dates_with_data = set(sub["date"].dt.strftime("%Y-%m-%d"))

                # ADDED: Mark dates that are missing for this ticker
                for date_str in failed_by_date.keys():
                    if date_str not in dates_with_data:
                        if t not in failed_by_date[date_str]:
                            failed_by_date[date_str].append(t)

                new_records.append(sub)

            except Exception as exc:

                print(f"  [FETCH][PARSE ERROR] {t}: {exc}")

                failed_tickers.append(t)
                # ADDED: Mark ticker as failed for all dates
                for date_str in failed_by_date.keys():
                    if t not in failed_by_date[date_str]:
                        failed_by_date[date_str].append(t)

    # =============================================================
    # FINAL CONCAT
    # =============================================================

    if not new_records:
        # CHANGED: return empty dict instead of empty list
        return pd.DataFrame(), failed_by_date

    df = pd.concat(new_records, ignore_index=True)

    df["date"] = pd.to_datetime(
        df["date"],
        utc=True,
    )

    df = df[df["date"].dt.dayofweek < 5]

    df = df[
        df["date"] >= to_utc_timestamp(start_date)
    ]

    # CHANGED: return dict instead of list
    return df, failed_by_date

# =============================================================
# STAGE 1B — read_raw_layer  (replay_from_raw mode)
# =============================================================

def read_raw_layer(
    bucket:     str,
    prefix:     str,
    start_date: date_type,
    end_date:   date_type,
) -> pd.DataFrame:
    """
    Read raw partitions (Layer 1) for every weekday in [start_date, end_date].
    Used exclusively when replay_from_raw=True.

    Properties:
    - Deterministic: same inputs -> same output, always.
    - Idempotent: reading never modifies Layer 1 (append-only source).
    - Partial availability: missing partitions are warned, not aborted,
      allowing replay of a date range with intentional gaps.
    """
    frames:  list      = []
    current: date_type = start_date

    while current <= end_date:
        if current.weekday() < 5:   # weekdays only
            y, m, d = current.year, current.month, current.day
            key     = f"{prefix}raw/year={y}/month={m:02d}/day={d:02d}/data.parquet"

            if s3_key_exists(bucket, key):
                part = read_parquet_from_s3(bucket, key)
                frames.append(part)
            else:
                print(f"  [RAW READ][WARN] partition missing — skipping: {key}")
                send_alert(
                    f"Raw partition missing during replay: {key}",
                    level="WARNING",
                    context={"key": key, "date": str(current)},
                )
        current += timedelta(days=1)

    if not frames:
        return pd.DataFrame()

    combined         = pd.concat(frames, ignore_index=True)
    combined["date"] = pd.to_datetime(combined["date"], utc=True)

    # Rebuild record_id if absent (partitions predating this field)
    if "record_id" not in combined.columns:
        combined["record_id"] = (
            combined["ticker"].astype(str) + "_" + combined["date"].astype(str)
        )

    print(f"  [RAW READ] {len(frames)} partition(s), {len(combined):,} rows loaded.")
    return combined

# =============================================================
# STAGE 2 — validate_data  (DQ filtering + outlier flagging)
# =============================================================

def validate_data(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """
    Hard-filter structurally invalid rows. Returns (clean_df, dq_report).

    Outliers are FLAGGED in the outlier_flag column — NOT silently dropped —
    so downstream consumers can make their own exclusion decisions with full
    auditability.
    """
    before = len(df)

    df = df.dropna(subset=["ticker", "close", "date"])
    df = df[df["close"]  > 0]
    df = df[df["open"]   > 0]
    df = df[df["volume"] >= CONFIG["min_volume"]]

    dropped = before - len(df)

    # Soft outlier detection: flag extreme single-day price moves
    df = df.sort_values(["ticker", "date"]).copy()
    df["_prev_close"]  = df.groupby("ticker")["close"].shift(1)
    df["_pct_chg"]     = (df["close"] - df["_prev_close"]).abs() / df["_prev_close"]
    df["outlier_flag"] = (df["_pct_chg"] > CONFIG["max_price_jump"]).astype(bool)
    outlier_count      = int(df["outlier_flag"].sum())
    df = df.drop(columns=["_prev_close", "_pct_chg"])

    if outlier_count > 0:
        send_alert(
            f"{outlier_count} rows flagged as price outliers (>{CONFIG['max_price_jump']*100:.0f}% single-day move).",
            level="WARNING",
            context={"outlier_count": outlier_count, "rows_total": len(df)},
        )

    dq_report = {
        "rows_before":   before,
        "rows_after":    len(df),
        "rows_dropped":  dropped,
        "outlier_flags": outlier_count,
    }
    print(f"  [DQ]  {dq_report}")
    return df, dq_report

# =============================================================
# STAGE 3 — validate_sla
# =============================================================

def validate_sla(
    df:               pd.DataFrame,
    expected_tickers: int,
    run_date:         date_type,
) -> dict:
    """
    SLA gate — pipeline raises RuntimeError on FAIL.
    """
    result = {"status": "PASS", "checks": {}}
    errors = []

    # 1. Coverage check 
    actual   = df["ticker"].nunique()
    coverage = actual / expected_tickers if expected_tickers else 0
    result["checks"]["coverage"] = f"{actual}/{expected_tickers} ({coverage:.1%})"

    if coverage < CONFIG["coverage_threshold"]:
        msg = f"Low coverage: {coverage:.1%} < required {CONFIG['coverage_threshold']:.0%}"
        errors.append(msg)
        send_alert(msg, level="ERROR", context={"actual": actual, "expected": expected_tickers})

    # 2. Freshness check
    max_ts = df["date"].max()
    # expected latest trading day
    expected_date = run_date

    while expected_date.weekday() >= 5:
        expected_date -= timedelta(days=1)

    result["checks"]["max_date"] = str(max_ts)

    if pd.isna(max_ts) or max_ts.date() < expected_date:

        msg = (
            f"Stale data: latest={max_ts}, "
            f"expected >= {expected_date}"
        )

        errors.append(msg)

        send_alert(
            msg,
            level="ERROR",
            context={
                "max_date": str(max_ts),
                "expected_date": str(expected_date),
            },
        )

    # 3. Duplicate check (warn; deduplication happens at write time)
    dup_count = int(df.duplicated(["ticker", "date"]).sum())
    result["checks"]["duplicates"] = dup_count
    if dup_count > 0:
        result["checks"]["duplicate_note"] = "will be resolved at write time (keep=last)"

    if errors:
        result["status"] = "FAIL"
        result["errors"] = errors

    return result

# =============================================================
# STAGE 4 — detect_anomalies  (data drift vs rolling baseline)
# =============================================================

def detect_anomalies(
    new_features:   pd.DataFrame,
    old_rolling_df: pd.DataFrame | None,
) -> list[dict]:
    """
    Compare current run statistics against the rolling baseline.
    Returns a list of anomaly dicts. Each detected anomaly also triggers
    send_alert so it is surfaced immediately — not just logged.

    Checks:
    1. Volume anomaly     — current avg volume vs historical avg volume.
    2. Volatility spike   — current avg vol_20d vs historical avg vol_20d.
    3. Coverage drop      — current unique tickers vs historical unique tickers.
    """
    anomalies: list[dict] = []

    if old_rolling_df is None or old_rolling_df.empty:
        print("  [ANOMALY] no baseline available — skipping drift checks.")
        return anomalies

    # 1. Volume anomaly
    curr_vol_avg = float(new_features["volume"].mean())
    hist_vol_avg = float(old_rolling_df["volume"].mean())
    if hist_vol_avg > 0:
        ratio = curr_vol_avg / hist_vol_avg
        if ratio > CONFIG["volume_anomaly_high"] or ratio < CONFIG["volume_anomaly_low"]:
            record = {
                "type":       "volume_anomaly",
                "current":    round(curr_vol_avg, 2),
                "historical": round(hist_vol_avg, 2),
                "ratio":      round(ratio, 3),
            }
            anomalies.append(record)
            send_alert(
                f"Volume anomaly detected: current/historical ratio={ratio:.2f}",
                level="WARNING",
                context=record,
            )

    # 2. Volatility spike
    if "vol_20d" in new_features.columns and "vol_20d" in old_rolling_df.columns:
        curr_vix = float(new_features["vol_20d"].dropna().mean())
        hist_vix = float(old_rolling_df["vol_20d"].dropna().mean())
        if hist_vix > 0:
            ratio = curr_vix / hist_vix
            if ratio > CONFIG["volatility_spike_ratio"]:
                record = {
                    "type":              "volatility_spike",
                    "current_vol_20d":   round(curr_vix, 6),
                    "historical_vol_20d": round(hist_vix, 6),
                    "ratio":             round(ratio, 3),
                }
                anomalies.append(record)
                send_alert(
                    f"Volatility spike detected: vol_20d ratio={ratio:.2f}x historical",
                    level="WARNING",
                    context=record,
                )

    # 3. Coverage drop
    curr_tickers = new_features["ticker"].nunique()
    hist_tickers = old_rolling_df["ticker"].nunique()
    if hist_tickers > 0:
        ratio = curr_tickers / hist_tickers
        if ratio < CONFIG["coverage_drop_ratio"]:
            record = {
                "type":               "coverage_drop",
                "current_tickers":    curr_tickers,
                "historical_tickers": hist_tickers,
                "ratio":              round(ratio, 3),
            }
            anomalies.append(record)
            send_alert(
                f"Coverage drop: {curr_tickers} tickers vs {hist_tickers} historical ({ratio:.1%})",
                level="ERROR",
                context=record,
            )

    if not anomalies:
        print("  [ANOMALY] no anomalies detected.")
    else:
        print(f"  [ANOMALY] {len(anomalies)} anomaly/anomalies detected.")

    return anomalies

# =============================================================
# STAGE 5 — generate_features  (pure — no I/O, no side effects)
# =============================================================

def generate_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Pure feature transformation: OHLCV -> enriched feature set.

    Contract:
    - Input must be sorted by (ticker, date).
    - record_id and pipeline_run_id are preserved so every output row
      traces back to its raw source event.
    - Zero I/O and zero global state mutation. Safe to unit-test in isolation.
    - Caller must enforce FEATURE_SCHEMA on the output via enforce_schema.
    """
    rfr     = CONFIG["risk_free_rate"]
    results = []

    for ticker, sub in tqdm(df.groupby("ticker"), desc="  Features", ncols=90):
        sub = sub.copy().sort_values("date")

        sub["daily_return"]    = sub["close"].pct_change()
        sub["log_return"]      = np.log(sub["close"] / sub["close"].shift(1))
        sub["high_low_spread"] = (sub["high"] - sub["low"]) / sub["open"]
        sub["close_open_diff"] = (sub["close"] - sub["open"]) / sub["open"]

        sub["vol_5d"]          = (
            sub["daily_return"].rolling(5,  min_periods=2).std() * np.sqrt(252)
        )
        sub["vol_20d"]         = (
            sub["daily_return"].rolling(20, min_periods=5).std() * np.sqrt(252)
        )
        sub["volatility_21d"]  = sub["daily_return"].rolling(21, min_periods=5).std()

        sub["ma_5"]            = sub["close"].rolling(5,  min_periods=1).mean()
        sub["ma_20"]           = sub["close"].rolling(20, min_periods=1).mean()
        sub["momentum_5d"]     = sub["close"].pct_change(5)
        sub["momentum_20d"]    = sub["close"].pct_change(20)
        sub["avg_volume_10d"]  = sub["volume"].rolling(10, min_periods=1).mean()
        sub["vol_change"]      = sub["volume"].pct_change().fillna(0)
        sub["excess_return"]   = sub["daily_return"] - (rfr / 252)

        sub["downside_risk"]   = (
            sub["daily_return"]
            .where(sub["daily_return"] < 0)
            .rolling(21, min_periods=5).std()
        )
        sub["sharpe_ratio"]    = (
            sub["excess_return"].rolling(21, min_periods=5).mean()
            / sub["volatility_21d"]
        )
        sub["sortino_ratio"]   = (
            sub["excess_return"].rolling(21, min_periods=5).mean()
            / sub["downside_risk"]
        )

        sub["downside_risk"]  = sub["downside_risk"].fillna(0)
        sub["sortino_ratio"]  = sub["sortino_ratio"].fillna(0)

        # outlier_flag must survive into feature output for FEATURE_SCHEMA
        if "outlier_flag" not in sub.columns:
            sub["outlier_flag"] = False

        results.append(sub)

    out = pd.concat(results, ignore_index=True)
    out.replace([np.inf, -np.inf], np.nan, inplace=True)
    out.dropna(subset=["close"], inplace=True)
    return out

# =============================================================
# STAGE 6 — add_metadata  (lineage enrichment)
# =============================================================

def add_metadata(
    df:                   pd.DataFrame,
    run_id:               str,
    run_ts:               datetime,
    input_source:         str,
    ticker_universe_size: int,
    input_rows:           int,
    output_rows:          int,
    processing_time_s:    float,
    ingestion_start_date: date_type,
    replay_mode:          bool,
    mode_label:           str,
) -> pd.DataFrame:
    """
    Stamp every output row with complete lineage and observability fields.

    Guarantees:
    - pipeline_run_id  is identical across all rows in the same run.
    - record_created_at is identical across all rows in the same run.
    - record_id links each feature row back to its exact raw source event.
    - schema_version + schema_hash make schema drift detectable in any row.
    - replay_mode flag makes recomputed rows distinguishable from originals.
    """
    df = df.copy()

    # Core lineage (spec-required columns)
    df["pipeline_name"]        = CONFIG["pipeline_name"]
    df["pipeline_run_id"]      = run_id               # constant per run
    df["data_source"]          = CONFIG["data_source"]
    df["input_source"]         = input_source
    df["transformation"]       = CONFIG["transformation"]
    df["feature_version"]      = CONFIG["feature_version"]  # config-driven
    df["schema_version"]       = SCHEMA_VERSION
    df["schema_hash"]          = SCHEMA_HASH
    df["record_created_at"]    = run_ts               # constant per run, UTC-aware

    # Data dimension
    df["data_date"]            = df["date"].dt.date.astype(str)
    df["ingestion_start_date"] = str(ingestion_start_date)

    # Run observability
    df["ticker_universe_size"] = ticker_universe_size
    df["input_rows"]           = input_rows
    df["output_rows"]          = output_rows
    df["processing_time_s"]    = round(processing_time_s, 3)

    # Mode flags
    df["replay_mode"]          = replay_mode
    df["run_mode"]             = mode_label
    # record_id preserved from fetch / raw-read stage — NOT re-stamped here.
    # This is the link from feature row -> raw source event.

    return df

# =============================================================
# STAGE 7 — write_raw_layer  (Layer 1, immutable)
# =============================================================

def _partition_key_raw(prefix: str, dt: date_type) -> str:
    return f"{prefix}raw/year={dt.year}/month={dt.month:02d}/day={dt.day:02d}/data.parquet"


def write_raw_layer(
    df:              pd.DataFrame,
    bucket:          str,
    prefix:          str,
    force_overwrite: bool,
) -> dict[str, str]:
    """
    Layer 1: write immutable raw OHLCV partitions (partitioned by date).

    Normal run  -> skip existing partitions (idempotent, cost-efficient).
    Backfill    -> overwrite when force_overwrite=True.

    Uses atomic writes to prevent partial/corrupt partitions.
    Returns {date_str: action} audit log for the run summary.
    """
    partition_log: dict[str, str] = {}

    for dt, sub in df.groupby(df["date"].dt.date):
        key    = _partition_key_raw(prefix, dt)
        exists = s3_key_exists(bucket, key)

        if exists and not force_overwrite:
            print(f"  [RAW][SKIP]      existing partition, force_overwrite=False: {key}")
            partition_log[str(dt)] = "SKIPPED"
            continue

        action = "OVERWRITE" if exists else "NEW"
        print(f"  [RAW][{action}]  s3://{bucket}/{key}  rows={len(sub):,}")
        atomic_write_parquet_to_s3(sub.copy(), bucket, key)
        partition_log[str(dt)] = action

    return partition_log

# =============================================================
# STAGE 8 — write_feature_layer  (Layer 2, versioned by run_id)
# =============================================================

def _partition_key_feature(prefix: str, dt: date_type, run_id: str) -> str:
    return (
        f"{prefix}features/"
        f"year={dt.year}/month={dt.month:02d}/day={dt.day:02d}/"
        f"run_id={run_id}/data.parquet"
    )


def write_feature_layer(
    df:              pd.DataFrame,
    bucket:          str,
    prefix:          str,
    run_id:          str,
    force_overwrite: bool,
) -> dict[str, str]:
    """
    Layer 2: write feature partitions versioned by run_id.

    Path: features/year=Y/month=MM/day=DD/run_id=<run_id>/data.parquet

    Versioning guarantee:
    - Each run_id gets its own partition folder — re-runs do NOT silently
      overwrite history. A new run always produces a new folder.
    - force_overwrite=True allows replacing the same run_id's output
      (e.g. on a task retry within the same Airflow run).

    After writing all date partitions, writes a features/latest.json
    pointer so downstream consumers always know the freshest run_id.

    Uses atomic writes to prevent partial/corrupt partitions.
    Returns {date_str: action} audit log for the run summary.
    """
    partition_log: dict[str, str] = {}

    for dt, sub in df.groupby(df["date"].dt.date):
        key    = _partition_key_feature(prefix, dt, run_id)
        exists = s3_key_exists(bucket, key)

        if exists and not force_overwrite:
            print(f"  [FEAT][SKIP]     existing versioned partition: {key}")
            partition_log[str(dt)] = "SKIPPED"
            continue

        action = "OVERWRITE" if exists else "NEW"
        print(f"  [FEAT][{action}]  s3://{bucket}/{key}  rows={len(sub):,}")
        atomic_write_parquet_to_s3(sub.copy(), bucket, key)
        partition_log[str(dt)] = action

    # Update latest.json pointer
    latest_key = f"{prefix}features/latest.json"
    write_json_to_s3(
        {
            "run_id":        run_id,
            "updated_at":    utc_now().isoformat(),
            "feature_version": CONFIG["feature_version"],
            "schema_version":  SCHEMA_VERSION,
            "schema_hash":     SCHEMA_HASH,
            "dates_written":   list(partition_log.keys()),
        },
        bucket,
        latest_key,
    )

    return partition_log

# =============================================================
# STAGE 9 — write_rolling_layer  (Layer 3, mutable serving file)
# =============================================================

def read_rolling_layer(
    bucket:      str,
    rolling_key: str,
) -> tuple[pd.DataFrame | None, date_type | None]:
    """
    Layer 3 read: returns (df, last_date) or (None, None) if not yet created.
    Date column is normalized to UTC on read.
    """
    if not s3_key_exists(bucket, rolling_key):
        return None, None

    df         = read_parquet_from_s3(bucket, rolling_key)
    df["date"] = pd.to_datetime(df["date"], utc=True)
    return df, df["date"].max().date()


def write_rolling_layer(
    new_features:   pd.DataFrame,
    old_rolling_df: pd.DataFrame | None,
    today:          date_type,
    bucket:         str,
    rolling_key:    str,
) -> pd.DataFrame:
    """
    Layer 3 write: merge new features into the rolling serving file.

    Merge semantics:
    - New rows always win over old rows for the same (ticker, date) pair.
    - Rows older than window_days are trimmed.
    - Correct for incremental, backfill, and replay runs — because dedup
      is key-based, not positional.

    Uses atomic write to prevent corrupt rolling file on partial failure.
    """
    cutoff = to_utc_timestamp(today - timedelta(days=CONFIG["window_days"]))

    combined = (
        pd.concat([old_rolling_df, new_features], ignore_index=True)
        if old_rolling_df is not None
        else new_features.copy()
    )

    combined = combined[combined["date"] >= cutoff]
    combined = combined.drop_duplicates(["ticker", "date"], keep="last")
    combined = combined.sort_values(["ticker", "date"]).reset_index(drop=True)

    atomic_write_parquet_to_s3(combined, bucket, rolling_key)
    return combined

# =============================================================
# STAGE 10 — write_metadata  (run audit trail)
# =============================================================

def write_metadata(
    bucket:          str,
    meta_key:        str,
    status:          str,
    run_id:          str,
    run_ts:          datetime,
    mode_label:      str,
    force_overwrite: bool,
    start_date:      date_type,
    today:           date_type,
    input_source:    str,
    ticker_universe_size: int,
    tickers_actual:  int,
    failed_tickers:  list,
    failure_rate:    float,
    input_rows:      int,
    output_rows:     int,
    processing_time_s: float,
    dq_report:       dict,
    sla:             dict,
    anomalies:       list,
    raw_partition_log:  dict,
    feat_partition_log: dict,
    reason:          str = "",
) -> None:
    """Write the complete run summary JSON. Called on success AND failure."""
    payload = {
        # Identity
        "run_id":              run_id,
        "run_ts":              run_ts.isoformat(),
        "status":              status,
        "mode":                mode_label,
        "force_overwrite":     force_overwrite,
        "reason":              reason,

        # Date range
        "start_date":          str(start_date),
        "end_date":            str(today),

        # Lineage
        "input_source":        input_source,
        "feature_version":     CONFIG["feature_version"],
        "transformation":      CONFIG["transformation"],
        "schema_version":      SCHEMA_VERSION,
        "schema_hash":         SCHEMA_HASH,

        # Ticker universe
        "tickers_expected":    ticker_universe_size,
        "tickers_actual":      tickers_actual,
        "failed_tickers":      failed_tickers,
        "failure_rate":        round(failure_rate, 4),

        # Volume
        "input_rows":          input_rows,
        "output_rows":         output_rows,
        "processing_time_s":   processing_time_s,

        # Quality
        "dq":                  dq_report,
        "sla":                 sla,
        "anomalies":           anomalies,

        # Partition audit
        "raw_partitions":      raw_partition_log,
        "feature_partitions":  feat_partition_log,
    }
    write_json_to_s3(payload, bucket, meta_key)

# =============================================================
# DLQ WRITE HELPER
# =============================================================

def write_dlq(
    failed_by_date: dict,
    run_id:         str,
    bucket:         str,
    prefix:         str,
) -> None:
    """
    Persist failed tickers to the DLQ immediately after fetch.
    Written before any subsequent stage that could raise, so failed tickers
    are always recoverable even if the pipeline crashes later.
    """
    if not failed_by_date:
        return

    for date_str, failed_tickers in failed_by_date.items():
        if not failed_tickers:
            continue

        failed = sorted(set(failed_tickers))

        key = f"{prefix}dlq/run_id={run_id}/date={date_str}/failed_tickers.json"

        payload = {
            "run_id": run_id,
            "date": date_str,
            "failures": [
                {
                    "ticker": t,
                    "reason": "missing_from_yfinance",
                    "stage": "fetch",
                    "detected_at": utc_now().isoformat()
                }
                for t in failed
            ],
            "count": len(failed),
            "written_at": utc_now().isoformat()
        }

        write_json_to_s3(payload, bucket, key)
        print(f"  [DLQ] {len(failed)} ticker(s) for {date_str} -> s3://{bucket}/{key}")
    
# =============================================================
# MAIN PIPELINE  —  update_market_features
# =============================================================

def update_market_features(
    input_filename:      str        = "final_tickers.csv",
    bucket:              str        = "yeeshu-equity-bucket",
    prefix:              str        = "historical-equity/",
    start_date_override: str        = None,   # "YYYY-MM-DD" -> backfill mode
    replay_from_raw:     bool       = False,  # True -> skip yfinance, read Layer 1
    force_overwrite:     bool       = False,  # True -> overwrite existing partitions
) -> tuple[pd.DataFrame | None, str]:
    """
    Main pipeline entry point.

    Modes
    -----
    Incremental (default)
        Reads rolling file to find start_date. Fetches from yfinance.

    Backfill  (start_date_override set)
        start_date = override. Fetches from yfinance.
        Set force_overwrite=True to replace existing feature partitions.

    Replay from raw  (replay_from_raw=True)
        Reads Layer 1 raw partitions — does NOT call yfinance.
        Deterministic and reproducible. Always force_overwrite for features.


    Idempotency
    -----------
    - Feature partitions are versioned by run_id — no silent history overwrite.
    - Raw partitions skip existing keys unless force_overwrite=True.
    - record_id is deterministic: "<TICKER>_<YYYY-MM-DD>".

    Airflow integration
    -------------------
        # Backfill conf:  {"start_date": "2024-06-01"}
        # Replay conf:    {"replay_from_raw": true, "start_date": "2024-06-01"}
        #
        # DAG settings:
        #   max_active_runs = 1        (S3 lock is extra safety, not primary)
        #   retries         = 2
        #   retry_delay     = timedelta(minutes=5)
    """

    # ------------------------------------------------------------------
    # 0. INIT — all run-level constants generated once, never mutated
    # ------------------------------------------------------------------
    run_ts  = utc_now()
    run_id  = f"run_{run_ts.strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
    today   = today_utc()

    effective_overwrite   = force_overwrite or replay_from_raw

    mode_label = (
        "replay_from_raw" if replay_from_raw
        else "backfill"    if start_date_override
        else "incremental"
    )

    print(f"\n{'=' * 66}")
    print(f"  PIPELINE START   run_id={run_id}")
    send_alert(
        "Pipeline started",
        level="INFO",
        context={
            "run_id": run_id,
            "mode": mode_label
        }
    )
    print(f"  mode={mode_label}   force_overwrite={effective_overwrite}")
    print(f"{'=' * 66}\n")

    # S3 path constants
    input_key    = prefix + input_filename
    input_source = f"s3://{bucket}/{input_key}"
    rolling_key  = prefix + "rolling/market_features_30d.parquet"
    meta_key     = f"{prefix}metadata/run_id={run_id}/run_summary.json"

    # Mutable run-level accumulators (updated as stages complete)
    failed_tickers:     list[str] = []
    failure_rate:       float     = 0.0
    dq_report:          dict      = {}
    sla:                dict      = {}
    anomalies:          list      = []
    raw_partition_log:  dict      = {}
    feat_partition_log: dict      = {}
    clean_df:           pd.DataFrame = pd.DataFrame()
    new_features:       pd.DataFrame = pd.DataFrame()
    processing_time_s:  float     = 0.0
    start_date:         date_type = today

    # Write STARTED sentinel immediately — detectable on mid-run crash
    write_json_to_s3(
        {"run_id": run_id, "run_ts": run_ts.isoformat(), "status": "STARTED", "mode": mode_label},
        bucket,
        meta_key,
    )

    # ------------------------------------------------------------------
    # Acquire S3 lock — released in finally block no matter what
    # ------------------------------------------------------------------
    lock_acquired = False
    try:
        acquire_s3_lock(bucket, prefix, run_id)
        lock_acquired = True
    except LockError:
        raise   # propagate — caller / Airflow handles retry

    try:
        # --------------------------------------------------------------
        # STAGE 1 — Load + validate ticker universe
        # --------------------------------------------------------------
        print("[ STAGE 1 ] Load and validate ticker universe")
        raw_tickers_df       = read_csv_from_s3(bucket, input_key)
        tickers              = validate_ticker_input(raw_tickers_df)
        ticker_universe_size = len(tickers)

        print(f"  effective tickers: {len(tickers):,} of {ticker_universe_size:,}")

        # --------------------------------------------------------------
        # STAGE 2 — Determine date range
        # --------------------------------------------------------------
        print("\n[ STAGE 2 ] Determine date range")
        old_rolling_df, last_date = read_rolling_layer(bucket, rolling_key)

        if start_date_override:
            start_date = pd.to_datetime(start_date_override, utc=True).date()
            print(f"  override -> start_date={start_date}")
        elif last_date is not None:
            if last_date >= today:
                print("  [SKIP] Rolling file already up to date.")
                write_json_to_s3(
                    {"run_id": run_id, "status": "SKIPPED", "reason": "already_up_to_date"},
                    bucket,
                    meta_key,
                )
                return None, "Already updated"
            start_date = last_date + timedelta(days=1)
        else:
            start_date = today - timedelta(days=365)

        print(f"  date range: {start_date} -> {today}")

        # --------------------------------------------------------------
        # STAGE 3 — Acquire raw data  (fetch or replay)
        # --------------------------------------------------------------
        if replay_from_raw:
            print("\n[ STAGE 3 ] Read raw data from Layer 1 (replay mode)")
            raw_df = read_raw_layer(bucket, prefix, start_date, today)

            if raw_df.empty:
                msg = "Replay aborted — no raw partitions found for the requested range."
                send_alert(msg, level="ERROR", context={"start_date": str(start_date), "end_date": str(today)})
                write_metadata(
                    bucket=bucket, meta_key=meta_key, status="FAILED", run_id=run_id,
                    run_ts=run_ts, mode_label=mode_label, force_overwrite=effective_overwrite,
                    start_date=start_date, today=today, input_source=input_source,
                    ticker_universe_size=ticker_universe_size, tickers_actual=0,
                    failed_tickers=[], failure_rate=0.0, input_rows=0, output_rows=0,
                    processing_time_s=0.0, dq_report={}, sla={}, anomalies=[],
                    raw_partition_log={}, feat_partition_log={}, reason=msg,
                )
                return None, msg


        else:
            print("\n[ STAGE 3 ] Fetch raw data from yfinance")
            raw_df, failed_by_date = fetch_raw_data(
                tickers    = tickers,
                start_date = start_date,
                end_date   = today,
                run_id     = run_id,
                run_ts     = run_ts,
            )

            # DLQ — written before anything that could raise
            write_dlq(failed_by_date, run_id, bucket, prefix)
            
            # Remove duplicates (production safety)
            failed_tickers = list(set(sum(failed_by_date.values(), [])))

            # Alert if any failures happened
            if failed_tickers:
                send_alert(
                    f"{len(failed_tickers)} tickers failed during fetch",
                    level="WARNING",
                    context={
                        "run_id": run_id,
                        "failed_count": len(failed_tickers)
                    }
                )

            if raw_df.empty:
                msg = "No data fetched from yfinance."
                send_alert(msg, level="ERROR", context={"tickers": len(tickers)})
                write_metadata(
                    bucket=bucket, meta_key=meta_key, status="FAILED", run_id=run_id,
                    run_ts=run_ts, mode_label=mode_label, force_overwrite=effective_overwrite,
                    start_date=start_date, today=today, input_source=input_source,
                    ticker_universe_size=ticker_universe_size, tickers_actual=0,
                    failed_tickers=failed_tickers, failure_rate=1.0, input_rows=0,
                    output_rows=0, processing_time_s=0.0, dq_report={}, sla={},
                    anomalies=[], raw_partition_log={}, feat_partition_log={}, reason=msg,
                )
                return None, msg

            failure_rate = len(failed_tickers) / max(len(tickers), 1)
            if failure_rate > CONFIG["failure_threshold"]:
                msg = f"Fetch failure rate {failure_rate:.1%} exceeds threshold — aborting."
                send_alert(msg, level="CRITICAL", context={"failed": len(failed_tickers), "total": len(tickers)})
                write_metadata(
                    bucket=bucket, meta_key=meta_key, status="FAILED", run_id=run_id,
                    run_ts=run_ts, mode_label=mode_label, force_overwrite=effective_overwrite,
                    start_date=start_date, today=today, input_source=input_source,
                    ticker_universe_size=ticker_universe_size, tickers_actual=0,
                    failed_tickers=failed_tickers, failure_rate=failure_rate,
                    input_rows=0, output_rows=0, processing_time_s=0.0, dq_report={},
                    sla={}, anomalies=[], raw_partition_log={}, feat_partition_log={},
                    reason=msg,
                )
                raise RuntimeError(msg)

        # --------------------------------------------------------------
        # STAGE 4 — Schema enforcement on raw data
        # --------------------------------------------------------------
        print("\n[ STAGE 4 ] Schema enforcement — raw layer")
        try:
            raw_df = enforce_schema(raw_df, RAW_SCHEMA, label="raw")
        except SchemaError:
            write_metadata(
                bucket=bucket, meta_key=meta_key, status="FAILED", run_id=run_id,
                run_ts=run_ts, mode_label=mode_label, force_overwrite=effective_overwrite,
                start_date=start_date, today=today, input_source=input_source,
                ticker_universe_size=ticker_universe_size, tickers_actual=0,
                failed_tickers=failed_tickers, failure_rate=failure_rate,
                input_rows=0, output_rows=0, processing_time_s=0.0, dq_report={},
                sla={}, anomalies=[], raw_partition_log={}, feat_partition_log={},
                reason="Schema enforcement failed on raw data",
            )
            raise

        # --------------------------------------------------------------
        # STAGE 5 — Write raw partitions (Layer 1)
        #           Skipped in replay mode — Layer 1 is immutable source
        # --------------------------------------------------------------
        if not replay_from_raw:
            print("\n[ STAGE 5 ] Write raw partitions (Layer 1)")
            raw_partition_log = write_raw_layer(
                raw_df, bucket, prefix, force_overwrite=effective_overwrite
            )
        else:
            print("\n[ STAGE 5 ] Skipped — Layer 1 immutable in replay mode")

        # --------------------------------------------------------------
        # STAGE 6 — Data quality + SLA gate
        # --------------------------------------------------------------
        print("\n[ STAGE 6 ] Data quality + SLA validation")
        clean_df, dq_report = validate_data(raw_df)
        sla                  = validate_sla(
            clean_df,
            expected_tickers = ticker_universe_size,
            run_date         = today,
        )

        if sla["status"] == "FAIL":
            msg = f"SLA FAIL: {sla.get('errors')}"
            write_metadata(
                bucket=bucket, meta_key=meta_key, status="FAILED", run_id=run_id,
                run_ts=run_ts, mode_label=mode_label, force_overwrite=effective_overwrite,
                start_date=start_date, today=today, input_source=input_source,
                ticker_universe_size=ticker_universe_size,
                tickers_actual=clean_df["ticker"].nunique(),
                failed_tickers=failed_tickers, failure_rate=failure_rate,
                input_rows=len(clean_df), output_rows=0, processing_time_s=0.0,
                dq_report=dq_report, sla=sla, anomalies=[], raw_partition_log=raw_partition_log,
                feat_partition_log={}, reason=msg,
            )
            raise RuntimeError(msg)

        # --------------------------------------------------------------
        # STAGE 7 — Feature engineering
        #           Prepend rolling tail to warm rolling window calcs
        # --------------------------------------------------------------
        print("\n[ STAGE 7 ] Feature engineering")
        feat_start = time.time()

        if old_rolling_df is not None:

            cutoff_date = to_utc_timestamp(start_date)

            tail_df = (
                old_rolling_df[old_rolling_df["date"] < cutoff_date]
                .sort_values(["ticker", "date"])
                .groupby("ticker", group_keys=False)
                .tail(CONFIG["buffer_days"])
            )

        else:
            tail_df = pd.DataFrame(columns=FEATURE_COLS)
      
        feature_input = (
            pd.concat([tail_df, clean_df], ignore_index=True)
            .sort_values(["ticker", "date"])
            .reset_index(drop=True)
        )

        feature_output = generate_features(feature_input)

        # Strip warm-up tail rows — keep only genuinely new dates
        first_new_date = to_utc_timestamp(start_date)
        new_features   = feature_output[feature_output["date"] >= first_new_date].copy()

        processing_time_s = round(time.time() - feat_start, 3)
        print(f"  elapsed: {processing_time_s}s  |  output rows: {len(new_features):,}")

        # Carry record_id from raw -> feature (event-level traceability)
        id_map = clean_df.set_index(["ticker", "date"])["record_id"]
        idx = new_features.set_index(["ticker", "date"]).index
        mapped_ids = pd.Series(
            idx.map(id_map),
            index=new_features.index
        )

        new_features["record_id"] = (
            new_features["record_id"]
            .fillna(mapped_ids)
        )


        # --------------------------------------------------------------
        # STAGE 8 — Anomaly detection (vs rolling baseline)
        # --------------------------------------------------------------
        print("\n[ STAGE 8 ] Anomaly detection")
        anomalies = detect_anomalies(new_features, old_rolling_df)

        # --------------------------------------------------------------
        # STAGE 9 — Attach lineage metadata
        # --------------------------------------------------------------
        print("\n[ STAGE 9 ] Attach lineage metadata")
        new_features = add_metadata(
            df                   = new_features,
            run_id               = run_id,
            run_ts               = run_ts,
            input_source         = input_source,
            ticker_universe_size = ticker_universe_size,
            input_rows           = len(clean_df),
            output_rows          = len(new_features),
            processing_time_s    = processing_time_s,
            ingestion_start_date = start_date,
            replay_mode          = replay_from_raw,
            mode_label           = mode_label,
        )
        # --------------------------------------------------------------
        # STAGE 10 — Schema enforcement on feature output (inter-stage)
        # --------------------------------------------------------------
        print("\n[ STAGE 10 ] Schema enforcement — feature layer")
        try:
            new_features = enforce_schema(new_features, FEATURE_SCHEMA, label="features")
        except SchemaError:
            write_metadata(
                bucket=bucket, meta_key=meta_key, status="FAILED", run_id=run_id,
                run_ts=run_ts, mode_label=mode_label, force_overwrite=effective_overwrite,
                start_date=start_date, today=today, input_source=input_source,
                ticker_universe_size=ticker_universe_size,
                tickers_actual=clean_df["ticker"].nunique(),
                failed_tickers=failed_tickers, failure_rate=failure_rate,
                input_rows=len(clean_df), output_rows=len(new_features),
                processing_time_s=processing_time_s, dq_report=dq_report, sla=sla,
                anomalies=[], raw_partition_log=raw_partition_log,
                feat_partition_log={}, reason="Feature schema enforcement failed",
            )
            raise

        # --------------------------------------------------------------
        # STAGE 11 — Write feature partitions (Layer 2, versioned by run_id)
        # --------------------------------------------------------------
        print("\n[ STAGE 11 ] Write feature partitions (Layer 2)")
        feat_partition_log = write_feature_layer(
            new_features,
            bucket,
            prefix,
            run_id          = run_id,
            force_overwrite = effective_overwrite,
        )

        # --------------------------------------------------------------
        # STAGE 12 — Update rolling serving file (Layer 3)
        # --------------------------------------------------------------
        print("\n[ STAGE 12 ] Update rolling window (Layer 3)")
        rolling_df = write_rolling_layer(
            new_features   = new_features,
            old_rolling_df = old_rolling_df,
            today          = today,
            bucket         = bucket,
            rolling_key    = rolling_key,
        )

        # --------------------------------------------------------------
        # STAGE 13 — Write run summary (overwrites STARTED sentinel)
        # --------------------------------------------------------------
        print("\n[ STAGE 13 ] Write run summary")
        write_metadata(
            bucket=bucket, meta_key=meta_key, status="SUCCESS", run_id=run_id,
            run_ts=run_ts, mode_label=mode_label, force_overwrite=effective_overwrite,
            start_date=start_date, today=today, input_source=input_source,
            ticker_universe_size=ticker_universe_size,
            tickers_actual=clean_df["ticker"].nunique(),
            failed_tickers=failed_tickers, failure_rate=failure_rate,
            input_rows=len(clean_df), output_rows=len(new_features),
            processing_time_s=processing_time_s, dq_report=dq_report, sla=sla,
            anomalies=anomalies, raw_partition_log=raw_partition_log,
            feat_partition_log=feat_partition_log,
        )

        print(f"\n{'=' * 66}")
        print(f"  PIPELINE SUCCESS   run_id={run_id}")
        print(f"  output rows  : {len(new_features):,}")
        print(f"  rolling rows : {len(rolling_df):,}")
        print(f"  feature time : {processing_time_s}s")
        print(f"  anomalies    : {len(anomalies)}")
        print(f"{'=' * 66}\n")
        print("PIPELINE SUCCESS")
        send_alert(
            "Pipeline completed successfully",
            level="INFO",
            context={
                "run_id": run_id,
                "rows": len(new_features),
                "anomalies": len(anomalies),
                "mode": mode_label
            }
        )
        return rolling_df, "Success"

    except Exception as exc:
        # Best-effort failure metadata — may already be written above for specific errors
        send_alert(
            f"Pipeline failed: {exc}",
            level="CRITICAL",
            context={"run_id": run_id, "mode": mode_label, "error": str(exc)},
        )
        raise

    finally:
        # S3 lock is ALWAYS released — even on crash / KeyboardInterrupt
        if lock_acquired:
            release_s3_lock(bucket, prefix)


# =============================================================
# replay_features_from_raw  —  deterministic raw replay
# =============================================================

def replay_features_from_raw(
    start_date: str,
    bucket:     str  = "yeeshu-equity-bucket",
    prefix:     str  = "historical-equity/",
) -> tuple[pd.DataFrame | None, str]:
    """
    Recompute features from Layer 1 raw partitions for a given date range.
    Does NOT call yfinance. Deterministic and reproducible.

    Each replay produces a new run_id -> a new versioned feature partition.
    History is preserved. Rolling layer is updated with the recomputed values.

    Airflow conf:  {"start_date": "2024-01-01"}
    DAG setting:   max_active_runs=1
    """
    return update_market_features(
        bucket               = bucket,
        prefix               = prefix,
        start_date_override  = start_date,
        replay_from_raw      = True,
        force_overwrite      = True,
    )