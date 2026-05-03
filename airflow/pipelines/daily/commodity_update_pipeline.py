"""
commodity_pipeline.py
======================
Production-grade commodity ingestion pipeline — hedge-fund quality.
Mirrors the architecture of market_features_pipeline.py and
fx_exposure_pipeline.py exactly.

Storage layout
--------------
Layer 1 — Raw       (immutable, append-only, partitioned by date)
    s3://<bucket>/<prefix>raw/year=Y/month=MM/day=DD/data.parquet
    Contains: date, open, high, low, close, volume, commodity_symbol,
              record_id, data_quality_flag, ingestion_ts, pipeline_run_id

Layer 2 — Features  (versioned per run_id, partitioned by date)
    s3://<bucket>/<prefix>features/year=Y/month=MM/day=DD/run_id=<id>/data.parquet
    s3://<bucket>/<prefix>features/latest.json   ← pointer to most recent run_id

Layer 3 — Rolling   (mutable serving layer, last N calendar days)
    s3://<bucket>/<prefix>rolling/commodities_30d.parquet

Support files
-------------
Lock     : s3://<bucket>/<prefix>locks/pipeline.lock
DLQ      : s3://<bucket>/<prefix>dlq/run_id=<id>/failed_tickers.json
Metadata : s3://<bucket>/<prefix>metadata/run_id=<id>/run_summary.json

data_quality_flag
-----------------
Every row in every layer carries this field:
  "REAL"      — row sourced directly from yfinance
  "SYNTHETIC" — row generated from the 3-day mean of recent history
                when today's market data is unavailable
This ensures full auditability: downstream consumers can always
distinguish live data from imputed fills.

Airflow integration notes
-------------------------
- DAG must set  max_active_runs=1  — S3 locking adds an extra safety net.
- Recommended:  retries=2, retry_delay=timedelta(minutes=5)
- Backfill:     pass {"start_date": "2024-01-01"} via Airflow conf
- Raw replay:   pass {"replay_from_raw": true, "start_date": "..."} via conf
- DLQ replay:   trigger separate DAG -> replay_failed_tickers(run_id=...)
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
from datetime import datetime, timedelta, timezone, date as date_type
from io import BytesIO
import requests
import os

# =============================================================
# CONFIG  —  single source of truth.  No magic numbers in code.
# =============================================================

CONFIG: dict = {
    # Fetch
    "max_retries":             3,
    "retry_backoff_s":         5,       # seconds; multiplied by attempt number

    # Failure handling
    "failure_threshold":       0.5,     # >50% failed tickers -> abort + alert

    # Feature engineering
    "roll_window":             60,      # window for VaR and rolling vol calcs
    "buffer_days":             60,      # tail rows prepended to warm rolling calcs

    # Data quality
    "coverage_threshold":      0.8,     # <80% of expected tickers -> SLA FAIL
    "max_price_jump":          0.15,    # >15% single-day move -> outlier_flag
    "synthetic_history_days":  3,       # days of history used to build synthetic row

    # Anomaly detection thresholds (vs rolling baseline)
    "vol_spike_ratio":         2.5,     # current vol_20d / historical > 2.5x
    "volume_anomaly_high":     3.0,     # current avg volume / historical > 3x
    "coverage_drop_ratio":     0.8,     # current tickers / historical < 80%

    # Rolling serving layer
    "window_days":             30,

    # Lineage  ← bump feature_version when engineering logic changes
    "pipeline_name":           "commodity_update_pipeline",
    "feature_version":         "v1",
    "transformation":          "commodity_features_v1",
    "data_source":             "yfinance",
    "input_source":            "yfinance",
}

# =============================================================
# BUSINESS CONSTANTS  —  default commodity universe
# =============================================================

DEFAULT_TICKERS: list[str] = ["GC=F", "CL=F", "SI=F", "NG=F", "ZC=F"]

# Human-readable names for observability logs
TICKER_NAMES: dict[str, str] = {
    "GC=F": "Gold",
    "CL=F": "Crude Oil WTI",
    "SI=F": "Silver",
    "NG=F": "Natural Gas",
    "ZC=F": "Corn",
}

# Canonical columns flowing between ingestion and feature stages
RAW_COLS: list[str] = [
    "date", "open", "high", "low", "close", "volume",
    "commodity_symbol", "record_id", "data_quality_flag",
]

# =============================================================
# SCHEMA DEFINITIONS
# =============================================================

SCHEMA_VERSION: str = "1.0"

# Canonical dtype for every column after raw load.
# All datetimes are UTC-aware. data_quality_flag is always present.
RAW_SCHEMA: dict[str, str] = {
    "date":              "datetime64[ns, UTC]",
    "open":              "float64",
    "high":              "float64",
    "low":               "float64",
    "close":             "float64",
    "volume":            "float64",
    "commodity_symbol":  "object",
    "record_id":         "object",
    "data_quality_flag": "object",
}

# Schema expected after feature engineering — validated before any write.
FEATURE_SCHEMA: dict[str, str] = {
    # OHLCV + identity
    "date":              "datetime64[ns, UTC]",
    "open":              "float64",
    "high":              "float64",
    "low":               "float64",
    "close":             "float64",
    "volume":            "float64",
    "commodity_symbol":  "object",
    "record_id":         "object",
    "data_quality_flag": "object",
    # Computed features
    "daily_return":      "float64",
    "log_return":        "float64",
    "vol_20d":           "float64",
    "VaR_95":            "float64",
    "VaR_99":            "float64",
    "pnl":               "float64",
    "outlier_flag":      "bool",
    # Lineage — presence validated; values set by add_metadata()
    "pipeline_name":     "object",
    "pipeline_run_id":   "object",
    "data_source":       "object",
    "input_source":      "object",
    "transformation":    "object",
    "feature_version":   "object",
    "schema_version":    "object",
    "schema_hash":       "object",
    "record_created_at": "datetime64[ns, UTC]",
    "data_date":         "object",
    "replay_mode":       "bool",
    "partial_run":       "bool",
}

# Deterministic fingerprint of the raw schema — written to every output row
# so schema drift is detectable at the individual record level.
SCHEMA_HASH: str = hashlib.md5(
    json.dumps(RAW_SCHEMA, sort_keys=True).encode()
).hexdigest()

# =============================================================
# CUSTOM EXCEPTIONS
# =============================================================

class SchemaError(Exception):
    """Raised when data does not conform to a declared schema."""


class LockError(Exception):
    """Raised when the S3 pipeline lock is already held."""


class InputContractError(Exception):
    """Raised when the tickers list fails contract validation."""

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

    In production replace the print stub with:
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
    print(
        f"  [ALERT][{level}] {message} "
        f"| context={json.dumps(payload['context'], default=str)}"
    )
    # Production hook (uncomment and configure)
    if level in ["ERROR", "CRITICAL"]:
        try:
            webhook = os.getenv("SLACK_WEBHOOK_URL")
            if webhook:
                text = (
                    f"*[{level}]* {CONFIG['pipeline_name']}\n"
                    f"message: {message}\n"
                    f"context: {json.dumps(payload['context'], default=str)}\n"
                    f"time: {payload['timestamp']}"
                )
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
# S3 HELPERS  —  basic I/O
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
      3. Server-side COPY temp -> final key (zero bytes transferred).
      4. DELETE temp key.

    The final key is either complete or absent — never partially written.
    The finally block ensures temp cleanup even if the copy fails.
    """
    temp_key = f"_temp/{key}"
    s3       = get_s3()

    buf = BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    raw_bytes = buf.getvalue()

    try:
        s3.put_object(Bucket=bucket, Key=temp_key, Body=raw_bytes)
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
        try:
            s3.delete_object(Bucket=bucket, Key=temp_key)
        except Exception:
            pass

# =============================================================
# S3 CONCURRENCY LOCKING
# =============================================================

_LOCK_KEY_SUFFIX = "locks/pipeline.lock"


def acquire_s3_lock(bucket: str, prefix: str, run_id: str) -> None:
    """
    Create an S3 lock file to prevent concurrent pipeline execution.

    For true atomic locking in high-concurrency environments, replace with
    DynamoDB conditional writes:
      put_item with ConditionExpression="attribute_not_exists(lock_id)".
    """
    lock_key = prefix + _LOCK_KEY_SUFFIX

    if s3_key_exists(bucket, lock_key):
        try:
            raw    = get_s3().get_object(Bucket=bucket, Key=lock_key)["Body"].read()
            info   = json.loads(raw)
            holder = info.get("run_id", "unknown")
            since  = info.get("acquired_at", "unknown")
        except Exception:
            holder, since = "unknown", "unknown"

        msg = (
            f"Pipeline locked by run_id={holder} (since {since}). "
            "Aborting to prevent concurrent writes."
        )
        send_alert(msg, level="CRITICAL", context={"lock_key": lock_key, "held_by": holder})
        raise LockError(msg)

    write_json_to_s3(
        {"run_id": run_id, "acquired_at": utc_now().isoformat()},
        bucket,
        lock_key,
    )
    print(f"  [LOCK] acquired -> s3://{bucket}/{lock_key}")


def release_s3_lock(bucket: str, prefix: str) -> None:
    """Release the S3 lock. Called in finally block — must never raise."""
    lock_key = prefix + _LOCK_KEY_SUFFIX
    try:
        get_s3().delete_object(Bucket=bucket, Key=lock_key)
        print(f"  [LOCK] released -> s3://{bucket}/{lock_key}")
    except Exception as exc:
        send_alert(
            f"Could not release S3 lock — manual cleanup required: s3://{bucket}/{lock_key}",
            level="WARNING",
            context={"error": str(exc)},
        )

# =============================================================
# INPUT CONTRACT VALIDATION
# =============================================================

def validate_ticker_contract(tickers: list) -> list[str]:
    """
    Validate the commodity tickers list before any network calls.
    Raises InputContractError immediately on violation.

    Returns a deduplicated, stripped, upper-cased list of valid tickers.
    """
    if not tickers:
        msg = "Tickers list is empty — cannot run pipeline with no symbols."
        send_alert(msg, level="CRITICAL")
        raise InputContractError(msg)

    cleaned = list({str(t).strip().upper() for t in tickers if str(t).strip()})

    if not cleaned:
        msg = "Tickers list contains no valid non-empty symbols after cleaning."
        send_alert(msg, level="CRITICAL")
        raise InputContractError(msg)

    # Flag unusually long symbols as likely data corruption
    bad = [t for t in cleaned if len(t) > 10]
    if bad:
        send_alert(
            f"{len(bad)} ticker(s) exceed 10 characters — possible data error.",
            level="WARNING",
            context={"examples": bad[:5]},
        )

    print(f"  [INPUT CONTRACT] OK — {len(cleaned)} valid ticker(s).")
    return cleaned

# =============================================================
# SCHEMA ENFORCEMENT  —  intra-stage and inter-stage
# =============================================================

def enforce_schema(
    df:     pd.DataFrame,
    schema: dict[str, str],
    label:  str = "unknown",
) -> pd.DataFrame:
    """
    Validate and coerce df to match the declared schema.

    - All schema keys must be present -> SchemaError on any missing column.
    - Each column is cast to its declared dtype.
    - All cast failures are collected before raising — every broken column
      appears in one error message.
    - Extra columns not in schema are preserved unchanged.
    - Never silently passes bad data.
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

    print(
        f"  [SCHEMA][{label}] OK — {len(schema)} columns validated "
        f"(version={SCHEMA_VERSION}, hash={SCHEMA_HASH[:8]})"
    )
    return df

# =============================================================
# STAGE 1A — fetch_commodity_data  (yfinance, per-ticker retry)
# =============================================================

def fetch_commodity_data(
    tickers:    list[str],
    start_date: date_type,
    end_date:   date_type,
    run_id:     str,
    run_ts:     datetime,
) -> tuple[pd.DataFrame, list[str]]:
    """
    Download OHLCV data from yfinance per ticker with retry + backoff.

    All successfully downloaded rows are stamped data_quality_flag="REAL".
    Failed tickers are returned for DLQ writing.

    record_id = "<commodity_symbol>_<YYYY-MM-DD>" is deterministic so
    downstream dedup is safe across replays.

    Returns:
        (raw_df, failed_tickers)
    """
    records:        list = []
    failed_tickers: list = []

    for tkr in tickers:
        data = None

        for attempt in range(1, CONFIG["max_retries"] + 1):
            try:
                data = yf.download(
                    tickers  = tkr,
                    start    = str(start_date),
                    end      = str(end_date + timedelta(days=1)),
                    progress = False,
                )
                if data is None or data.empty:
                    raise ValueError(f"empty response for {tkr}")
                break

            except Exception as exc:
                print(
                    f"  [FETCH] {tkr} attempt {attempt}/{CONFIG['max_retries']} "
                    f"failed: {exc}"
                )
                if attempt < CONFIG["max_retries"]:
                    time.sleep(CONFIG["retry_backoff_s"] * attempt)
                else:
                    print(f"  [FETCH] {tkr} exhausted retries -> DLQ")
                    failed_tickers.append(tkr)

        if data is None or data.empty:
            continue

        # Normalise columns — yfinance may return multi-level or flat
        if isinstance(data.columns, pd.MultiIndex):
            data.columns = [c[0] for c in data.columns]

        data = data.reset_index()
        data.columns = [str(c).strip() for c in data.columns]

        rename_map = {
            "Date":   "date",
            "Open":   "open",
            "High":   "high",
            "Low":    "low",
            "Close":  "close",
            "Volume": "volume",
        }
        data = data.rename(columns=rename_map)

        # Keep only OHLCV + date; drop Adj Close if present
        keep_cols = [c for c in ["date", "open", "high", "low", "close", "volume"]
                     if c in data.columns]
        data = data[keep_cols].copy()

        data["commodity_symbol"]  = tkr
        data["data_quality_flag"] = "REAL"         # ← source of truth flag
        data["record_id"]         = tkr + "_" + data["date"].astype(str)
        data["ingestion_ts"]      = run_ts
        data["pipeline_run_id"]   = run_id

        records.append(data)

    if not records:
        return pd.DataFrame(), failed_tickers

    df         = pd.concat(records, ignore_index=True)
    df["date"] = pd.to_datetime(df["date"], utc=True)
    df         = df[df["date"].dt.dayofweek < 5]              # weekdays only
    df         = df[df["date"] >= to_utc_timestamp(start_date)]
    df         = df.dropna(subset=["close"])
    df         = df.sort_values(["commodity_symbol", "date"]).reset_index(drop=True)

    return df, failed_tickers


# =============================================================
# STAGE 1B — read_raw_layer  (replay_from_raw mode)
# =============================================================

def read_raw_layer(
    bucket:           str,
    prefix:           str,
    start_date:       date_type,
    end_date:         date_type,
    tickers_override: list[str] = None,
) -> pd.DataFrame:
    """
    Read raw partitions (Layer 1) for every weekday in [start_date, end_date].
    Used exclusively when replay_from_raw=True.

    Raw partitions contain both REAL and SYNTHETIC rows (data_quality_flag),
    so replay is fully self-contained and deterministic.

    Missing partitions are warned but not fatal — partial replay is valid.
    """
    frames:  list      = []
    current: date_type = start_date

    while current <= end_date:
        if current.weekday() < 5:
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

    if tickers_override:
        combined = combined[combined["commodity_symbol"].isin(set(tickers_override))]

    # Rebuild record_id if absent (partitions predating this field)
    if "record_id" not in combined.columns:
        combined["record_id"] = (
            combined["commodity_symbol"] + "_" + combined["date"].astype(str)
        )

    # Ensure data_quality_flag is present (partitions predating this field)
    if "data_quality_flag" not in combined.columns:
        combined["data_quality_flag"] = "REAL"

    print(f"  [RAW READ] {len(frames)} partition(s), {len(combined):,} rows loaded.")
    return combined


# =============================================================
# SYNTHETIC ROW BUILDER  —  fills today when market data unavailable
# =============================================================

def build_synthetic_rows(
    fetched_df:   pd.DataFrame,
    old_rolling:  pd.DataFrame | None,
    tickers:      list[str],
    today:        date_type,
    run_id:       str,
    run_ts:       datetime,
) -> pd.DataFrame:
    """
    For every ticker where today's row is absent in fetched_df, generate
    a synthetic row using the mean of the last CONFIG["synthetic_history_days"]
    rows from the rolling layer (existing history).

    Synthetic rows are stamped data_quality_flag="SYNTHETIC" to ensure
    they are always distinguishable from real market data.

    This exactly mirrors the original pipeline logic: if today's data is
    missing, fill with the last-3-day average from existing history.

    Returns a DataFrame of synthetic rows only (may be empty if all tickers
    have real data or insufficient history to synthesize).
    """
    if old_rolling is None or old_rolling.empty:
        return pd.DataFrame()

    synth_records: list = []
    today_ts           = to_utc_timestamp(today)
    history_n          = CONFIG["synthetic_history_days"]

    for tkr in tickers:
        # Check if today is already present in fetched data for this ticker
        tkr_today = fetched_df[
            (fetched_df["commodity_symbol"] == tkr) &
            (fetched_df["date"].dt.date == today)
        ] if not fetched_df.empty else pd.DataFrame()

        if not tkr_today.empty:
            continue  # real data exists — no synthetic needed

        # Look up recent history from rolling layer
        prev = (
            old_rolling[old_rolling["commodity_symbol"] == tkr]
            .sort_values("date")
            .tail(history_n)
        )

        if prev.empty:
            send_alert(
                f"Insufficient history to synthesize today's row for {tkr} — skipping.",
                level="WARNING",
                context={"ticker": tkr, "today": str(today)},
            )
            continue

        avg_row = {
            "date":              today_ts,
            "open":              round(float(prev["open"].mean()),   2),
            "high":              round(float(prev["high"].mean()),   2),
            "low":               round(float(prev["low"].mean()),    2),
            "close":             round(float(prev["close"].mean()),  2),
            "volume":            round(float(prev["volume"].mean()), 2),
            "commodity_symbol":  tkr,
            "data_quality_flag": "SYNTHETIC",   # ← explicit imputation flag
            "record_id":         tkr + "_" + str(today),
            "ingestion_ts":      run_ts,
            "pipeline_run_id":   run_id,
        }
        synth_records.append(avg_row)
        print(
            f"  [SYNTHETIC] {TICKER_NAMES.get(tkr, tkr)}: "
            f"today's data missing — filled with {history_n}-day average."
        )

    if not synth_records:
        return pd.DataFrame()

    synth_df         = pd.DataFrame(synth_records)
    synth_df["date"] = pd.to_datetime(synth_df["date"], utc=True)
    return synth_df


# =============================================================
# STAGE 2 — validate_data  (DQ filtering + outlier flagging)
# =============================================================

def validate_data(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """
    Hard-filter structurally invalid rows. Returns (clean_df, dq_report).

    REAL rows: full OHLCV validation applied.
    SYNTHETIC rows: only close > 0 is enforced (no volume check, no prev_close).

    Outliers are FLAGGED in outlier_flag — NOT silently dropped — so
    downstream consumers can make their own exclusion decisions.
    """
    before = len(df)

    df = df.dropna(subset=["date", "commodity_symbol", "close"])
    df = df[df["close"] > 0]
    df = df[df["open"]  > 0]

    dropped = before - len(df)

    # Soft outlier detection — only on REAL rows (synthetic are already averaged)
    df = df.sort_values(["commodity_symbol", "date"]).copy()
    real_mask          = df["data_quality_flag"] == "REAL"
    df["_prev_close"]  = df.groupby("commodity_symbol")["close"].shift(1)
    df["_pct_chg"]     = (df["close"] - df["_prev_close"]).abs() / df["_prev_close"]
    df["outlier_flag"] = (real_mask & (df["_pct_chg"] > CONFIG["max_price_jump"])).astype(bool)
    outlier_count      = int(df["outlier_flag"].sum())
    df = df.drop(columns=["_prev_close", "_pct_chg"])

    if outlier_count > 0:
        send_alert(
            f"{outlier_count} row(s) flagged as price outliers "
            f"(>{CONFIG['max_price_jump']*100:.0f}% single-day move).",
            level="WARNING",
            context={"outlier_count": outlier_count},
        )

    dq_report = {
        "rows_before":    before,
        "rows_after":     len(df),
        "rows_dropped":   dropped,
        "outlier_flags":  outlier_count,
        "synthetic_rows": int((df["data_quality_flag"] == "SYNTHETIC").sum()),
        "real_rows":      int((df["data_quality_flag"] == "REAL").sum()),
    }
    print(f"  [DQ] {dq_report}")
    return df, dq_report


# =============================================================
# STAGE 3 — validate_sla
# =============================================================

def validate_sla(
    df:               pd.DataFrame,
    expected_tickers: int,
    run_date:         date_type,
    is_partial_run:   bool = False,
) -> dict:
    """
    SLA gate. Pipeline raises RuntimeError on FAIL.
    is_partial_run=True relaxes the coverage check for DLQ replay.
    """
    result = {"status": "PASS", "checks": {}}
    errors = []

    # 1. Coverage
    actual   = df["commodity_symbol"].nunique()
    coverage = actual / expected_tickers if expected_tickers else 0
    result["checks"]["coverage"] = f"{actual}/{expected_tickers} ({coverage:.1%})"
    if not is_partial_run and coverage < CONFIG["coverage_threshold"]:
        msg = f"Low ticker coverage: {coverage:.1%} < required {CONFIG['coverage_threshold']:.0%}"
        errors.append(msg)
        send_alert(msg, level="ERROR", context={"actual": actual, "expected": expected_tickers})

    # 2. Freshness — today must be present (REAL or SYNTHETIC)
    max_ts = df["date"].max()
    result["checks"]["max_date"] = str(max_ts)
    if pd.isna(max_ts) or max_ts.date() < run_date:
        msg = f"Stale data: latest={max_ts}, expected >= {run_date}"
        errors.append(msg)
        send_alert(msg, level="ERROR", context={"max_date": str(max_ts), "run_date": str(run_date)})

    # 3. Duplicates (warn; resolved at write time)
    dup_count = int(df.duplicated(["commodity_symbol", "date"]).sum())
    result["checks"]["duplicates"] = dup_count
    if dup_count > 0:
        result["checks"]["duplicate_note"] = "will be resolved at write time (keep=last)"

    if errors:
        result["status"] = "FAIL"
        result["errors"] = errors

    return result


# =============================================================
# STAGE 4 — detect_anomalies  (drift vs rolling baseline)
# =============================================================

def detect_anomalies(
    new_features:   pd.DataFrame,
    old_rolling_df: pd.DataFrame | None,
) -> list[dict]:
    """
    Compare current run statistics against the rolling baseline.
    Each detected anomaly calls send_alert immediately.

    Checks:
    1. Volatility spike   — current vol_20d vs historical vol_20d.
    2. Volume anomaly     — current avg volume vs historical avg volume.
    3. Ticker coverage drop — current symbols vs historical symbols.
    """
    anomalies: list[dict] = []

    if old_rolling_df is None or old_rolling_df.empty:
        print("  [ANOMALY] no baseline available — skipping drift checks.")
        return anomalies

    # 1. Volatility spike (only on REAL rows to avoid synthetic distortion)
    if "vol_20d" in new_features.columns and "vol_20d" in old_rolling_df.columns:
        real_mask = new_features["data_quality_flag"] == "REAL"
        curr_vol  = float(new_features.loc[real_mask, "vol_20d"].dropna().mean())
        hist_vol  = float(old_rolling_df["vol_20d"].dropna().mean())
        if hist_vol > 0:
            ratio = curr_vol / hist_vol
            if ratio > CONFIG["vol_spike_ratio"]:
                record = {
                    "type":         "volatility_spike",
                    "current_vol":  round(curr_vol, 6),
                    "historical_vol": round(hist_vol, 6),
                    "ratio":        round(ratio, 3),
                }
                anomalies.append(record)
                send_alert(
                    f"Volatility spike: vol_20d ratio={ratio:.2f}x historical baseline",
                    level="WARNING",
                    context=record,
                )

    # 2. Volume anomaly
    curr_vol_avg = float(new_features["volume"].mean())
    hist_vol_avg = float(old_rolling_df["volume"].mean())
    if hist_vol_avg > 0:
        ratio = curr_vol_avg / hist_vol_avg
        if ratio > CONFIG["volume_anomaly_high"] or ratio < (1 / CONFIG["volume_anomaly_high"]):
            record = {
                "type":            "volume_anomaly",
                "current_avg":     round(curr_vol_avg, 2),
                "historical_avg":  round(hist_vol_avg, 2),
                "ratio":           round(ratio, 3),
            }
            anomalies.append(record)
            send_alert(
                f"Volume anomaly: current/historical ratio={ratio:.2f}x",
                level="WARNING",
                context=record,
            )

    # 3. Ticker coverage drop
    curr_syms = new_features["commodity_symbol"].nunique()
    hist_syms = old_rolling_df["commodity_symbol"].nunique()
    if hist_syms > 0:
        ratio = curr_syms / hist_syms
        if ratio < CONFIG["coverage_drop_ratio"]:
            record = {
                "type":              "coverage_drop",
                "current_symbols":   curr_syms,
                "historical_symbols": hist_syms,
                "ratio":             round(ratio, 3),
            }
            anomalies.append(record)
            send_alert(
                f"Coverage drop: {curr_syms} symbols vs {hist_syms} historical ({ratio:.1%})",
                level="ERROR",
                context=record,
            )

    if not anomalies:
        print("  [ANOMALY] no anomalies detected.")
    else:
        print(f"  [ANOMALY] {len(anomalies)} anomaly/anomalies detected.")

    return anomalies


# =============================================================
# STAGE 5 — generate_commodity_features  (pure — no I/O)
# =============================================================

def generate_commodity_features(
    clean_df:    pd.DataFrame,
    old_rolling: pd.DataFrame | None,
) -> pd.DataFrame:
    """
    Pure feature transformation: OHLCV -> enriched feature set.

    Preserves the original pipeline logic exactly:
      - daily_return  = close.pct_change()
      - log_return    = log(close / close.shift(1))
      - vol_20d       = 20-day rolling std of daily_return × sqrt(252)
      - VaR_95        = roll_window rolling 5th percentile of daily_return
      - VaR_99        = roll_window rolling 1st percentile of daily_return
      - pnl           = close.diff()

    Rolling window logic:
      - For each ticker, the last CONFIG["buffer_days"] rows from
        old_rolling are prepended to warm up the rolling calculations.
      - These warm-up rows are stripped from the output — only genuinely
        new dates are written to Layer 2 and Layer 3.

    data_quality_flag is preserved throughout so SYNTHETIC rows are
    traceable in the feature layer.

    Zero I/O. Zero global state mutation. Safe to unit-test in isolation.
    """
    roll_window = CONFIG["roll_window"]
    buffer_days = CONFIG["buffer_days"]
    results:    list = []

    for tkr, sub_new in clean_df.groupby("commodity_symbol"):

        # Prepend rolling tail to warm up rolling calculations
        if old_rolling is not None and not old_rolling.empty:
            prev_rows = (
                old_rolling[
                    (old_rolling["commodity_symbol"] == tkr) &
                    (old_rolling["date"] < sub_new["date"].min())
                ]
                .sort_values("date")
                .tail(buffer_days)
            )
            combined = pd.concat(
                [prev_rows[RAW_COLS], sub_new[RAW_COLS]],
                ignore_index=True,
            )
        else:
            combined = sub_new[RAW_COLS].copy()

        combined = combined.sort_values("date").drop_duplicates(
            subset=["commodity_symbol", "date"], keep="last"
        ).reset_index(drop=True)

        # ── Feature engineering (original logic preserved exactly) ──────
        combined["daily_return"] = combined["close"].pct_change()

        combined["log_return"]   = np.log(
            combined["close"] / combined["close"].shift(1)
        )

        combined["vol_20d"] = (
            combined["daily_return"].rolling(20).std() * np.sqrt(252)
        )

        combined["VaR_95"] = (
            combined["daily_return"].rolling(roll_window).quantile(0.05)
        )

        combined["VaR_99"] = (
            combined["daily_return"].rolling(roll_window).quantile(0.01)
        )

        combined["pnl"] = combined["close"].diff()

        # Strip warm-up rows — only keep genuinely new dates
        first_new_date = sub_new["date"].min()
        combined = combined[combined["date"] >= first_new_date].copy()

        results.append(combined)

    if not results:
        return pd.DataFrame()

    out = pd.concat(results, ignore_index=True)
    out.replace([np.inf, -np.inf], np.nan, inplace=True)
    out = out.dropna(subset=["daily_return"])
    return out


# =============================================================
# STAGE 6 — add_metadata  (lineage enrichment)
# =============================================================

def add_metadata(
    df:               pd.DataFrame,
    run_id:           str,
    run_ts:           datetime,
    input_source:     str,
    expected_tickers: int,
    input_rows:       int,
    output_rows:      int,
    processing_time_s: float,
    start_date,
    replay_mode:      bool,
    is_partial_run:   bool,
) -> pd.DataFrame:
    """
    Stamp every output row with complete lineage and observability fields.

    Guarantees:
    - pipeline_run_id and record_created_at are identical across all rows
      in the same run.
    - record_id links each feature row back to its raw source event.
    - schema_version + schema_hash make schema drift detectable per row.
    - data_quality_flag preserved from raw — REAL vs SYNTHETIC traceable
      all the way through to the feature layer.
    - replay_mode flag distinguishes recomputed from live-ingested rows.
    """
    df = df.copy()

    # Core lineage
    df["pipeline_name"]      = CONFIG["pipeline_name"]
    df["pipeline_run_id"]    = run_id                       # constant per run
    df["data_source"]        = CONFIG["data_source"]
    df["input_source"]       = input_source
    df["transformation"]     = CONFIG["transformation"]
    df["feature_version"]    = CONFIG["feature_version"]    # config-driven
    df["schema_version"]     = SCHEMA_VERSION
    df["schema_hash"]        = SCHEMA_HASH
    df["record_created_at"]  = run_ts                       # constant per run, UTC-aware

    # Data dimension
    df["data_date"]          = df["date"].dt.date.astype(str)
    df["ingestion_start_date"] = str(start_date)

    # Run observability
    df["ticker_universe_size"] = expected_tickers
    df["input_rows"]           = input_rows
    df["output_rows"]          = output_rows
    df["processing_time_s"]    = round(processing_time_s, 3)

    # Mode flags
    df["replay_mode"]    = replay_mode
    df["partial_run"]    = is_partial_run

    # data_quality_flag and record_id are preserved from earlier stages —
    # not re-stamped here.

    return df


# =============================================================
# WRITE HELPERS — Layer 1 (raw, immutable)
# =============================================================

def _raw_partition_key(prefix: str, dt: date_type) -> str:
    return f"{prefix}raw/year={dt.year}/month={dt.month:02d}/day={dt.day:02d}/data.parquet"


def write_raw_layer(
    df:              pd.DataFrame,
    bucket:          str,
    prefix:          str,
    force_overwrite: bool,
) -> dict[str, str]:
    """
    Layer 1: write immutable raw OHLCV partitions by date.

    Includes data_quality_flag so replay knows which rows were synthetic.
    Normal run  -> skip existing partitions (idempotent, cost-efficient).
    Backfill    -> overwrite when force_overwrite=True.

    Uses atomic writes to prevent partial/corrupt partitions.
    Returns {date_str: action} audit log for the run summary.
    """
    partition_log: dict[str, str] = {}

    for dt, sub in df.groupby(df["date"].dt.date):
        key    = _raw_partition_key(prefix, dt)
        exists = s3_key_exists(bucket, key)

        if exists and not force_overwrite:
            print(f"  [RAW][SKIP]      partition exists, force_overwrite=False: {key}")
            partition_log[str(dt)] = "SKIPPED"
            continue

        action = "OVERWRITE" if exists else "NEW"
        print(f"  [RAW][{action}]  s3://{bucket}/{key}  rows={len(sub):,}")
        atomic_write_parquet_to_s3(sub.copy(), bucket, key)
        partition_log[str(dt)] = action

    return partition_log


# =============================================================
# WRITE HELPERS — Layer 2 (features, versioned by run_id)
# =============================================================

def _feature_partition_key(prefix: str, dt: date_type, run_id: str) -> str:
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

    Versioning guarantee:
    - Each run_id gets its own partition folder — re-runs do NOT silently
      overwrite history.
    - force_overwrite=True replaces the same run_id's output (Airflow retry).

    Updates features/latest.json pointer after writing all partitions.
    Uses atomic writes to prevent partial/corrupt partitions.
    Returns {date_str: action} audit log for the run summary.
    """
    partition_log: dict[str, str] = {}

    for dt, sub in df.groupby(df["date"].dt.date):
        key    = _feature_partition_key(prefix, dt, run_id)
        exists = s3_key_exists(bucket, key)

        if exists and not force_overwrite:
            print(f"  [FEAT][SKIP]     existing versioned partition: {key}")
            partition_log[str(dt)] = "SKIPPED"
            continue

        action = "OVERWRITE" if exists else "NEW"
        print(f"  [FEAT][{action}]  s3://{bucket}/{key}  rows={len(sub):,}")
        atomic_write_parquet_to_s3(sub.copy(), bucket, key)
        partition_log[str(dt)] = action

    # Update latest.json pointer — downstream consumers find the freshest run
    latest_key = f"{prefix}features/latest.json"
    write_json_to_s3(
        {
            "run_id":          run_id,
            "updated_at":      utc_now().isoformat(),
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
# WRITE HELPERS — Layer 3 (rolling serving file)
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
    - New rows win over old rows for the same (commodity_symbol, date) pair.
    - Rows older than window_days are trimmed.
    - Correct for incremental, backfill, and replay runs — dedup is
      key-based, not positional.
    - data_quality_flag is preserved so consumers know which rows are
      SYNTHETIC vs REAL in the serving layer.

    Uses atomic write to prevent corrupt rolling file on partial failure.
    """
    cutoff = to_utc_timestamp(today - timedelta(days=CONFIG["window_days"]))

    combined = (
        pd.concat([old_rolling_df, new_features], ignore_index=True)
        if old_rolling_df is not None
        else new_features.copy()
    )

    combined = combined[combined["date"] >= cutoff]
    combined = combined.drop_duplicates(
        subset=["commodity_symbol", "date"], keep="last"
    )
    combined = combined.sort_values(
        ["commodity_symbol", "date"]
    ).reset_index(drop=True)

    atomic_write_parquet_to_s3(combined, bucket, rolling_key)
    return combined


# =============================================================
# DLQ WRITE HELPER
# =============================================================

def write_dlq(
    failed_tickers: list[str],
    run_id:         str,
    bucket:         str,
    prefix:         str,
) -> None:
    """
    Persist failed tickers to the DLQ immediately after the fetch stage —
    before any processing that could raise — so they are always recoverable
    even if the pipeline crashes later.
    """
    if not failed_tickers:
        return

    key = f"{prefix}dlq/run_id={run_id}/failed_tickers.json"
    write_json_to_s3(
        {
            "run_id":          run_id,
            "failed_tickers":  failed_tickers,
            "count":           len(failed_tickers),
            "written_at":      utc_now().isoformat(),
        },
        bucket,
        key,
    )
    print(f"  [DLQ] {len(failed_tickers)} ticker(s) -> s3://{bucket}/{key}")


# =============================================================
# METADATA WRITE HELPER
# =============================================================

def write_metadata(
    bucket:           str,
    meta_key:         str,
    status:           str,
    run_id:           str,
    run_ts:           datetime,
    mode_label:       str,
    force_overwrite:  bool,
    start_date:       date_type,
    today:            date_type,
    input_source:     str,
    expected_tickers: int,
    actual_tickers:   int,
    failed_tickers:   list,
    failure_rate:     float,
    input_rows:       int,
    output_rows:      int,
    processing_time_s: float,
    dq_report:        dict,
    sla:              dict,
    anomalies:        list,
    raw_partition_log:  dict,
    feat_partition_log: dict,
    reason:           str = "",
) -> None:
    """Write the complete run summary JSON. Called on both success and failure."""
    payload = {
        "run_id":              run_id,
        "run_ts":              run_ts.isoformat(),
        "status":              status,
        "mode":                mode_label,
        "force_overwrite":     force_overwrite,
        "reason":              reason,
        "start_date":          str(start_date),
        "end_date":            str(today),
        "input_source":        input_source,
        "feature_version":     CONFIG["feature_version"],
        "transformation":      CONFIG["transformation"],
        "schema_version":      SCHEMA_VERSION,
        "schema_hash":         SCHEMA_HASH,
        "expected_tickers":    expected_tickers,
        "actual_tickers":      actual_tickers,
        "failed_tickers":      failed_tickers,
        "failure_rate":        round(failure_rate, 4),
        "input_rows":          input_rows,
        "output_rows":         output_rows,
        "processing_time_s":   processing_time_s,
        "dq":                  dq_report,
        "sla":                 sla,
        "anomalies":           anomalies,
        "raw_partitions":      raw_partition_log,
        "feature_partitions":  feat_partition_log,
    }
    write_json_to_s3(payload, bucket, meta_key)


# =============================================================
# MAIN PIPELINE  —  update_commodity_pipeline
# =============================================================

def update_commodity_pipeline(
    bucket:              str        = "yeeshu-commodity-bucket",
    prefix:              str        = "historical-commodity/",
    tickers:             list[str]  = None,
    start_date_override: str        = None,   # "YYYY-MM-DD" -> backfill mode
    replay_from_raw:     bool       = False,  # True -> skip yfinance, read Layer 1
    force_overwrite:     bool       = False,  # True -> overwrite existing partitions
    tickers_override:    list[str]  = None,   # selective processing (DLQ replay)
) -> tuple[pd.DataFrame | None, str]:
    """
    Main pipeline entry point.

    Modes
    -----
    Incremental (default)
        Reads rolling file for start_date. Fetches from yfinance.
        Generates synthetic rows for any ticker missing today's data.

    Backfill  (start_date_override set)
        start_date = override. Fetches from yfinance.
        Set force_overwrite=True to replace existing feature partitions.

    Replay from raw  (replay_from_raw=True)
        Reads Layer 1 raw partitions — does NOT call yfinance.
        Deterministic and reproducible. data_quality_flag preserved from raw.
        Feature partitions are always versioned under the new run_id.

    Selective / DLQ replay  (tickers_override set)
        Processes only the specified tickers. Coverage SLA is relaxed.
        Used by replay_failed_tickers() — do not call directly.

    Idempotency
    -----------
    - Feature partitions versioned by run_id — no silent history overwrite.
    - Raw partitions skip existing keys unless force_overwrite=True.
    - record_id = "<commodity_symbol>_<YYYY-MM-DD>" is deterministic.

    Airflow integration
    -------------------
        # Backfill conf:  {"start_date": "2024-06-01"}
        # Replay conf:    {"replay_from_raw": true, "start_date": "2024-06-01"}
        # DLQ replay:     call replay_failed_tickers(run_id) in a separate DAG.
        #
        # DAG settings:
        #   max_active_runs = 1
        #   retries         = 2
        #   retry_delay     = timedelta(minutes=5)
    """

    # ── 0. INIT ────────────────────────────────────────────────────────
    run_ts  = utc_now()
    run_id  = f"run_{run_ts.strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
    today   = today_utc()

    is_partial_run      = tickers_override is not None
    effective_overwrite = force_overwrite or replay_from_raw

    mode_label = (
        "replay_from_raw" if replay_from_raw
        else "dlq_replay"  if is_partial_run
        else "backfill"    if start_date_override
        else "incremental"
    )

    print(f"\n{'=' * 66}")
    print(f"  COMMODITY PIPELINE START  run_id={run_id}")
    print(f"  mode={mode_label}   force_overwrite={effective_overwrite}")
    print(f"  partial_run={is_partial_run}   replay_from_raw={replay_from_raw}")
    print(f"{'=' * 66}\n")

    rolling_key  = prefix + "rolling/commodities_30d.parquet"
    meta_key     = f"{prefix}metadata/run_id={run_id}/run_summary.json"
    input_source = "yfinance"

    # Mutable run-level accumulators
    failed_tickers:     list  = []
    failure_rate:       float = 0.0
    dq_report:          dict  = {}
    sla:                dict  = {}
    anomalies:          list  = []
    raw_partition_log:  dict  = {}
    feat_partition_log: dict  = {}
    clean_df:    pd.DataFrame = pd.DataFrame()
    new_features: pd.DataFrame = pd.DataFrame()
    processing_time_s:  float  = 0.0
    start_date: date_type      = today

    # Write STARTED sentinel immediately — detectable on mid-run crash
    write_json_to_s3(
        {"run_id": run_id, "run_ts": run_ts.isoformat(),
         "status": "STARTED", "mode": mode_label},
        bucket,
        meta_key,
    )

    lock_acquired = False
    try:
        acquire_s3_lock(bucket, prefix, run_id)
        lock_acquired = True
    except LockError:
        raise

    try:

        # ── STAGE 1: Load existing rolling data + validate ticker contract ─
        print("[ STAGE 1 ] Load existing rolling data + validate ticker contract")

        old_rolling_df, last_date = read_rolling_layer(bucket, rolling_key)

        # Resolve effective ticker universe
        raw_tickers = tickers if tickers is not None else DEFAULT_TICKERS
        all_tickers = validate_ticker_contract(raw_tickers)
        expected_tickers = len(all_tickers)

        # Selective override for DLQ replay — only process failed tickers
        active_tickers = tickers_override if is_partial_run else all_tickers
        print(
            f"  effective tickers : {len(active_tickers)} of {expected_tickers}  "
            f"({', '.join(active_tickers)})"
        )

        # ── STAGE 2: Determine date range ────────────────────────────────
        print("\n[ STAGE 2 ] Determine date range")

        if start_date_override:
            start_date = pd.to_datetime(start_date_override, utc=True).date()
            print(f"  override -> start_date={start_date}")
        elif last_date is not None:
            if last_date >= today:
                print("  [SKIP] Rolling file already up to date.")
                write_json_to_s3(
                    {"run_id": run_id, "status": "SKIPPED",
                     "reason": "already_up_to_date"},
                    bucket, meta_key,
                )
                return None, "NO_UPDATE_NEEDED"
            start_date = last_date + timedelta(days=1)
        else:
            start_date = today - timedelta(days=365)

        print(f"  date range: {start_date} -> {today}")

        # ── STAGE 3: Fetch raw data (or read from raw layer) ─────────────
        if replay_from_raw:
            print("\n[ STAGE 3 ] Read raw data from Layer 1 (replay mode)")
            raw_df = read_raw_layer(
                bucket, prefix, start_date, today, tickers_override
            )

            if raw_df.empty:
                msg = "Replay aborted — no raw partitions found for the requested range."
                send_alert(msg, level="ERROR",
                           context={"start_date": str(start_date), "end_date": str(today)})
                write_metadata(
                    bucket=bucket, meta_key=meta_key, status="FAILED",
                    run_id=run_id, run_ts=run_ts, mode_label=mode_label,
                    force_overwrite=effective_overwrite, start_date=start_date,
                    today=today, input_source=input_source,
                    expected_tickers=expected_tickers, actual_tickers=0,
                    failed_tickers=[], failure_rate=0.0, input_rows=0, output_rows=0,
                    processing_time_s=0.0, dq_report={}, sla={}, anomalies=[],
                    raw_partition_log={}, feat_partition_log={}, reason=msg,
                )
                return None, msg

        else:
            print("\n[ STAGE 3 ] Fetch raw data from yfinance")
            raw_df, failed_tickers = fetch_commodity_data(
                tickers    = active_tickers,
                start_date = start_date,
                end_date   = today,
                run_id     = run_id,
                run_ts     = run_ts,
            )

            # DLQ written before anything that could raise
            write_dlq(failed_tickers, run_id, bucket, prefix)

            # Build synthetic rows for tickers missing today's data
            print("  [STAGE 3b] Building synthetic rows for missing today's data...")
            synth_df = build_synthetic_rows(
                fetched_df  = raw_df,
                old_rolling = old_rolling_df,
                tickers     = active_tickers,
                today       = today,
                run_id      = run_id,
                run_ts      = run_ts,
            )

            # Combine real + synthetic; synthetic appended after real rows
            if not synth_df.empty:
                raw_df = pd.concat([raw_df, synth_df], ignore_index=True)

            # Filter to strictly new dates only
            if last_date is not None:
                raw_df = raw_df[raw_df["date"].dt.date > last_date]

            if raw_df.empty:
                msg = "No new data fetched and no synthetic rows generated."
                send_alert(msg, level="WARNING",
                           context={"start_date": str(start_date)})
                write_metadata(
                    bucket=bucket, meta_key=meta_key, status="FAILED",
                    run_id=run_id, run_ts=run_ts, mode_label=mode_label,
                    force_overwrite=effective_overwrite, start_date=start_date,
                    today=today, input_source=input_source,
                    expected_tickers=expected_tickers, actual_tickers=0,
                    failed_tickers=failed_tickers, failure_rate=1.0, input_rows=0,
                    output_rows=0, processing_time_s=0.0, dq_report={}, sla={},
                    anomalies=[], raw_partition_log={}, feat_partition_log={},
                    reason=msg,
                )
                return None, "NO_NEW_DATA"

            failure_rate = len(failed_tickers) / max(len(active_tickers), 1)
            if failure_rate > CONFIG["failure_threshold"]:
                msg = (
                    f"Fetch failure rate {failure_rate:.1%} exceeds "
                    f"threshold {CONFIG['failure_threshold']:.0%} — aborting."
                )
                send_alert(msg, level="CRITICAL",
                           context={"failed": len(failed_tickers),
                                    "total": len(active_tickers)})
                write_metadata(
                    bucket=bucket, meta_key=meta_key, status="FAILED",
                    run_id=run_id, run_ts=run_ts, mode_label=mode_label,
                    force_overwrite=effective_overwrite, start_date=start_date,
                    today=today, input_source=input_source,
                    expected_tickers=expected_tickers, actual_tickers=0,
                    failed_tickers=failed_tickers, failure_rate=failure_rate,
                    input_rows=0, output_rows=0, processing_time_s=0.0,
                    dq_report={}, sla={}, anomalies=[],
                    raw_partition_log={}, feat_partition_log={}, reason=msg,
                )
                raise RuntimeError(msg)

        # ── STAGE 4: Schema enforcement — raw ───────────────────────────
        print("\n[ STAGE 4 ] Schema enforcement — raw layer")
        try:
            raw_df = enforce_schema(raw_df, RAW_SCHEMA, label="raw")
        except SchemaError:
            write_metadata(
                bucket=bucket, meta_key=meta_key, status="FAILED",
                run_id=run_id, run_ts=run_ts, mode_label=mode_label,
                force_overwrite=effective_overwrite, start_date=start_date,
                today=today, input_source=input_source,
                expected_tickers=expected_tickers, actual_tickers=0,
                failed_tickers=failed_tickers, failure_rate=failure_rate,
                input_rows=0, output_rows=0, processing_time_s=0.0,
                dq_report={}, sla={}, anomalies=[],
                raw_partition_log={}, feat_partition_log={},
                reason="Raw schema enforcement failed",
            )
            raise

        # ── STAGE 5: Write raw partitions (Layer 1) ─────────────────────
        if not replay_from_raw:
            print("\n[ STAGE 5 ] Write raw partitions (Layer 1)")
            raw_partition_log = write_raw_layer(
                raw_df, bucket, prefix, force_overwrite=effective_overwrite
            )
        else:
            print("\n[ STAGE 5 ] Skipped — Layer 1 is immutable source in replay mode")

        # ── STAGE 6: Data quality + SLA gate ────────────────────────────
        print("\n[ STAGE 6 ] Data quality + SLA validation")
        clean_df, dq_report = validate_data(raw_df)
        sla = validate_sla(
            clean_df,
            expected_tickers = expected_tickers,
            run_date         = today,
            is_partial_run   = is_partial_run,
        )

        if sla["status"] == "FAIL":
            msg = f"SLA FAIL: {sla.get('errors')}"
            write_metadata(
                bucket=bucket, meta_key=meta_key, status="FAILED",
                run_id=run_id, run_ts=run_ts, mode_label=mode_label,
                force_overwrite=effective_overwrite, start_date=start_date,
                today=today, input_source=input_source,
                expected_tickers=expected_tickers,
                actual_tickers=clean_df["commodity_symbol"].nunique(),
                failed_tickers=failed_tickers, failure_rate=failure_rate,
                input_rows=len(clean_df), output_rows=0,
                processing_time_s=0.0, dq_report=dq_report, sla=sla,
                anomalies=[], raw_partition_log=raw_partition_log,
                feat_partition_log={}, reason=msg,
            )
            raise RuntimeError(msg)

        # ── STAGE 7: Feature engineering ────────────────────────────────
        print("\n[ STAGE 7 ] Feature engineering")
        feat_start = time.time()

        new_features = generate_commodity_features(
            clean_df    = clean_df,
            old_rolling = old_rolling_df,
        )

        processing_time_s = round(time.time() - feat_start, 3)
        print(f"  elapsed: {processing_time_s}s  |  output rows: {len(new_features):,}")

        if new_features.empty:
            msg = "Feature engineering produced no output rows — aborting."
            send_alert(msg, level="ERROR",
                       context={"input_rows": len(clean_df)})
            write_metadata(
                bucket=bucket, meta_key=meta_key, status="FAILED",
                run_id=run_id, run_ts=run_ts, mode_label=mode_label,
                force_overwrite=effective_overwrite, start_date=start_date,
                today=today, input_source=input_source,
                expected_tickers=expected_tickers,
                actual_tickers=clean_df["commodity_symbol"].nunique(),
                failed_tickers=failed_tickers, failure_rate=failure_rate,
                input_rows=len(clean_df), output_rows=0,
                processing_time_s=processing_time_s, dq_report=dq_report,
                sla=sla, anomalies=[], raw_partition_log=raw_partition_log,
                feat_partition_log={}, reason=msg,
            )
            return None, msg

        # ── STAGE 8: Anomaly detection ────────────────────────────────
        print("\n[ STAGE 8 ] Anomaly detection")
        anomalies = detect_anomalies(new_features, old_rolling_df)

        # ── STAGE 9: Attach lineage metadata ────────────────────────────
        print("\n[ STAGE 9 ] Attach lineage metadata")
        new_features = add_metadata(
            df                = new_features,
            run_id            = run_id,
            run_ts            = run_ts,
            input_source      = input_source,
            expected_tickers  = expected_tickers,
            input_rows        = len(clean_df),
            output_rows       = len(new_features),
            processing_time_s = processing_time_s,
            start_date        = start_date,
            replay_mode       = replay_from_raw,
            is_partial_run    = is_partial_run,
        )

        # ── STAGE 10: Schema enforcement — features ──────────────────────
        print("\n[ STAGE 10 ] Schema enforcement — feature layer")
        try:
            new_features = enforce_schema(new_features, FEATURE_SCHEMA, label="features")
        except SchemaError:
            write_metadata(
                bucket=bucket, meta_key=meta_key, status="FAILED",
                run_id=run_id, run_ts=run_ts, mode_label=mode_label,
                force_overwrite=effective_overwrite, start_date=start_date,
                today=today, input_source=input_source,
                expected_tickers=expected_tickers,
                actual_tickers=clean_df["commodity_symbol"].nunique(),
                failed_tickers=failed_tickers, failure_rate=failure_rate,
                input_rows=len(clean_df), output_rows=len(new_features),
                processing_time_s=processing_time_s, dq_report=dq_report,
                sla=sla, anomalies=anomalies,
                raw_partition_log=raw_partition_log, feat_partition_log={},
                reason="Feature schema enforcement failed",
            )
            raise

        # ── STAGE 11: Write feature partitions (Layer 2) ─────────────────
        print("\n[ STAGE 11 ] Write feature partitions (Layer 2)")
        feat_partition_log = write_feature_layer(
            new_features, bucket, prefix,
            run_id          = run_id,
            force_overwrite = effective_overwrite,
        )

        # ── STAGE 12: Update rolling serving file (Layer 3) ──────────────
        print("\n[ STAGE 12 ] Update rolling window (Layer 3)")
        rolling_df = write_rolling_layer(
            new_features   = new_features,
            old_rolling_df = old_rolling_df,
            today          = today,
            bucket         = bucket,
            rolling_key    = rolling_key,
        )

        # ── STAGE 13: Write run summary (overwrites STARTED sentinel) ────
        print("\n[ STAGE 13 ] Write run summary")
        write_metadata(
            bucket=bucket, meta_key=meta_key, status="SUCCESS",
            run_id=run_id, run_ts=run_ts, mode_label=mode_label,
            force_overwrite=effective_overwrite, start_date=start_date,
            today=today, input_source=input_source,
            expected_tickers=expected_tickers,
            actual_tickers=clean_df["commodity_symbol"].nunique(),
            failed_tickers=failed_tickers, failure_rate=failure_rate,
            input_rows=len(clean_df), output_rows=len(new_features),
            processing_time_s=processing_time_s, dq_report=dq_report,
            sla=sla, anomalies=anomalies,
            raw_partition_log=raw_partition_log,
            feat_partition_log=feat_partition_log,
        )

        print(f"\n{'=' * 66}")
        print(f"  COMMODITY PIPELINE SUCCESS   run_id={run_id}")
        print(f"  output rows   : {len(new_features):,}")
        print(f"  rolling rows  : {len(rolling_df):,}")
        print(f"  feature time  : {processing_time_s}s")
        print(f"  anomalies     : {len(anomalies)}")
        print(f"  synthetic rows: {int((new_features['data_quality_flag'] == 'SYNTHETIC').sum()):,}")
        print(f"{'=' * 66}\n")
        send_alert(
            "Commodity pipeline completed successfully",
            level="INFO",
            context={
                "run_id": run_id,
                "rows": len(new_features),
                "tickers": new_features["commodity_symbol"].nunique()
            }
        )
        return rolling_df, f"UPDATED_{len(new_features)}_ROWS"

    except Exception as exc:
        send_alert(
            f"Pipeline failed: {exc}",
            level   = "CRITICAL",
            context = {"run_id": run_id, "mode": mode_label, "error": str(exc)},
        )
        raise

    finally:
        if lock_acquired:
            release_s3_lock(bucket, prefix)


# =============================================================
# DLQ REPLAY  —  selective re-fetch of failed tickers only
# =============================================================

def replay_failed_tickers(
    run_id: str,
    bucket: str = "yeeshu-commodity-bucket",
    prefix: str = "historical-commodity/",
) -> tuple[pd.DataFrame | None, str]:
    """
    Selectively reprocess ONLY the failed tickers from a past run.

    Reads the DLQ for the given run_id, extracts failed tickers, and
    passes them as tickers_override to update_commodity_pipeline. This
    processes ONLY the failed tickers — the rest of the dataset is untouched.

    Usage:
        replay_failed_tickers("run_20250418_143201_a3f9bc")

    Airflow:
        Wire an S3 sensor on the metadata key.
        Trigger when run_summary["failed_tickers"] is non-empty.
        Separate replay DAG with max_active_runs=1.
    """
    dlq_key  = f"{prefix}dlq/run_id={run_id}/failed_tickers.json"
    meta_key = f"{prefix}metadata/run_id={run_id}/run_summary.json"

    if not s3_key_exists(bucket, dlq_key):
        print(f"[REPLAY-DLQ] No DLQ entry found for run_id={run_id}")
        return None, "No DLQ entry found"

    if not s3_key_exists(bucket, meta_key):
        msg = f"[REPLAY-DLQ] Run summary not found for run_id={run_id}"
        send_alert(msg, level="ERROR", context={"run_id": run_id})
        return None, msg

    dlq  = json.loads(get_s3().get_object(Bucket=bucket, Key=dlq_key)["Body"].read())
    meta = json.loads(get_s3().get_object(Bucket=bucket, Key=meta_key)["Body"].read())

    failed = dlq.get("failed_tickers", [])
    if not failed:
        print("[REPLAY-DLQ] DLQ is empty — nothing to replay.")
        return None, "DLQ empty"

    original_start = meta.get("start_date")
    print(
        f"[REPLAY-DLQ] Replaying {len(failed)} ticker(s) from run_id={run_id} "
        f"using start_date={original_start}"
    )

    # KEY: tickers_override ensures ONLY failed tickers are re-fetched
    return update_commodity_pipeline(
        bucket               = bucket,
        prefix               = prefix,
        start_date_override  = original_start,
        force_overwrite      = True,
        tickers_override     = failed,
    )


# =============================================================
# RAW REPLAY CONVENIENCE WRAPPER
# =============================================================

def replay_commodities_from_raw(
    start_date: str,
    bucket:     str  = "yeeshu-commodity-bucket",
    prefix:     str  = "historical-commodity/",
    tickers:    list[str] = None,
) -> tuple[pd.DataFrame | None, str]:
    """
    Recompute commodity features from Layer 1 raw partitions for a given
    date range. Does NOT call yfinance — fully deterministic and reproducible.

    SYNTHETIC rows are replayed as-is from the raw layer — the imputation
    that was originally applied is preserved and traceable via data_quality_flag.

    Each replay produces a new run_id -> new versioned feature partition.
    History is preserved. Rolling layer is updated with the recomputed values.

    Airflow conf:  {"start_date": "2024-01-01"}
    DAG setting:   max_active_runs=1
    """
    return update_commodity_pipeline(
        bucket               = bucket,
        prefix               = prefix,
        tickers              = tickers,
        start_date_override  = start_date,
        replay_from_raw      = True,
        force_overwrite      = True,
    )