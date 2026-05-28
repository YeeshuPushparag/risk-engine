"""
fx_exposure_pipeline.py
========================
Production-grade FX exposure pipeline — hedge-fund quality.
Mirrors the architecture of market_features_pipeline.py exactly.

Storage layout
--------------
Layer 1 — Raw       (immutable, append-only, partitioned by date)
    s3://<bucket>/<prefix>raw/year=Y/month=MM/day=DD/data.parquet
    Contains: date, currency_pair, fx_rate, interest_diff, record_id
    Both yfinance and FRED data are stored here so replay is self-contained.

Layer 2 — Features  (versioned per run_id, partitioned by date)
    s3://<bucket>/<prefix>features/year=Y/month=MM/day=DD/run_id=<id>/data.parquet
    s3://<bucket>/<prefix>features/latest.json   <- pointer to most recent run_id

Layer 3 — Rolling   (mutable serving layer, last N calendar days)
    s3://<bucket>/<prefix>rolling/fx_exposure_30d.parquet

Support files
-------------
Lock     : s3://<bucket>/<prefix>locks/pipeline.lock
DLQ      : s3://<bucket>/<prefix>dlq/run_id=<id>/failed_pairs.json
Metadata : s3://<bucket>/<prefix>metadata/run_id=<id>/run_summary.json

Airflow integration notes
-------------------------
- DAG must set  max_active_runs=1  — S3 locking adds a safety net on top.
- Recommended:  retries=2, retry_delay=timedelta(minutes=5)
- Backfill:     pass {"start_date": "2024-01-01"} via Airflow conf
- Raw replay:   pass {"replay_from_raw": true, "start_date": "..."} via conf
  Wire an S3 sensor on the metadata key; trigger when failed_pairs > 0.
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
import os
import time
import pandas as pd
import numpy as np
import yfinance as yf
import boto3
from botocore.exceptions import ClientError
from fredapi import Fred
from datetime import datetime, timedelta, timezone, date as date_type
from io import BytesIO
import requests
from pendulum import timezone

# =============================================================
# CONFIG  —  single source of truth.  No magic numbers in code.
# =============================================================

CONFIG: dict = {
    # Fetch
    "max_retries":            3,
    "retry_backoff_s":        5,       # seconds; multiplied by attempt number

    # Failure handling
    "failure_threshold":      0.5,     # >50% failed pairs -> abort + alert
                                       # (FX has fewer pairs than equity tickers,
                                       #  so threshold is more lenient)

    # Data quality
    "coverage_threshold":     0.8,     # <80% of expected pairs -> SLA FAIL
    "max_fx_jump":            0.1,     # >10% single-day FX move -> outlier_flag
    "fred_lookback_days":     182,     # how far back to fetch FRED rate history

    # Anomaly detection thresholds (vs rolling baseline)
    "fx_rate_spike_ratio":    2.0,     # current stddev / historical stddev > 2x
    "interest_diff_jump":     2.0,     # abs(current - historical mean) > 2 * std
    "coverage_drop_ratio":    0.8,     # current pairs / historical pairs < 80%

    # Rolling serving layer
    "window_days":            30,

    # Lineage  <- bump feature_version when engineering logic changes
    "pipeline_name":          "fx_exposure_pipeline",
    "feature_version":        "v1",
    "transformation":         "fx_exposure_v1",
    "data_source":            "yfinance+fred",
}

# =============================================================
# BUSINESS CONSTANTS  —  sector -> currency mapping
# Kept at module level so they are version-controlled with the code.
# =============================================================

SECTOR_CURRENCY_MAP: dict[str, str] = {
    "Technology":             "USDEUR",
    "Healthcare":             "USDCHF",
    "Consumer Cyclical":      "USDEUR",
    "Financial Services":     "USDJPY",
    "Consumer Defensive":     "USDCAD",
    "Utilities":              "USDGBP",
    "Basic Materials":        "USDAUD",
    "Industrials":            "USDCNY",
    "Real Estate":            "USDJPY",
    "Energy":                 "USDCAD",
    "Communication Services": "USDEUR",
}

FOREIGN_RATIO: dict[str, float] = {
    "USDEUR": 0.45,
    "USDCHF": 0.30,
    "USDJPY": 0.25,
    "USDCAD": 0.33,
    "USDGBP": 0.10,
    "USDAUD": 0.40,
    "USDCNY": 0.35,
}

# FRED series IDs for each currency — USD is always the base.
FRED_RATE_SERIES: dict[str, str] = {
    "USD": "FEDFUNDS",
    "EUR": "ECBDFR",
    "GBP": "IR3TIB01GBM156N",
    "AUD": "IR3TIB01AUM156N",
    "CAD": "IR3TIB01CAM156N",
    "JPY": "IR3TIB01JPM156N",
    "CHF": "IR3TIB01CHM156N",
    "CNY": "IR3TIB01CNQ156N",
}

# Columns that must pass between ingestion and feature engineering.
RAW_COLS: list[str] = ["date", "currency_pair", "fx_rate", "interest_diff"]

# =============================================================
# SCHEMA DEFINITIONS
# =============================================================

SCHEMA_VERSION: str = "1.0"

# Canonical dtype for every column that must exist after raw load.
# Types are post-timezone-normalization (all datetimes are UTC-aware).
RAW_SCHEMA: dict[str, str] = {
    "date":          "datetime64[ns, UTC]",
    "currency_pair": "object",
    "fx_rate":       "float64",
    "interest_diff": "float64",
    "record_id":     "object",
}

# Schema expected after feature engineering — validated before any write.
# Macro columns are dynamic (sourced from MACRO_KEY CSV) so they are
# validated for presence but not individually listed here.
FEATURE_SCHEMA: dict[str, str] = {
    "date":                  "datetime64[ns, UTC]",
    "ticker":                "object",
    "sector":                "object",
    "industry":              "object",
    "currency_pair":         "object",
    "foreign_revenue_ratio": "float64",
    "fx_rate":               "float64",
    "interest_diff":         "float64",
    "revenue":               "float64",
    "record_id":             "object",
    "outlier_flag":          "bool",
    # Lineage — validated presence only; values are set by add_metadata()
    "pipeline_name":         "object",
    "pipeline_run_id":       "object",
    "data_source":           "object",
    "transformation":        "object",
    "feature_version":       "object",
    "schema_version":        "object",
    "schema_hash":           "object",
    "record_created_at":     "datetime64[ns, UTC]",
    "data_date":             "object",
    "replay_mode":           "bool",
}

# Deterministic fingerprint — written to every output row so schema drift
# is detectable at the individual record level without scanning metadata.
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
    """Raised when the input reference file fails contract validation."""

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
# MARKET DATE HELPERS
# =============================================================

def get_market_end_date() -> date_type:
    """
    Determine the appropriate end date for market data processing.
    
    - If before 4:00 PM ET (market close): use yesterday
    - If after 4:00 PM ET: use today
    """
    eastern = timezone("America/New_York")
    now_et = datetime.now(eastern)
    current_date_et = now_et.date()
    current_time_et = now_et.time()
    
    market_close = datetime.strptime("16:00", "%H:%M").time()
    
    if current_time_et < market_close:
        end_date = current_date_et - timedelta(days=1)
        print(f"  [MARKET] Before 4:00 PM ET — using yesterday as end_date: {end_date}")
    else:
        end_date = current_date_et
        print(f"  [MARKET] After 4:00 PM ET — using today as end_date: {end_date}")
    
    return end_date

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


def read_csv_from_s3(bucket: str, key: str) -> pd.DataFrame:
    print(f"  [S3 READ CSV]     s3://{bucket}/{key}")
    obj = get_s3().get_object(Bucket=bucket, Key=key)
    return pd.read_csv(BytesIO(obj["Body"].read()))


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
    s3 = get_s3()

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
            raw   = get_s3().get_object(Bucket=bucket, Key=lock_key)["Body"].read()
            info  = json.loads(raw)
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

def validate_input_contract(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate the reference input file (final_merged.parquet) before any
    expensive network calls. Raises InputContractError immediately on violation.

    Returns the cleaned, deduplicated ticker-sector-industry reference table.
    """
    required = ["ticker", "sector", "industry"]
    missing  = [c for c in required if c not in df.columns]
    if missing:
        msg = f"Input file missing required columns: {missing}. Found: {list(df.columns)}"
        send_alert(msg, level="CRITICAL", context={"missing": missing})
        raise InputContractError(msg)

    ref = df[required].drop_duplicates(subset=["ticker"]).dropna(subset=["ticker"]).copy()
    ref["ticker"] = ref["ticker"].astype(str).str.strip().str.upper()
    ref = ref[ref["ticker"] != ""]

    if ref.empty:
        msg = "Input reference file has no valid ticker rows after cleaning."
        send_alert(msg, level="CRITICAL")
        raise InputContractError(msg)

    # Validate all sectors are known
    unknown = set(ref["sector"].dropna()) - set(SECTOR_CURRENCY_MAP.keys())
    if unknown:
        send_alert(
            f"{len(unknown)} unknown sector(s) — no currency mapping available.",
            level="WARNING",
            context={"unknown_sectors": list(unknown)},
        )

    print(f"  [INPUT CONTRACT] OK — {len(ref):,} unique tickers, {ref['sector'].nunique()} sectors.")
    return ref

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

    - All schema keys must be present -> SchemaError on missing column.
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
# STAGE 1 HELPER — fetch_fx_data  (yfinance, per-pair retry + DLQ)
# =============================================================

def fetch_fx_data(
    pairs:      list[str],
    start_date: date_type,
    end_date:   date_type,
    run_id:     str,
    run_ts:     datetime,
) -> tuple[pd.DataFrame, list[str]]:
    """
    Download daily FX close rates from yfinance for each currency pair.

    Retry strategy: up to CONFIG["max_retries"] per pair with exponential
    backoff. Pairs that exhaust retries are added to failed_pairs (DLQ).

    Returns:
        (fx_long_df, failed_pairs)
        fx_long_df has columns: date, currency_pair, fx_rate, record_id,
                                ingestion_ts, pipeline_run_id
    """
    records: list = []
    failed_by_date: dict = {}
    
    # Initialize failed_by_date for all dates in range
    current_date = start_date
    while current_date <= end_date:
        if current_date.weekday() < 5:
            date_str = current_date.strftime("%Y-%m-%d")
            failed_by_date[date_str] = []
        current_date += timedelta(days=1)
    
    for pair in pairs:
        yahoo_symbol = pair + "=X"
        data = None

        for attempt in range(1, CONFIG["max_retries"] + 1):
            try:
                raw = yf.download(
                    tickers  = yahoo_symbol,
                    start    = str(start_date),
                    end      = str(end_date + timedelta(days=1)),
                    progress = False,
                )
                if raw is None or raw.empty:
                    raise ValueError(f"empty response for {yahoo_symbol}")
                data = raw
                break

            except Exception as exc:
                print(f"  [FETCH FX] {pair} attempt {attempt}/{CONFIG['max_retries']} failed: {exc}")
                if attempt < CONFIG["max_retries"]:
                    time.sleep(CONFIG["retry_backoff_s"] * attempt)
                else:
                    print(f"  [FETCH FX] {pair} exhausted retries -> DLQ")
                    # Mark this pair as failed for ALL dates
                    for date_str in failed_by_date.keys():
                        if pair not in failed_by_date[date_str]:
                            failed_by_date[date_str].append(pair)

        if data is None or data.empty:
            continue

        # Flatten yfinance MultiIndex columns
        if isinstance(data.columns, pd.MultiIndex):
            data.columns = ["_".join([str(x) for x in col if x]).strip() for col in data.columns.values]

        # Extract Close column
        close_cols = [c for c in data.columns if "Close" in c]
        if not close_cols:
            for date_str in failed_by_date.keys():
                if pair not in failed_by_date[date_str]:
                    failed_by_date[date_str].append(pair)
            continue

        target_col = None
        for col in close_cols:
            if yahoo_symbol in col:
                target_col = col
                break

        if target_col is None:
            for date_str in failed_by_date.keys():
                if pair not in failed_by_date[date_str]:
                    failed_by_date[date_str].append(pair)
            continue

        sub = data[[target_col]].copy().reset_index()
        sub.columns = ["date", "fx_rate"]
        sub["fx_rate"] = pd.to_numeric(sub["fx_rate"], errors="coerce")
      
        if sub["fx_rate"].isna().all():
            print(f"  [FETCH FX][EMPTY] {pair} - no FX rate data")
            for date_str in failed_by_date.keys():
                if pair not in failed_by_date[date_str]:
                    failed_by_date[date_str].append(pair)
            continue

        sub["currency_pair"] = pair
        sub["record_id"] = pair + "_" + sub["date"].astype(str)
        sub["ingestion_ts"] = run_ts
        sub["pipeline_run_id"] = run_id
        
        # Track which dates have data
        dates_with_data = set(sub["date"].dt.strftime("%Y-%m-%d"))
        
        # Mark missing dates for this pair
        for date_str in failed_by_date.keys():
            if date_str not in dates_with_data:
                if pair not in failed_by_date[date_str]:
                    failed_by_date[date_str].append(pair)
        
        
        records.append(sub[["date", "currency_pair", "fx_rate", "record_id", "ingestion_ts", "pipeline_run_id"]])

    if not records:
        return pd.DataFrame(), failed_by_date

    df = pd.concat(records, ignore_index=True)
    df["date"] = pd.to_datetime(df["date"], utc=True)
    df = df[df["date"].dt.dayofweek < 5]
    df = df[df["date"] >= to_utc_timestamp(start_date)]
    df.sort_values(["currency_pair", "date"], inplace=True)
    df["fx_rate"] = df.groupby("currency_pair")["fx_rate"].ffill()

    return df, failed_by_date

# =============================================================
# STAGE 1 HELPER — fetch_interest_rates  (FRED, per-currency retry)
# =============================================================

def fetch_interest_rates(
    start_date: date_type,
    end_date:   date_type,
) -> tuple[pd.DataFrame, list[str]]:
    """
    Download interest rates from FRED for each currency in FRED_RATE_SERIES.
    Computes interest_diff = USD_rate - foreign_rate for each USD<X> pair.

    Retry strategy: up to CONFIG["max_retries"] per FRED series.
    Currencies that fail are logged (non-fatal if USD succeeds).

    Returns:
        (rates_long_df, failed_currencies)
        rates_long_df has columns: date, currency_pair, interest_diff
    """
    fred_api_key   = os.getenv("FRED_API_KEY")
    fred           = Fred(api_key=fred_api_key)
    fred_start     = end_date - timedelta(days=CONFIG["fred_lookback_days"])

    all_rates:        dict = {}
    failed_currencies: list = []

    for currency, series_id in FRED_RATE_SERIES.items():
        for attempt in range(1, CONFIG["max_retries"] + 1):
            try:
                values = fred.get_series(
                    series_id,
                    observation_start = fred_start,
                    observation_end   = end_date,
                )
                if values is None or len(values) == 0:
                    raise ValueError(f"empty series for {series_id}")
                all_rates[currency] = pd.DataFrame({
                    "date": values.index,
                    currency: values.values,
                })
                break

            except Exception as exc:
                print(
                    f"  [FETCH FRED] {currency} ({series_id}) "
                    f"attempt {attempt}/{CONFIG['max_retries']} failed: {exc}"
                )
                if attempt < CONFIG["max_retries"]:
                    time.sleep(CONFIG["retry_backoff_s"] * attempt)
                else:
                    send_alert(
                        f"FRED series failed after retries: {currency} ({series_id})",
                        level="WARNING",
                        context={"currency": currency, "series_id": series_id},
                    )
                    failed_currencies.append(currency)

    if "USD" not in all_rates:
        msg = "FRED fetch failed for USD (FEDFUNDS) — cannot compute interest differentials."
        send_alert(msg, level="CRITICAL")
        raise RuntimeError(msg)

    # Merge all currency rate series into a wide table
    rates_df = None
    for currency, df_cur in all_rates.items():
        rates_df = (
            df_cur if rates_df is None
            else rates_df.merge(df_cur, on="date", how="outer")
        )

    rates_df.sort_values("date", inplace=True)
    rates_df.ffill(inplace=True)
    rates_df.bfill(inplace=True)

    # Compute interest_diff = USD - foreign for every USD<X> pair
    # Matching original pipeline logic exactly
    for currency in FRED_RATE_SERIES:
        if currency != "USD" and currency in rates_df.columns:
            rates_df[f"USD{currency}"] = rates_df["USD"] - rates_df[currency]
    rates_df["USDUSD"] = 0.0

    rates_df["date"] = pd.to_datetime(rates_df["date"], utc=True)
    rates_df = rates_df[rates_df["date"].dt.dayofweek < 5]

    # Filter to the requested date range
    rates_df = rates_df[
        (rates_df["date"] >= to_utc_timestamp(start_date)) &
        (rates_df["date"] <= to_utc_timestamp(end_date))
    ]

    # Melt to long format: date, currency_pair, interest_diff
    diff_cols = [c for c in rates_df.columns if c.startswith("USD") and c != "USD"]
    rates_long = rates_df.melt(
        id_vars    = ["date"],
        value_vars = diff_cols,
        var_name   = "currency_pair",
        value_name = "interest_diff",
    ).dropna(subset=["date", "currency_pair"]).sort_values("date")

    return rates_long, failed_currencies


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

    Both fx_rate and interest_diff were stored together at ingestion time,
    so replay is fully self-contained — no yfinance or FRED calls needed.

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


    if "record_id" not in combined.columns:
        combined["record_id"] = (
            combined["currency_pair"] + "_" + combined["date"].astype(str)
        )

    print(f"  [RAW READ] {len(frames)} partition(s), {len(combined):,} rows loaded.")
    return combined


# =============================================================
# STAGE 2 — validate_data  (DQ filtering + outlier flagging)
# =============================================================

def validate_data(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """
    Hard-filter structurally invalid rows. Returns (clean_df, dq_report).

    Outliers are FLAGGED in outlier_flag — NOT silently dropped —
    so downstream consumers make their own exclusion decisions with full
    auditability.
    """
    before = len(df)

    df = df.dropna(subset=["date", "currency_pair", "fx_rate"])
    df = df[df["fx_rate"] > 0]
    dropped = before - len(df)

    # Soft outlier detection: flag large single-day FX rate moves
    df = df.sort_values(["currency_pair", "date"]).copy()
    df["_prev_rate"]  = df.groupby("currency_pair")["fx_rate"].shift(1)
    df["_pct_chg"]    = (df["fx_rate"] - df["_prev_rate"]).abs() / df["_prev_rate"]
    df["outlier_flag"] = (df["_pct_chg"] > CONFIG["max_fx_jump"]).astype(bool)
    outlier_count      = int(df["outlier_flag"].sum())
    df = df.drop(columns=["_prev_rate", "_pct_chg"])

    if outlier_count > 0:
        send_alert(
            f"{outlier_count} rows flagged as FX rate outliers "
            f"(>{CONFIG['max_fx_jump']*100:.0f}% single-day move).",
            level="WARNING",
            context={"outlier_count": outlier_count},
        )

    dq_report = {
        "rows_before":   before,
        "rows_after":    len(df),
        "rows_dropped":  dropped,
        "outlier_flags": outlier_count,
    }
    print(f"  [DQ] {dq_report}")
    return df, dq_report


# =============================================================
# STAGE 3 — validate_sla
# =============================================================

def validate_sla(
    df:             pd.DataFrame,
    expected_pairs: int,
    run_date:       date_type,
) -> dict:
    """
    SLA gate. Pipeline raises RuntimeError on FAIL.
    """
    result = {"status": "PASS", "checks": {}}
    errors = []

    # 1. Coverage
    actual   = df["currency_pair"].nunique()
    coverage = actual / expected_pairs if expected_pairs else 0
    result["checks"]["coverage"] = f"{actual}/{expected_pairs} ({coverage:.1%})"
    if coverage < CONFIG["coverage_threshold"]:
        msg = f"Low pair coverage: {coverage:.1%} < required {CONFIG['coverage_threshold']:.0%}"
        errors.append(msg)
        send_alert(msg, level="ERROR", context={"actual": actual, "expected": expected_pairs})

    # 2. Freshness
    max_ts = df["date"].max()

    # expected latest business day
    expected_date = run_date

    while expected_date.weekday() >= 5:
        expected_date -= timedelta(days=1)

    result["checks"]["max_date"] = str(max_ts)

    if pd.isna(max_ts) or max_ts.date() < expected_date:

        msg = (
            f"Stale FX data: latest={max_ts}, "
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
        
    # 3. Duplicates (warn; resolved at write time)
    dup_count = int(df.duplicated(["currency_pair", "date"]).sum())
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
    Each detected anomaly calls send_alert immediately — not just logged.

    Checks:
    1. FX rate volatility spike   — current stddev vs historical stddev.
    2. Interest differential jump — current mean vs historical mean + std.
    3. Currency pair coverage drop — current pairs vs historical pairs.
    """
    anomalies: list[dict] = []

    if old_rolling_df is None or old_rolling_df.empty:
        print("  [ANOMALY] no baseline available — skipping drift checks.")
        return anomalies

    # 1. FX rate volatility spike
    curr_std = float(new_features["fx_rate"].std())
    hist_std = float(old_rolling_df["fx_rate"].std())
    if hist_std > 0:
        ratio = curr_std / hist_std
        if ratio > CONFIG["fx_rate_spike_ratio"]:
            record = {
                "type":       "fx_rate_volatility_spike",
                "current_std":    round(curr_std, 6),
                "historical_std": round(hist_std, 6),
                "ratio":          round(ratio, 3),
            }
            anomalies.append(record)
            send_alert(
                f"FX rate volatility spike: current/historical std ratio={ratio:.2f}x",
                level="WARNING",
                context=record,
            )

    # 2. Interest differential jump
    if "interest_diff" in new_features.columns and "interest_diff" in old_rolling_df.columns:
        curr_mean = float(new_features["interest_diff"].dropna().mean())
        hist_mean = float(old_rolling_df["interest_diff"].dropna().mean())
        hist_std2 = float(old_rolling_df["interest_diff"].dropna().std())
        if hist_std2 > 0:
            z_score = abs(curr_mean - hist_mean) / hist_std2
            if z_score > CONFIG["interest_diff_jump"]:
                record = {
                    "type":         "interest_diff_jump",
                    "current_mean": round(curr_mean, 4),
                    "historical_mean": round(hist_mean, 4),
                    "z_score":      round(z_score, 3),
                }
                anomalies.append(record)
                send_alert(
                    f"Interest differential jump: z-score={z_score:.2f} (threshold={CONFIG['interest_diff_jump']})",
                    level="WARNING",
                    context=record,
                )

    # 3. Currency pair coverage drop
    curr_pairs = new_features["currency_pair"].nunique()
    hist_pairs = old_rolling_df["currency_pair"].nunique()
    if hist_pairs > 0:
        ratio = curr_pairs / hist_pairs
        if ratio < CONFIG["coverage_drop_ratio"]:
            record = {
                "type":           "coverage_drop",
                "current_pairs":  curr_pairs,
                "historical_pairs": hist_pairs,
                "ratio":          round(ratio, 3),
            }
            anomalies.append(record)
            send_alert(
                f"Currency pair coverage drop: {curr_pairs} vs {hist_pairs} historical ({ratio:.1%})",
                level="ERROR",
                context=record,
            )

    if not anomalies:
        print("  [ANOMALY] no anomalies detected.")
    else:
        print(f"  [ANOMALY] {len(anomalies)} anomaly/anomalies detected.")

    return anomalies


# =============================================================
# STAGE 5 — build_fx_features  (pure — no I/O, no side effects)
# =============================================================

def build_fx_features(
    clean_raw_df:    pd.DataFrame,
    ticker_ref:      pd.DataFrame,
    input_parquet:   pd.DataFrame,
    macro_df:        pd.DataFrame,
) -> pd.DataFrame:
    """
    Pure feature transformation. Applies all business enrichment logic.

    Inputs:
        clean_raw_df  — validated raw FX + interest_diff data
        ticker_ref    — ticker/sector/industry reference (from input contract)
        input_parquet — full input file (for revenue column)
        macro_df      — macro CSV loaded upstream

    Steps  (preserving original pipeline logic exactly):
        1. Add sector -> currency_pair and foreign_revenue_ratio to ticker_ref.
        2. Join ticker_ref with FX + interest_diff on currency_pair + date.
        3. Merge revenue from input_parquet.
        4. Merge macro by month.

    Returns the fully enriched feature DataFrame.
    No I/O. Zero global state mutation. Safe to unit-test in isolation.
    """
    # Step 1 — attach currency mapping to ticker reference
    ref = ticker_ref.copy()
    ref["currency_pair"]         = ref["sector"].map(SECTOR_CURRENCY_MAP)
    ref["foreign_revenue_ratio"] = ref["currency_pair"].map(FOREIGN_RATIO)

    # Step 2 — join with FX rates and interest differentials via merge_asof
    # Use only the columns needed to avoid carrying ingestion metadata into features
    fx_cols     = ["date", "currency_pair", "fx_rate", "interest_diff",
                   "record_id", "outlier_flag"]
    available   = [c for c in fx_cols if c in clean_raw_df.columns]
    fx_base     = clean_raw_df[available].copy()

    # Merge ticker-currency mapping with FX data (left: all ticker dates)
    company_fx = ref.merge(fx_base, on="currency_pair", how="left")
    company_fx = company_fx.sort_values("date")

    # Step 3 — merge revenue from the input parquet
    revenue_ref = (
        input_parquet[["ticker", "revenue"]]
        .drop_duplicates(subset=["ticker"])
        .copy()
    )
    df_final = company_fx.merge(revenue_ref, on="ticker", how="inner")

    # Step 4 — merge macro data by month (matching original pipeline logic)
    df_final["month"] = df_final["date"].dt.to_period("M")
    macro_df           = macro_df.copy()
    macro_df["month"]  = pd.to_datetime(macro_df["date"], utc=True).dt.to_period("M")

    df_final = df_final.merge(
        macro_df.drop(columns=["date"], errors="ignore"),
        on    = "month",
        how   = "inner",
    ).drop(columns=["month"])

    return df_final


# =============================================================
# STAGE 6 — add_metadata  (lineage enrichment)
# =============================================================

def add_metadata(
    df:             pd.DataFrame,
    run_id:         str,
    run_ts:         datetime,
    input_source:   str,
    input_rows:     int,
    output_rows:    int,
    processing_time_s: float,
    start_date,
    replay_mode:    bool,
    mode_label:     str,
) -> pd.DataFrame:
    """
    Stamp every output row with complete lineage and observability fields.

    Guarantees:
    - pipeline_run_id and record_created_at are identical across all rows
      in the same run.
    - record_id links each feature row back to its exact raw source event.
    - schema_version + schema_hash make schema drift detectable per row.
    - replay_mode flag distinguishes recomputed from live-ingested rows.
    """
    df = df.copy()

    # Core lineage
    df["pipeline_name"]        = CONFIG["pipeline_name"]
    df["pipeline_run_id"]      = run_id                     # constant per run
    df["data_source"]          = CONFIG["data_source"]
    df["input_source"]         = input_source
    df["transformation"]       = CONFIG["transformation"]
    df["feature_version"]      = CONFIG["feature_version"]  # config-driven
    df["schema_version"]       = SCHEMA_VERSION
    df["schema_hash"]          = SCHEMA_HASH
    df["record_created_at"]    = run_ts                     # constant per run, UTC-aware

    # Data dimension
    df["data_date"]            = df["date"].dt.date.astype(str)
    df["ingestion_start_date"] = str(start_date)

    # Run observability
    df["input_rows"]           = input_rows
    df["output_rows"]          = output_rows
    df["processing_time_s"]    = round(processing_time_s, 3)

    # Mode flags
    df["replay_mode"]          = replay_mode
    df["run_mode"]             = mode_label

    # record_id was carried forward from the raw stage — not re-stamped here.

    return df


# =============================================================
# WRITE HELPERS — Layer 1 (raw)
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
    Layer 1: write immutable raw partitions (FX rates + interest_diff) by date.

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
    - force_overwrite=True replaces the same run_id's output (Airflow
      task retry case).

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

    # Update latest.json pointer so downstream consumers know the freshest run
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
    - New rows win over old rows for the same (ticker, currency_pair, date).
    - Keeps ONLY last N trading/business dates.
    - Correct for incremental, backfill, and replay runs.

    Uses atomic write to prevent a corrupt rolling file on partial failure.
    """

    combined = (
        pd.concat([old_rolling_df, new_features], ignore_index=True)
        if old_rolling_df is not None
        else new_features.copy()
    )

    combined = combined.drop_duplicates(
        subset=["ticker", "currency_pair", "date"],
        keep="last"
    )

    combined = combined.sort_values(
        ["ticker", "currency_pair", "date"]
    ).reset_index(drop=True)

    # =========================================================
    # KEEP LAST N TRADING / BUSINESS DATES
    # =========================================================

    unique_dates = (
        combined["date"]
        .drop_duplicates()
        .sort_values()
    )

    last_dates = unique_dates.tail(CONFIG["window_days"])

    combined = combined[
        combined["date"].isin(last_dates)
    ]

    combined = combined.sort_values(
        ["ticker", "currency_pair", "date"]
    ).reset_index(drop=True)

    atomic_write_parquet_to_s3(
        combined,
        bucket,
        rolling_key
    )

    return combined


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
    Persist failed currency pairs to the DLQ immediately after the fetch
    stage — before any processing that could raise — so they are always
    recoverable even if the pipeline crashes later.
    """
    if not failed_by_date:
        return

    for date_str, failed_pairs in failed_by_date.items():
        if not failed_pairs:
            continue

        failed = sorted(set(failed_pairs))

        key = f"{prefix}dlq/run_id={run_id}/date={date_str}/failed_pairs.json"

        payload = {
            "run_id": run_id,
            "date": date_str,
            "failed_pairs": failed,
            "count": len(failed),
            "written_at": utc_now().isoformat()
        }

        write_json_to_s3(payload, bucket, key)
        print(f"  [DLQ] {len(failed)} pair(s) for {date_str} -> s3://{bucket}/{key}")

# =============================================================
# METADATA WRITE HELPER
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
    end_date:        date_type,
    input_source:    str,
    expected_pairs:  int,
    actual_pairs:    int,
    failed_pairs:    list,
    failure_rate:    float,
    input_rows:      int,
    output_rows:     int,
    processing_time_s: float,
    dq_report:       dict,
    sla:             dict,
    anomalies:       list,
    raw_partition_log:  dict,
    feat_partition_log: dict,
    airflow_metadata: dict = None,
    reason:          str = "",
) -> None:
    """Write the complete run summary JSON. Called on both success and failure."""
    payload = {
        "run_id":              run_id,
        "run_ts":              run_ts.isoformat(),
        "status":              status,
        "mode":                mode_label,
        "force_overwrite":     force_overwrite,
        "reason":              reason,
        "airflow":             airflow_metadata, 
        "start_date":          str(start_date),
        "end_date":            str(end_date),
        "input_source":        input_source,
        "feature_version":     CONFIG["feature_version"],
        "transformation":      CONFIG["transformation"],
        "schema_version":      SCHEMA_VERSION,
        "schema_hash":         SCHEMA_HASH,
        "expected_pairs":      expected_pairs,
        "actual_pairs":        actual_pairs,
        "failed_pairs":        failed_pairs,
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
# MAIN PIPELINE  —  update_fx_pipeline
# =============================================================

def update_fx_pipeline(
    bucket:              str       = "yeeshu-fx-bucket",
    prefix:              str       = "historical-fx/",
    input_key:           str       = "historical-fx/final_merged.parquet",
    macro_key:           str       = "historical-fx/macro_data.csv",
    airflow_metadata:    dict      = None,
    start_date_override: str       = None,   # "YYYY-MM-DD" -> backfill mode
    replay_from_raw:     bool      = False,  # True -> skip yfinance+FRED, read Layer 1
    force_overwrite:     bool      = False,  # True -> overwrite existing partitions
) -> tuple[pd.DataFrame | None, str]:
    """
    Main pipeline entry point.

    Args:
        ...
        airflow_metadata: Airflow execution context (dag_id, task_id, etc.)
        ...

    Modes
    -----
    Incremental (default)
        Reads rolling file for start_date. Fetches from yfinance + FRED.

    Backfill  (start_date_override set)
        start_date = override. Fetches from yfinance + FRED.
        Set force_overwrite=True to replace existing feature partitions.

    Replay from raw  (replay_from_raw=True)
        Reads Layer 1 raw partitions — does NOT call yfinance or FRED.
        Deterministic and reproducible. Feature partitions are always
        versioned under the new run_id.



    Airflow integration
    -------------------
        # Backfill conf:  {"start_date": "2024-06-01"}
        # Replay conf:    {"replay_from_raw": true, "start_date": "2024-06-01"}
        #
        # DAG settings:
        #   max_active_runs = 1
        #   retries         = 2
        #   retry_delay     = timedelta(minutes=5)
    """

    # ── 0. INIT ────────────────────────────────────────────────────────
    run_ts  = utc_now()
    run_id  = f"run_{run_ts.strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
    end_date = get_market_end_date()

    effective_overwrite = force_overwrite or replay_from_raw

    mode_label = (
        "replay_from_raw" if replay_from_raw
        else "backfill"    if start_date_override
        else "incremental"
    )

    print(f"\n{'=' * 66}")
    print(f"  FX PIPELINE START  run_id={run_id}")
    print(f"  mode={mode_label}   force_overwrite={effective_overwrite}")
    print(f"{'=' * 66}\n")

    rolling_key  = prefix + "rolling/fx_exposure_30d.parquet"
    meta_key     = f"{prefix}metadata/run_id={run_id}/run_summary.json"
    input_source = f"s3://{bucket}/{input_key}"

    # Mutable accumulators — updated as stages complete
    failed_pairs:       list  = []
    failure_rate:       float = 0.0
    dq_report:          dict  = {}
    sla:                dict  = {}
    anomalies:          list  = []
    raw_partition_log:  dict  = {}
    feat_partition_log: dict  = {}
    clean_raw_df:  pd.DataFrame = pd.DataFrame()
    new_features:  pd.DataFrame = pd.DataFrame()
    processing_time_s:  float   = 0.0
    start_date: date_type       = today

    # Write STARTED sentinel immediately — detectable on mid-run crash
    write_json_to_s3(
        {"run_id": run_id, "run_ts": run_ts.isoformat(),
         "status": "STARTED", "mode": mode_label},
        bucket,
        meta_key,
    )

    # Acquire S3 lock — always released in finally
    lock_acquired = False
    try:
        acquire_s3_lock(bucket, prefix, run_id)
        lock_acquired = True
    except LockError:
        raise

    try:

        # ── STAGE 1: Load and validate input reference ──────────────────
        print("[ STAGE 1 ] Load and validate input reference")
        raw_input    = read_parquet_from_s3(bucket, input_key)
        ticker_ref   = validate_input_contract(raw_input)

        # Determine the full expected pair universe from the reference data
        ticker_ref["currency_pair"]         = ticker_ref["sector"].map(SECTOR_CURRENCY_MAP)
        ticker_ref["foreign_revenue_ratio"] = ticker_ref["currency_pair"].map(FOREIGN_RATIO)
        active_pairs    = ticker_ref["currency_pair"].dropna().unique().tolist()
        expected_pairs = len(active_pairs)

        print(f"  effective pairs: {len(active_pairs)} of {expected_pairs}")

        # ── STAGE 2: Determine date range ───────────────────────────────
        print("\n[ STAGE 2 ] Determine date range")
        old_rolling_df, last_date = read_rolling_layer(bucket, rolling_key)

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
                return None, "Already updated"
            start_date = last_date + timedelta(days=1)
        else:
            start_date = today - timedelta(days=7)

        print(f"  date range: {start_date} -> {end_date}")

        # ── STAGE 3: Fetch raw data ──────────────────────────────────────
        if replay_from_raw:
            print("\n[ STAGE 3 ] Read raw data from Layer 1 (replay mode)")
            raw_df = read_raw_layer(bucket, prefix, start_date, end_date)

            if raw_df.empty:
                msg = "Replay aborted — no raw partitions found for the requested range."
                send_alert(msg, level="ERROR",
                           context={"start_date": str(start_date), "end_date": str(end_date)})
                write_metadata(
                    bucket=bucket, meta_key=meta_key, status="FAILED",
                    run_id=run_id, run_ts=run_ts, mode_label=mode_label,
                    force_overwrite=effective_overwrite, start_date=start_date,
                    end_date=end_date, input_source=input_source,
                    expected_pairs=expected_pairs, actual_pairs=0,
                    failed_pairs=[], failure_rate=0.0, input_rows=0, output_rows=0,
                    processing_time_s=0.0, dq_report={}, sla={}, anomalies=[],
                    raw_partition_log={}, feat_partition_log={}, airflow_metadata=airflow_metadata, reason=msg,
                )
                return None, msg

        else:
            print("\n[ STAGE 3 ] Fetch raw FX data (yfinance) + interest rates (FRED)")

            print("  [STAGE 3a] Fetching FX rates from yfinance...")
            fx_df, failed_by_date = fetch_fx_data(
                pairs      = active_pairs,
                start_date = start_date,
                end_date   = end_date,
                run_id     = run_id,
                run_ts     = run_ts,
            )

            # Write DLQ before anything that could raise
            write_dlq(failed_by_date, run_id, bucket, prefix)
            
            # Also flatten for failure rate:
            all_failed = []
            for failures in failed_by_date.values():
                all_failed.extend(failures)
            failed_pairs = list(set(all_failed))

            if failed_pairs:
                print(f"  [WARNING] Failed pairs: {failed_pairs}")
                send_alert(
                    f"{len(failed_pairs)} pairs failed during fetch",
                    level="WARNING",
                    context={
                        "run_id": run_id,
                        "failed_count": len(failed_pairs)
                    }
                )

            if fx_df.empty:
                msg = "No FX data fetched from yfinance."
                send_alert(msg, level="ERROR", context={"pairs": active_pairs})
                write_metadata(
                    bucket=bucket, meta_key=meta_key, status="FAILED",
                    run_id=run_id, run_ts=run_ts, mode_label=mode_label,
                    force_overwrite=effective_overwrite, start_date=start_date,
                    end_date=end_date, input_source=input_source,
                    expected_pairs=expected_pairs, actual_pairs=0,
                    failed_pairs=failed_pairs, failure_rate=1.0, input_rows=0,
                    output_rows=0, processing_time_s=0.0, dq_report={}, sla={},
                    anomalies=[], raw_partition_log={}, feat_partition_log={},airflow_metadata=airflow_metadata,
                    reason=msg,
                )
                return None, msg

            failure_rate = len(failed_pairs) / max(len(active_pairs), 1)
            if failure_rate > CONFIG["failure_threshold"]:
                msg = (
                    f"FX fetch failure rate {failure_rate:.1%} "
                    "exceeds threshold — aborting."
                )
                send_alert(
                    "High FX fetch failure rate",
                    level="CRITICAL",
                    context={
                        "failed_pairs": len(failed_pairs),
                        "total_pairs": len(active_pairs),
                        "failure_rate": round(failure_rate, 3)
                    }
                )
                write_metadata(
                    bucket=bucket, meta_key=meta_key, status="FAILED",
                    run_id=run_id, run_ts=run_ts, mode_label=mode_label,
                    force_overwrite=effective_overwrite, start_date=start_date,
                    end_date=end_date, input_source=input_source,
                    expected_pairs=expected_pairs, actual_pairs=0,
                    failed_pairs=failed_pairs, failure_rate=failure_rate,
                    input_rows=0, output_rows=0, processing_time_s=0.0,
                    dq_report={}, sla={}, anomalies=[],
                    raw_partition_log={}, feat_partition_log={}, airflow_metadata=airflow_metadata,reason=msg,
                )
                raise RuntimeError(msg)

            print("  [STAGE 3b] Fetching interest rates from FRED...")
            rates_long, failed_currencies = fetch_interest_rates(start_date, end_date)

            # Join fx_rate with interest_diff via merge_asof (original logic)
            fx_df      = fx_df.sort_values("date")
            rates_long = rates_long.sort_values("date")
            raw_df     = pd.merge_asof(
                fx_df,
                rates_long,
                on        = "date",
                by        = "currency_pair",
                direction = "backward",
            )

            # Filter to new dates only (exclude already-stored in rolling)
            if last_date is not None:
                raw_df = raw_df[raw_df["date"].dt.date > last_date]

        # ── STAGE 4: Schema enforcement — raw ───────────────────────────
        print("\n[ STAGE 4 ] Schema enforcement — raw layer")
        try:
            raw_df = enforce_schema(raw_df, RAW_SCHEMA, label="raw")
        except SchemaError:
            write_metadata(
                bucket=bucket, meta_key=meta_key, status="FAILED",
                run_id=run_id, run_ts=run_ts, mode_label=mode_label,
                force_overwrite=effective_overwrite, start_date=start_date,
                end_date=end_date, input_source=input_source,
                expected_pairs=expected_pairs, actual_pairs=0,
                failed_pairs=failed_pairs, failure_rate=failure_rate,
                input_rows=0, output_rows=0, processing_time_s=0.0,
                dq_report={}, sla={}, anomalies=[],
                raw_partition_log={}, feat_partition_log={}, airflow_metadata=airflow_metadata,
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
        clean_raw_df, dq_report = validate_data(raw_df)
        sla = validate_sla(
            clean_raw_df,
            expected_pairs = expected_pairs,
            run_date       = today,
        )

        if sla["status"] == "FAIL":
            msg = f"SLA FAIL: {sla.get('errors')}"
            write_metadata(
                bucket=bucket, meta_key=meta_key, status="FAILED",
                run_id=run_id, run_ts=run_ts, mode_label=mode_label,
                force_overwrite=effective_overwrite, start_date=start_date,
                end_date=end_date, input_source=input_source,
                expected_pairs=expected_pairs,
                actual_pairs=clean_raw_df["currency_pair"].nunique(),
                failed_pairs=failed_pairs, failure_rate=failure_rate,
                input_rows=len(clean_raw_df), output_rows=0,
                processing_time_s=0.0, dq_report=dq_report, sla=sla,
                anomalies=[], raw_partition_log=raw_partition_log,
                feat_partition_log={}, airflow_metadata=airflow_metadata, reason=msg,
            )
            raise RuntimeError(msg)

        # ── STAGE 7: Feature engineering ────────────────────────────────
        print("\n[ STAGE 7 ] Feature engineering")
        feat_start = time.time()

        macro_df = read_csv_from_s3(bucket, macro_key)

        new_features = build_fx_features(
            clean_raw_df  = clean_raw_df,
            ticker_ref    = ticker_ref,
            input_parquet = raw_input,
            macro_df      = macro_df,
        )

        processing_time_s = round(time.time() - feat_start, 3)
        print(f"  elapsed: {processing_time_s}s  |  output rows: {len(new_features):,}")

        # ── STAGE 8: Anomaly detection ───────────────────────────────────
        print("\n[ STAGE 8 ] Anomaly detection")
        anomalies = detect_anomalies(new_features, old_rolling_df)

        # ── STAGE 9: Attach lineage metadata ────────────────────────────
        print("\n[ STAGE 9 ] Attach lineage metadata")
        new_features = add_metadata(
            df                = new_features,
            run_id            = run_id,
            run_ts            = run_ts,
            input_source      = input_source,
            input_rows        = len(clean_raw_df),
            output_rows       = len(new_features),
            processing_time_s = processing_time_s,
            start_date        = start_date,
            replay_mode       = replay_from_raw,
            mode_label        = mode_label
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
                end_date=end_date, input_source=input_source,
                expected_pairs=expected_pairs,
                actual_pairs=clean_raw_df["currency_pair"].nunique(),
                failed_pairs=failed_pairs, failure_rate=failure_rate,
                input_rows=len(clean_raw_df), output_rows=len(new_features),
                processing_time_s=processing_time_s, dq_report=dq_report,
                sla=sla, anomalies=anomalies,
                raw_partition_log=raw_partition_log, feat_partition_log={},
                airflow_metadata=airflow_metadata, reason="Feature schema enforcement failed",
            )
            raise

        # ── STAGE 11: Write feature partitions (Layer 2) ────────────────
        print("\n[ STAGE 11 ] Write feature partitions (Layer 2)")
        feat_partition_log = write_feature_layer(
            new_features, bucket, prefix,
            run_id          = run_id,
            force_overwrite = effective_overwrite,
        )

        # ── STAGE 12: Update rolling serving file (Layer 3) ─────────────
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
            end_date=end_date, input_source=input_source,
            expected_pairs=expected_pairs,
            actual_pairs=clean_raw_df["currency_pair"].nunique(),
            failed_pairs=failed_pairs, failure_rate=failure_rate,
            input_rows=len(clean_raw_df), output_rows=len(new_features),
            processing_time_s=processing_time_s, dq_report=dq_report,
            sla=sla, anomalies=anomalies,
            raw_partition_log=raw_partition_log,
            feat_partition_log=feat_partition_log, airflow_metadata=airflow_metadata,
        )

        print(f"\n{'=' * 66}")
        print(f"  FX PIPELINE SUCCESS   run_id={run_id}")
        print(f"  output rows  : {len(new_features):,}")
        print(f"  rolling rows : {len(rolling_df):,}")
        print(f"  feature time : {processing_time_s}s")
        print(f"  anomalies    : {len(anomalies)}")
        print(f"{'=' * 66}\n")

        return rolling_df, f"UPDATED_ROWS_{len(new_features)}"

    except Exception as exc:
        send_alert(
            f"Pipeline failed: {exc}",
            level  = "CRITICAL",
            context = {"run_id": run_id, "mode": mode_label, "error": str(exc)},
        )
        raise

    finally:
        if lock_acquired:
            release_s3_lock(bucket, prefix)



# =============================================================
# RAW REPLAY CONVENIENCE WRAPPER
# =============================================================

def replay_fx_from_raw(
    start_date: str,
    bucket:     str = "yeeshu-fx-bucket",
    prefix:     str = "historical-fx/",
) -> tuple[pd.DataFrame | None, str]:
    """
    Recompute FX features from Layer 1 raw partitions for a given date range.
    Does NOT call yfinance or FRED — fully deterministic and reproducible.

    Each replay produces a new run_id -> new versioned feature partition.
    History is preserved. Rolling layer is updated with recomputed values.

    Airflow conf:  {"start_date": "2024-01-01"}
    DAG setting:   max_active_runs=1
    """
    return update_fx_pipeline(
        bucket               = bucket,
        prefix               = prefix,
        start_date_override  = start_date,
        replay_from_raw      = True,
        force_overwrite      = True,
    )