"""
equity_consumer.py
=========================
Production-grade Spark Structured Streaming consumer for real-time equity ticks.

Storage rules
-------------
READ-ONLY  : s3://yeeshu-equity-bucket   (static positions)
ALL WRITES : s3://risk-platform-pushparag-analytics  (parquet output, DLQ, snapshots)

Storage layout (writes)
-----------------------
Processed output — LIVE (parquet, partitioned):
    s3://risk-platform-pushparag-analytics/equity/data/
        date=YYYY-MM-DD/hour=HH/batch_<batch_id>.parquet

Processed output — REPLAY (parquet, partitioned):
    s3://risk-platform-pushparag-analytics/equity/replay/
        date=YYYY-MM-DD/hour=HH/batch_<batch_id>.parquet

    REPLAY output is ALWAYS isolated from LIVE output.
    REPLAY_MODE=s3 NEVER writes to equity/data/.

Consumer DLQ (S3, parquet, partitioned by day):
    s3://risk-platform-pushparag-analytics/kafka_dlq/consumer/
        year=Y/month=MM/day=DD/dlq_<batch_id>.parquet

Consumer DLQ (Kafka topic, for stream-level routing) — LIVE MODE ONLY:
    topic: equity_stream_dlq
    Not used during replay (Kafka not needed in S3 replay mode).

Raw Kafka storage (for replay):
    s3://risk-platform-pushparag-analytics/kafka_raw/equity/
        year=YYYY/month=MM/day=DD/hour=HH/batch_<batch_id>.parquet

Replay modes
------------
REPLAY_MODE = "kafka"  -> normal Spark Structured Streaming from Kafka
REPLAY_MODE = "s3"     -> reads raw parquet from kafka_raw/equity/ directly,
                         processes through the same pipeline, writes to equity/replay/.
                         Replay NEVER touches Redis or overwrites equity/data/.

Redis policy
------------
Redis represents LIVE state only.
    Live mode  : Redis snapshot published after every batch.
    Replay mode: Redis is NEVER written or published to.

State rebuild policy
--------------------
State rebuild runs ONLY in live mode (run_mode=live, replay_mode!=s3).
State rebuild is NEVER run during replay or backfill.
State rebuild is NEVER run at or near market open — buffers start empty.
State rebuild loads only the recent recovery window (state_rebuild_minutes).
State rebuild NEVER loads previous trading session data (e.g. Friday on Monday).

Late Event Handling
-------------------
Events older than 5 minutes from current time are skipped.
Prevents stale data from corrupting rolling metrics.

Lineage on every output row
----------------------------
    kafka_offset           — exact offset in source partition
    kafka_partition        — source partition
    producer_pipeline_name — value from event envelope
    consumer_pipeline_name — "equity_stream_consumer"
    pipeline_run_id        — UUID per batch (constant within one batch)
    processing_timestamp   — ET timestamp when batch was processed

Idempotency
-----------
    No Redis dependency for dedup — simpler, faster, deterministic
    Spark checkpoints handle offset tracking

Buffer safety
-------------
    MAX_BUFFER_PER_TICKER   — rolling window cap per symbol
    MAX_TOTAL_TICKER_SYMBOLS — evict oldest symbols when total count exceeds cap

Prometheus metrics
------------------
    Exposed on /metrics (port 8000 by default, configurable via METRICS_PORT).
    Prometheus scrapes every 60 s.
    Run-level counters only — no ticker / batch_id labels.

    consumer_batches_total           — total micro-batches processed
    consumer_failures_total          — batches that raised an unhandled exception
    consumer_dlq_total               — total events routed to DLQ
    consumer_processed_s3_writes_total — successful S3 output writes
    consumer_redis_writes_total      — successful Redis snapshot publishes (live only)
    consumer_last_success_timestamp  — Unix timestamp of last fully successful batch
    consumer_duration_seconds        — histogram of per-batch wall-clock duration

    Replay metrics (incremented when REPLAY_MODE=s3):
    replay_jobs_total                — total replay jobs started
    replay_records_processed_total   — total records processed across all replay batches
    replay_failures_total            — replay partitions that failed
    replay_duration_seconds          — total wall-clock time of the replay job
"""

import os
import json
import uuid
import time
import boto3
import pandas as pd
import numpy as np
from io import BytesIO
from datetime import datetime, timedelta
from typing import List, Dict
import requests
import pendulum
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, explode
from pyspark.sql.types import (
    StructType, StructField, StringType, FloatType, ArrayType
)
import redis
from kafka import KafkaProducer
from prometheus_client import (
    Counter, Gauge, Histogram, CollectorRegistry,
    push_to_gateway, start_http_server,
)

# =============================================================
# CONFIG  —  single source of truth.  No magic numbers in code.
# =============================================================

CONFIG: dict = {
    # Kafka
    "kafka_broker":         os.getenv("KAFKA_BROKER", "kafka:9092"),
    "topic":                "equity_stream",
    "dlq_topic":            "equity_stream_dlq",

    # Run mode
    "run_mode": os.getenv("EQUITY_MODE", "live").lower(),

    # Replay
    "replay_mode":          os.getenv("EQUITY_REPLAY_MODE", "kafka"),   # "kafka" | "s3"
    "replay_date":          os.getenv("EQUITY_REPLAY_DATE", ""),         # "YYYY-MM-DD" for s3 replay
    "replay_hour":          os.getenv("EQUITY_REPLAY_HOUR", ""),         # "HH" optional filter

    # S3
    "read_bucket":          "yeeshu-equity-bucket",
    "write_bucket":         "risk-platform-pushparag-analytics",
    "positions_key":        "historical-equity/final_merged.parquet",
    # Live output — never touched by replay
    "output_prefix":        "equity/data",
    # Replay output — never touches live output
    "replay_output_prefix": "equity/replay",
    "consumer_dlq_prefix":  "kafka_dlq/equity/consumer",
    "raw_replay_prefix":    "kafka_raw/equity/backfill/",
    "checkpoint_dir":       os.getenv("CHECKPOINT_DIR","s3a://risk-platform-pushparag-analytics") + "/equity/checkpoints",

    # Raw Kafka storage (for state rebuild - live recovery only)
    "raw_state_rebuild_prefix": "kafka_raw/equity/live/",  

    # Late event handling
    "late_event_max_minutes":   5,

    # Buffer safety (single definition)
    "max_buffer_per_ticker":    120,
    "max_total_ticker_symbols": 500,

    # Redis (snapshots only — dedup removed; only used in live mode)
    "redis_host":           os.getenv("REDIS_HOST", "localhost"),
    "redis_port":           int(os.getenv("REDIS_PORT", 6379)),
    "redis_db_stream":      int(os.getenv("REDIS_DB_STREAM", 1)),

    # Lineage
    "consumer_pipeline_name":   "equity_stream_consumer",
    "data_source":               "kafka_equity_stream",
    "transformation":            "intraday_metrics_v1",

    # Spark
    "spark_app_name":       "EquityKafkaStreaming_Production",
    "spark_shuffle_partitions": "4",

    # Positions columns to drop
    "positions_drop_cols": [
        "option_type", "Discretion", "other_manager",
        "Sole", "Shared", "None", "beta",
        "dividendYield", "gdp", "unrate", "cpi", "fedfunds", "date"
    ],

    # Prometheus
    "pushgateway_url":      os.getenv("PUSHGATEWAY_URL"),
    "metrics_port":         int(os.getenv("METRICS_PORT", "8000")),

    # Market Hours (Shutdown with 3-minute extension)
    "market_shutdown_hour": 16,
    "market_shutdown_minute": 3,

    # State rebuild — live mode recovery only.
    # Number of minutes of raw S3 data to load when recovering mid-session.
    # Must be > 0 and < the longest rolling window in compute_metrics (15m).
    # Set to 15 to ensure vol_15m can warm up immediately after recovery.
    "state_rebuild_minutes": 15,

    # Grace period after market open during which state rebuild is SKIPPED.
    # If startup occurs within market_open + state_rebuild_grace_minutes,
    # buffers start empty (new session, nothing to recover).
    # Matches state_rebuild_minutes so that the two windows are symmetric.
    "state_rebuild_grace_minutes": 15,
}

# =============================================================
# REPLAY / BACKFILL MODE DETECTION  —  consulted throughout the module
# =============================================================

IS_REPLAY   = CONFIG["replay_mode"] == "s3"
IS_BACKFILL = CONFIG["run_mode"] == "backfill"

# =============================================================
# PROMETHEUS  — run-level counters, no ticker/batch labels
# =============================================================

_prom_registry = CollectorRegistry()

# Consumer metrics (live + replay share these)
PROM_BATCHES_TOTAL = Counter(
    "consumer_batches_total",
    "Total micro-batches processed",
    registry=_prom_registry,
)
PROM_FAILURES_TOTAL = Counter(
    "consumer_failures_total",
    "Batches that raised an unhandled exception",
    registry=_prom_registry,
)
PROM_DLQ_TOTAL = Counter(
    "consumer_dlq_total",
    "Total events routed to DLQ",
    registry=_prom_registry,
)
PROM_S3_WRITES_TOTAL = Counter(
    "consumer_processed_s3_writes_total",
    "Successful S3 output parquet writes",
    registry=_prom_registry,
)
PROM_REDIS_WRITES_TOTAL = Counter(
    "consumer_redis_writes_total",
    "Successful Redis snapshot publishes (live mode only)",
    registry=_prom_registry,
)
PROM_RECORDS_PROCESSED_TOTAL = Counter(
    "consumer_records_processed_total",
    "Total records processed",
    registry=_prom_registry,
)
PROM_LAST_SUCCESS_TIMESTAMP = Gauge(
    "consumer_last_success_timestamp",
    "Unix timestamp of the last fully successful batch",
    registry=_prom_registry,
)
PROM_DURATION_SECONDS = Histogram(
    "consumer_duration_seconds",
    "Wall-clock duration of each batch in seconds",
    buckets=[0.1, 0.5, 1, 2, 5, 10, 30, 60, 120],
    registry=_prom_registry,
)

# Replay-specific metrics
PROM_REPLAY_JOBS_TOTAL = Counter(
    "replay_jobs_total",
    "Total replay jobs started (REPLAY_MODE=s3)",
    registry=_prom_registry,
)
PROM_REPLAY_RECORDS_TOTAL = Counter(
    "replay_records_processed_total",
    "Total records processed across all replay batches",
    registry=_prom_registry,
)
PROM_REPLAY_FAILURES_TOTAL = Counter(
    "replay_failures_total",
    "Replay partitions that failed",
    registry=_prom_registry,
)
PROM_REPLAY_DURATION_SECONDS = Histogram(
    "replay_duration_seconds",
    "Total wall-clock duration of a replay job in seconds",
    buckets=[1, 5, 10, 30, 60, 300, 600, 1800, 3600],
    registry=_prom_registry,
)

# Shared run_id for Pushgateway job label (set in __main__)
_producer_run_id: str = "unknown"


def _push_metrics() -> None:
    """
    Push accumulated Prometheus metrics to Pushgateway.
    Mirrors the Airflow pattern: push_to_gateway(url, job=<pipeline>_<run_id>, registry).
    Non-fatal — a Pushgateway failure must never stop the consumer.
    """
    try:
        push_to_gateway(
            CONFIG["pushgateway_url"],
            job=f"{CONFIG['consumer_pipeline_name']}_{_producer_run_id}",
            registry=_prom_registry,
        )
    except Exception as exc:
        log("WARNING", "Prometheus push_to_gateway failed", {"error": str(exc)})


def _start_metrics_server() -> None:
    """
    Expose /metrics on a background HTTP server for Prometheus scraping.
    Passes _prom_registry explicitly so all custom metrics are visible at /metrics.
    The same registry is used for push_to_gateway, ensuring consistency between
    scraped and pushed metric values.
    """
    try:
        start_http_server(CONFIG["metrics_port"], registry=_prom_registry)
        log("INFO", "Prometheus /metrics server started",
            {"port": CONFIG["metrics_port"]})
    except Exception as exc:
        log("WARNING", "Prometheus HTTP server failed to start",
            {"error": str(exc)})

# =============================================================
# ALERT
# =============================================================

def send_alert(message: str):
    log("CRITICAL", f"[ALERT] {message}")

    webhook = os.getenv("SLACK_WEBHOOK_URL")
    if not webhook:
        return

    try:
        text = f"* [CRITICAL] equity_kafka_consumer *\n{message}"
        requests.post(webhook, json={"text": text}, timeout=3)
    except Exception as e:
        print(f"[ALERT ERROR] Slack failed: {e}")

# =============================================================
# STRUCTURED LOGGING
# =============================================================

def log(level: str, message: str, context: dict = None) -> None:
    """
    Emit a structured JSON log line.
    Wire to CloudWatch / Datadog / ELK by replacing print in production.
    """
    if context is None:
        context = {}
    record = {
        "ts":       pendulum.now("America/New_York").to_iso8601_string(),
        "level":    level,
        "pipeline": CONFIG["consumer_pipeline_name"],
        "msg":      message,
        **context,
    }
    print(json.dumps(record, default=str))

# =============================================================
# REDIS  —  snapshots ONLY (live mode only; replay never touches Redis)
# =============================================================

# redis_client is initialised lazily so that S3-replay mode can start
# even if Redis is unavailable (replay has no dependency on Redis).
_redis_client = None


def _get_redis():
    """
    Return the shared Redis client, creating it on first call.
    Only called from save_latest_snapshot_all_tickers which is already
    guarded by IS_REPLAY — so this is never reached during replay.
    """
    global _redis_client
    if _redis_client is None:
        _redis_client = redis.Redis(
            host             = CONFIG["redis_host"],
            port             = CONFIG["redis_port"],
            db               = CONFIG["redis_db_stream"],
            decode_responses = True,
        )
    return _redis_client

# =============================================================
# KAFKA DLQ PRODUCER  (live mode only)
# =============================================================

# _dlq_producer is initialised lazily so that replay mode never attempts
# a Kafka connection (replay has no Kafka dependency).
_dlq_producer = None


def _get_dlq_producer():
    """
    Return the shared Kafka DLQ producer, creating it on first call.
    Only called in live mode — replay routes DLQ to S3 only, not Kafka.
    """
    global _dlq_producer
    if _dlq_producer is None:
        _dlq_producer = KafkaProducer(
            bootstrap_servers = CONFIG["kafka_broker"],
            value_serializer  = lambda v: json.dumps(v, default=str).encode("utf-8"),
        )
    return _dlq_producer


def send_to_kafka_dlq(payload: dict) -> None:
    """
    Send a single DLQ record to the Kafka DLQ topic (non-blocking).
    LIVE MODE ONLY — never called during replay.
    """
    try:
        _get_dlq_producer().send(CONFIG["dlq_topic"], payload)
    except Exception as exc:
        log(
            "ERROR",
            "Kafka DLQ send failed",
            {"error": str(exc)},
        )

# =============================================================
# S3 HELPERS  —  atomic writes
# =============================================================

def get_s3():
    return boto3.client("s3")


def s3_key_exists(bucket: str, key: str) -> bool:
    try:
        get_s3().head_object(Bucket=bucket, Key=key)
        return True
    except Exception:
        return False


def atomic_write_parquet_to_s3(df: pd.DataFrame, bucket: str, key: str) -> None:
    """
    Atomically write DataFrame as parquet to S3.
    PUT -> _temp/<key> -> COPY -> DELETE temp.
    Final key is either complete or absent — never partial.
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

    except Exception as exc:
        log("ERROR", "S3 atomic write failed",
            {"bucket": bucket, "key": key, "error": str(exc)})
        raise
    finally:
        try:
            s3.delete_object(Bucket=bucket, Key=temp_key)
        except Exception:
            pass


def list_s3_files_with_prefix(bucket: str, prefix: str) -> List[str]:
    """List all parquet files with given prefix from S3."""
    s3 = get_s3()
    try:
        paginator = s3.get_paginator("list_objects_v2")
        pages     = paginator.paginate(Bucket=bucket, Prefix=prefix)
        keys = [
            obj["Key"]
            for page in pages
            for obj in page.get("Contents", [])
            if obj["Key"].endswith(".parquet")
        ]
        return keys
    except Exception as exc:
        log("ERROR", "Failed to list S3 files",
            {"bucket": bucket, "prefix": prefix, "error": str(exc)})
        return []


def read_parquet_from_s3(bucket: str, key: str) -> pd.DataFrame:
    """Read a single parquet file from S3. Returns empty DataFrame on error."""
    try:
        obj = get_s3().get_object(Bucket=bucket, Key=key)
        df  = pd.read_parquet(BytesIO(obj["Body"].read()))
        return df
    except Exception as exc:
        log("ERROR", "Failed to read parquet from S3",
            {"bucket": bucket, "key": key, "error": str(exc)})
        return pd.DataFrame()


def flush_consumer_dlq_to_s3(
    dlq_buffer: list,
    batch_id:   str,
) -> None:
    """
    Batch-flush consumer DLQ records to S3 as parquet (atomic write).
    Called once per batch — not per failed event.

    Path: s3://risk-platform-pushparag-analytics/kafka_dlq/consumer/
              year=Y/month=MM/day=DD/dlq_<batch_id>.parquet

    Works in both live and replay mode — DLQ is always written to S3.
    """
    if not dlq_buffer:
        return

    df  = pd.DataFrame(dlq_buffer)
    now = pendulum.now("America/New_York")
    key = (
        f"{CONFIG['consumer_dlq_prefix']}/"
        f"year={now.year}/month={now.month:02d}/day={now.day:02d}/"
        f"dlq_{batch_id}.parquet"
    )

    try:
        atomic_write_parquet_to_s3(df, CONFIG["write_bucket"], key)
        log("WARNING", "Consumer DLQ batch flushed to S3",
            {"batch_id": batch_id, "failed_rows": len(df), "key": key})
    except Exception as exc:
        log("ERROR", "Consumer DLQ S3 flush failed — dumping to stdout",
            {"batch_id": batch_id, "error": str(exc)})
        send_alert(f"DLQ S3 flush FAILED | batch_id={batch_id} | error={str(exc)}")
        for entry in dlq_buffer:
            print(json.dumps({"CONSUMER_DLQ_FALLBACK": entry}, default=str))

# =============================================================
# STATIC POSITIONS  (read-only from equity-bucket)
# =============================================================

def load_positions() -> pd.DataFrame:
    """
    Load static position data from S3 (read-only bucket).
    Drops non-essential columns. Cached once at startup.
    """
    log("INFO", "Loading static positions",
        {"bucket": CONFIG["read_bucket"], "key": CONFIG["positions_key"]})
    try:
        obj = get_s3().get_object(
            Bucket=CONFIG["read_bucket"],
            Key=CONFIG["positions_key"],
        )
        df = pd.read_parquet(BytesIO(obj["Body"].read()))
        df = df.drop(
            columns=[c for c in CONFIG["positions_drop_cols"] if c in df.columns]
        )
        log("INFO", "Positions loaded",
            {"rows": len(df),
             "tickers": df["ticker"].nunique() if "ticker" in df.columns else "N/A"})
        return df
    except Exception as exc:
        log("ERROR", "Failed to load positions", {"error": str(exc)})
        raise


# Global ticker buffer — shared across batches within the same executor
ticker_buffers: dict = {}


def update_ticker_buffer(ticker: str, row_dict: dict) -> None:
    """
    Add a row to the per-ticker rolling buffer.

    Safety guards:
    1. MAX_BUFFER_PER_TICKER: trim oldest rows if buffer exceeds cap.
    2. MAX_TOTAL_TICKER_SYMBOLS: evict least-recently-updated symbol
       if total symbol count exceeds cap.
    """
    global ticker_buffers

    # Guard 1: evict excess symbols (FIFO — remove oldest key)
    if (ticker not in ticker_buffers and
            len(ticker_buffers) >= CONFIG["max_total_ticker_symbols"]):
        evict_key = next(iter(ticker_buffers))
        del ticker_buffers[evict_key]
        log("WARNING", "Ticker buffer symbol evicted — max_total_ticker_symbols reached",
            {"evicted": evict_key, "total_symbols": len(ticker_buffers)})

    if ticker not in ticker_buffers:
        ticker_buffers[ticker] = []

    ticker_buffers[ticker].append(row_dict)

    # Guard 2: trim per-ticker buffer if over cap
    cap = CONFIG["max_buffer_per_ticker"]
    if len(ticker_buffers[ticker]) > cap:
        overflow = len(ticker_buffers[ticker]) - cap
        ticker_buffers[ticker] = ticker_buffers[ticker][overflow:]


# =============================================================
# STATE REBUILD  —  LIVE MODE RECOVERY ONLY
# =============================================================

def _get_now_et():
    """
    Return current time in Eastern Time (US/Eastern).
    Handles Daylight Saving Time automatically.
    """
    return pendulum.now('America/New_York')


def _is_equity_market_open_today(now_et: datetime) -> bool:
    """
    Return True if today is a weekday (Mon-Fri).
    NYSE is Mon-Fri.
    """
    return now_et.weekday() < 5  # 0=Mon … 4=Fri


def _equity_session_open_et(now_et: datetime) -> datetime:
    """
    Return today's NYSE market open time in Eastern Time.
    NYSE opens at 09:30 ET.
    """
    return now_et.set(hour=9, minute=30, second=0, microsecond=0)


def _should_skip_equity_state_rebuild(now_et: datetime) -> bool:
    """
    Return True if state rebuild must be skipped.

    Skip rebuild when any of the following are true:
    1. Today is not a trading day (weekend).
    2. Current time is before today's market open (pre-market startup).
    3. Current time is within the grace window after market open
       (market_open + state_rebuild_grace_minutes) — fresh session start.
    """
    if not _is_equity_market_open_today(now_et):
        return True

    market_open = _equity_session_open_et(now_et)
    grace_cutoff = market_open.add(minutes=CONFIG["state_rebuild_grace_minutes"])

    if now_et < market_open:
        return True

    if now_et < grace_cutoff:
        return True

    return False


def rebuild_state_from_s3() -> None:
    """
    Populate ticker_buffers with recent raw S3 data for live mode recovery.

    PURPOSE
    -------
    When the consumer restarts mid-session due to a crash, pod restart, or
    Kafka/Spark failure, the in-memory ticker_buffers are lost. Without rebuild,
    rolling metrics (return_5m, vol_15m, trend_slope_5m) produce NaN for the
    first ~15 minutes after recovery.

    This function restores enough intraday history to allow those metrics
    to compute immediately after restart.

    WHEN IT RUNS
    ------------
    ONLY when ALL of the following are true:
      - run_mode == "live"
      - replay_mode != "s3" (not IS_REPLAY)
      - IS_BACKFILL is False
      - Market is currently open (between 9:30 ET and 16:03 ET)
      - Startup is NOT within the grace window after market open

    NEVER runs during:
      - Replay (IS_REPLAY = True)
      - Backfill (IS_BACKFILL = True)
      - Pre-market or near-open startups (fresh session)
      - Weekends or after-market close

    WHAT IT LOADS
    -------------
    Scans kafka_raw/equity/backfill/ for partitions that fall within:
        [now - state_rebuild_minutes, now]

    Only the current trading day is scanned. The function determines which
    hour partitions overlap with the rebuild window and requests only those.

    DST HANDLING
    ------------
    Uses Eastern Time (America/New_York) with pendulum for automatic DST handling.
    S3 paths still use ET for partitioning.
    """
    # ── Guard: only live, non-replay, non-backfill ─────────────────────
    if IS_REPLAY or IS_BACKFILL:
        log("INFO", "State rebuild skipped — not in live mode",
            {"IS_REPLAY": IS_REPLAY, "IS_BACKFILL": IS_BACKFILL})
        return

    if CONFIG["run_mode"] != "live":
        log("INFO", "State rebuild skipped — run_mode is not live",
            {"run_mode": CONFIG["run_mode"]})
        return

    # ── Get current time in Eastern Time (handles DST) ─────────
    now_et = _get_now_et()

    # ── Guard: skip rebuild for fresh session starts or closed market ───
    if _should_skip_equity_state_rebuild(now_et):
        log("INFO", "State rebuild skipped — fresh session start or market closed",
            {"now_et":        now_et.isoformat(),
             "weekday":       now_et.weekday(),
             "grace_minutes": CONFIG["state_rebuild_grace_minutes"]})
        return

    rebuild_minutes = CONFIG["state_rebuild_minutes"]

    rebuild_start_et = pd.Timestamp(
        now_et - timedelta(minutes=rebuild_minutes)
    )

    now_et = pd.Timestamp(now_et)

    log("INFO", "Starting equity state rebuild",
        {"rebuild_start_et": rebuild_start_et.isoformat(),
         "rebuild_end_et":   now_et.isoformat(),
         "window_minutes":   rebuild_minutes,
         "current_et":       now_et.isoformat()})

    # ── Determine which hour partitions to scan (using ET) ────────────
    hours_to_scan = set()
    cursor = rebuild_start_et
    while cursor <= now_et:
        hours_to_scan.add(cursor.hour)
        cursor += timedelta(hours=1)

    base_prefix = CONFIG["raw_state_rebuild_prefix"]
    if not base_prefix.endswith("/"):
        base_prefix += "/"

    today_prefix = (
        f"{base_prefix}"
        f"year={now_et.year}/month={now_et.month:02d}/day={now_et.day:02d}/"
    )

    # Collect keys only from required hour partitions
    all_keys: List[str] = []
    s3 = get_s3()
    for hour in sorted(hours_to_scan):
        hour_prefix = f"{today_prefix}hour={hour:02d}/"
        try:
            paginator = s3.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=CONFIG["write_bucket"], Prefix=hour_prefix)
            for page in pages:
                for obj in page.get("Contents", []):
                    if obj["Key"].endswith(".parquet"):
                        all_keys.append(obj["Key"])
        except Exception as exc:
            log("WARNING", "State rebuild: failed to list S3 prefix",
                {"prefix": hour_prefix, "error": str(exc)})

    if not all_keys:
        log("INFO", "State rebuild: no raw S3 files found in recovery window",
            {"today_prefix": today_prefix, "hours": sorted(hours_to_scan)})
        return

    log("INFO", "State rebuild: raw S3 files found",
        {"file_count": len(all_keys), "hours": sorted(hours_to_scan)})

    # ── Read, filter to rebuild window, and populate ticker_buffers ─────
    frames: List[pd.DataFrame] = []
    for key in all_keys:
        try:
            obj = s3.get_object(Bucket=CONFIG["write_bucket"], Key=key)
            df = pd.read_parquet(BytesIO(obj["Body"].read()))
            if df.empty:
                continue
            frames.append(df)
        except Exception as exc:
            log("WARNING", "State rebuild: failed to read S3 file",
                {"key": key, "error": str(exc)})

    if not frames:
        log("INFO", "State rebuild: all files were empty or unreadable")
        return

    combined = pd.concat(frames, ignore_index=True)

    # Filter to only events within the rebuild window
    if "timestamp" not in combined.columns:
        log("WARNING", "State rebuild: timestamp column missing — skipping rebuild")
        return

    combined["_evt_dt"] = (
        pd.to_datetime(
            combined["timestamp"],
            utc=True,
            errors="coerce"
        )
        .dt.tz_convert("America/New_York")
    )

    combined = combined.dropna(subset=["_evt_dt"])

    combined = combined[
        (combined["_evt_dt"] >= rebuild_start_et)
        & (combined["_evt_dt"] <= now_et)
    ]

    if combined.empty:
        log("INFO", "State rebuild: no events in rebuild window after filtering",
            {"rebuild_start_et": rebuild_start_et.isoformat(),
             "now_et":           now_et.isoformat()})
        return

    # Sort chronologically
    combined = combined.sort_values("_evt_dt").reset_index(drop=True)
    combined = combined.drop(columns=["_evt_dt"])

    # Required OHLCV columns
    required_cols = {"ticker", "open", "high", "low", "close", "volume"}
    missing = required_cols - set(combined.columns)
    if missing:
        log("WARNING", "State rebuild: required columns missing — skipping rebuild",
            {"missing_columns": list(missing)})
        return

    records_loaded = 0
    for _, row in combined.iterrows():
        ticker = row.get("ticker")
        if not ticker:
            continue

        try:
            close = float(row["close"])
            high = float(row["high"])
            low = float(row["low"])
            volume = float(row["volume"])
        except (TypeError, ValueError):
            continue

        if close <= 0 or high < low:
            continue

        row_dict = row.to_dict()
        update_ticker_buffer(ticker, row_dict)
        records_loaded += 1

    log("INFO", "Equity state rebuild complete",
        {"records_loaded": records_loaded,
         "tickers_in_buffer": len(ticker_buffers),
         "rebuild_window_minutes": rebuild_minutes})

# =============================================================
# METRICS PER BATCH
# =============================================================

def make_batch_metrics() -> dict:
    return {
        "events_received":  0,
        "events_processed": 0,
        "events_failed":    0,
        "events_deduped":   0,
        "events_late":      0,
        "batch_latency_s":  0.0,
    }


def log_batch_metrics(batch_id: str, metrics: dict) -> None:
    log("INFO", "Batch metrics", {"batch_id": batch_id, **metrics})

# =============================================================
# FEATURE ENGINEERING  (unchanged from original)
# =============================================================

def compute_metrics(buffer: list) -> dict:
    """
    Compute intraday rolling metrics from the per-ticker buffer.
    Business logic preserved exactly from original pipeline.
    """
    closes  = np.array([r["close"]  for r in buffer])
    highs   = np.array([r["high"]   for r in buffer])
    lows    = np.array([r["low"]    for r in buffer])
    volumes = np.array([r["volume"] for r in buffer])

    # ---------------------------------------------------------
    # Time-based windows
    # ---------------------------------------------------------
    try:
        latest_ts = pd.to_datetime(
            buffer[-1].get("timestamp"),
            utc=True,
            errors="coerce"
        ).floor("min")

        if pd.isna(latest_ts):
            latest_ts = None
    except Exception:
        latest_ts = None

    # Build time windows from latest timestamp
    window_1m = []
    window_5m = []
    window_15m = []

    if latest_ts is not None:
        cutoff_1m = latest_ts - pd.Timedelta(minutes=1)
        cutoff_5m = latest_ts - pd.Timedelta(minutes=5)
        cutoff_15m = latest_ts - pd.Timedelta(minutes=15)

        for r in buffer:
            ts = pd.to_datetime(
                r.get("timestamp"),
                utc=True,
                errors="coerce"
            ).floor("min")

            if pd.isna(ts):
                continue

            if ts >= cutoff_1m:
                window_1m.append(r)

            if ts >= cutoff_5m:
                window_5m.append(r)

            if ts >= cutoff_15m:
                window_15m.append(r)

    metrics = {
        "return_1m":         np.nan,
        "return_5m":         np.nan,
        "vol_15m":           np.nan,
        "range_pct_1m":      np.nan,
        "rolling_vwap_5m":   np.nan,
        "close_diff":        np.nan,
        "rolling_high_5m":   np.nan,
        "rolling_low_5m":    np.nan,
        "trend_slope_5m":    np.nan,
        "breakout_strength": np.nan,
        "volume_burst":      np.nan,
    }

    if len(closes) >= 2:
        # Previous observation difference (keep as-is)
        metrics["close_diff"] = closes[-1] - closes[-2]

    if len(window_1m) >= 2:

        first_close = window_1m[0]["close"]
        last_close  = window_1m[-1]["close"]

        if pd.notna(first_close) and first_close != 0:

            metrics["return_1m"] = (
                last_close - first_close
            ) / first_close

            latest_high = window_1m[-1]["high"]
            latest_low  = window_1m[-1]["low"]

            metrics["range_pct_1m"] = (
                latest_high - latest_low
            ) / first_close

    if len(window_5m) >= 2:

        first_close = window_5m[0]["close"]
        last_close  = window_5m[-1]["close"]

        if pd.notna(first_close) and first_close != 0:
            metrics["return_5m"] = (
                last_close - first_close
            ) / first_close

            metrics["trend_slope_5m"] = (
                last_close - first_close
            ) / 5

    if len(window_15m) >= 2:

        closes_15m = np.array(
            [r["close"] for r in window_15m],
            dtype=float
        )

        returns = (
            closes_15m[1:] - closes_15m[:-1]
        ) / closes_15m[:-1]

        returns = returns[np.isfinite(returns)]

        if len(returns) > 0:
            metrics["vol_15m"] = float(
                np.std(np.log1p(returns))
            )

    if len(window_5m) > 0:

        closes_5m = np.array(
            [r["close"] for r in window_5m],
            dtype=float
        )

        highs_5m = np.array(
            [r["high"] for r in window_5m],
            dtype=float
        )

        lows_5m = np.array(
            [r["low"] for r in window_5m],
            dtype=float
        )

        volumes_5m = np.array(
            [r["volume"] for r in window_5m],
            dtype=float
        )

        volume_sum = np.sum(volumes_5m)

        if volume_sum > 0:
            metrics["rolling_vwap_5m"] = float(
                np.sum(closes_5m * volumes_5m)
                / volume_sum
            )

        metrics["rolling_high_5m"] = float(np.max(highs_5m))
        metrics["rolling_low_5m"] = float(np.min(lows_5m))

        high5 = np.max(highs_5m)
        low5 = np.min(lows_5m)
        rng = high5 - low5

        if rng > 0:
            metrics["breakout_strength"] = (
                closes_5m[-1] - low5
            ) / rng


    if len(window_5m) >= 2:

        vols = np.array(
            [r["volume"] for r in window_5m],
            dtype=float
        )

        avg_volume = np.mean(vols[:-1])

        if avg_volume > 0:
            metrics["volume_burst"] = float(
                vols[-1] / avg_volume
            )

    return metrics


def enrich_intraday_with_positions(
    df:           pd.DataFrame,
    positions_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Merge static position data and compute intraday exposure + PnL.
    Business logic preserved exactly from original pipeline.
    """
    if df.empty:
        return df

    df = df.merge(positions_df, on="ticker", how="left")

    df["intraday_exposure"] = df["Shares"] * df["close"]

    df["intraday_pnl"] = np.where(
        df["close_diff"].notna(),
        df["Shares"] * df["close_diff"],
        np.nan,
    )

    df["portfolio_intraday_pnl"] = (
        df.groupby("asset_manager")["intraday_pnl"].transform("sum")
    )

    df["portfolio_intraday_exposure"] = (
        df.groupby("asset_manager")["intraday_exposure"].transform("sum")
    )

    return df

# =============================================================
# SAVE OUTPUT  (atomic S3 write, partitioned by date + hour)
# =============================================================

def save_to_parquet(df: pd.DataFrame, batch_id: str, is_replay: bool = False) -> None:
    """
    Write processed batch to S3 as parquet using atomic write.
    Partitioned by date and hour — never overwrites existing batches.

    LIVE mode  : writes to equity/data/date=.../hour=.../batch_<id>.parquet
    REPLAY mode: writes to equity/replay/date=.../hour=.../batch_<id>.parquet

    The is_replay flag ensures replay output is ALWAYS isolated from live output.
    Replay can never overwrite equity/data/ regardless of configuration.
    """
    if df.empty:
        return

    # Select the correct output prefix based on mode — never mix them
    output_prefix = CONFIG["replay_output_prefix"] if is_replay else CONFIG["output_prefix"]

    try:
        ts             = pd.to_datetime(df["timestamp"].iloc[0])
        partition_date = ts.strftime("%Y-%m-%d")
        partition_hour = ts.strftime("%H")

        key = (
            f"{output_prefix}/"
            f"date={partition_date}/hour={partition_hour}/"
            f"batch_{batch_id}.parquet"
        )
        atomic_write_parquet_to_s3(df, CONFIG["write_bucket"], key)
        PROM_S3_WRITES_TOTAL.inc()

    except Exception as exc:
        log("ERROR", "save_to_parquet failed",
            {"batch_id": batch_id, "is_replay": is_replay, "error": str(exc)})
        send_alert(f"S3 write FAILED | batch_id={batch_id} | error={str(exc)}")

# =============================================================
# SNAPSHOT TO REDIS  (LIVE MODE ONLY)
# =============================================================

def save_latest_snapshot_all_tickers(
    ticker_buffers: dict,
    positions_df:   pd.DataFrame,
) -> None:
    """
    Build and publish a per-ticker snapshot to Redis.

    LIVE MODE ONLY — this function must never be called during replay.
    The call site (process_batch) enforces this via the is_replay parameter.

    Business logic preserved exactly from original pipeline.
    Adds prev_OHLCV columns from the second-to-last buffer row.
    """
    latest_rows = []

    for ticker, buffer in ticker_buffers.items():
        if not buffer:
            continue

        latest_row = buffer[-1].copy()

        if len(buffer) >= 2:
            prev = buffer[-2]
            latest_row.update({
                "prev_open":   prev.get("open",   np.nan),
                "prev_high":   prev.get("high",   np.nan),
                "prev_low":    prev.get("low",    np.nan),
                "prev_close":  prev.get("close",  np.nan),
                "prev_volume": prev.get("volume", np.nan),
            })
        else:
            latest_row.update({
                "prev_open": np.nan, "prev_high":   np.nan,
                "prev_low":  np.nan, "prev_close":  np.nan,
                "prev_volume": np.nan,
            })

        latest_rows.append(latest_row)

    if not latest_rows:
        return

    try:
        df_latest     = pd.DataFrame(latest_rows)
        df_latest     = enrich_intraday_with_positions(df_latest, positions_df)
        df_latest     = df_latest.where(pd.notnull(df_latest), None)
        snapshot_json = df_latest.to_json(orient="records")

        rc = _get_redis()
        rc.set("equity_latest_snapshot", snapshot_json)
        rc.publish("equity_stream", snapshot_json)

        PROM_REDIS_WRITES_TOTAL.inc()

    except Exception as exc:
        log("ERROR", "Redis snapshot publish failed", {"error": str(exc)})
        send_alert(f"Redis snapshot FAILED | error={str(exc)}")

# =============================================================
# ADD LINEAGE TO OUTPUT ROWS
# =============================================================

def add_lineage(
    row_dict:             dict,
    batch_id:             str,
    pipeline_run_id:      str,
    processing_timestamp: str,
    is_replay:            bool,
) -> dict:
    """
    Stamp lineage fields onto an output row dict.
    All required lineage fields are set here; values are constant
    within a batch (pipeline_run_id, processing_timestamp) or per-row
    (kafka_offset, kafka_partition, producer_pipeline_name).
    """
    row_dict["consumer_pipeline_name"] = CONFIG["consumer_pipeline_name"]
    row_dict["pipeline_run_id"]        = pipeline_run_id
    row_dict["processing_timestamp"]   = processing_timestamp
    row_dict["data_source"]            = CONFIG["data_source"]
    row_dict["transformation"]         = CONFIG["transformation"]
    row_dict["record_created_at"]      = processing_timestamp
    row_dict["run_mode"]               = ("replay" if is_replay else CONFIG["run_mode"])
    return row_dict

# =============================================================
# PROCESS BATCH  (core consumer logic — live and replay share this)
# =============================================================

def process_batch(
    batch_df,
    batch_id:     int,
    positions_df: pd.DataFrame,
    is_replay:    bool = False,
) -> None:
    """
    Spark foreachBatch handler AND replay batch processor.
    Called once per micro-batch (live) or per S3 partition (replay).

    is_replay flag controls:
      - Output path: equity/replay/ vs equity/data/
      - Redis: SKIPPED entirely when is_replay=True
      - Kafka DLQ publish: SKIPPED when is_replay=True (S3 DLQ only)

    Per-batch lifecycle:
      1. Convert Spark -> pandas
      2. Filter late events (older than late_event_max_minutes)
         [late filtering uses wall-clock time; during replay, historical
         events will ALWAYS appear "late" by wall clock — late filtering
         is therefore DISABLED for replay to prevent all events being dropped]
      3. For each row: validation -> buffer update -> metrics -> lineage
      4. Enrich with positions
      5. Atomic S3 write (replay -> equity/replay/, live -> equity/data/)
      6. Redis snapshot [LIVE ONLY — skipped when is_replay=True]
      7. Flush consumer DLQ (S3 always; Kafka topic only in live mode)
      8. Push Prometheus metrics

    Idempotency:
      - Spark checkpoints handle offset tracking (live mode)
      - Duplicate replay runs are idempotent (parquet append is safe)

    Partial failure handling:
      Exceptions within a row's processing are caught individually.
      Failed rows are collected in dlq_buffer and written once at end.
      Successful rows are never rolled back by a subsequent row's failure.
    """
    pipeline_run_id      = str(uuid.uuid4())
    processing_timestamp = pendulum.now("America/New_York").to_iso8601_string()
    metrics              = make_batch_metrics()
    dlq_buffer:   list   = []
    batch_start          = time.monotonic()
    current_time = pd.Timestamp.now(tz="America/New_York")
    late_cutoff = current_time - pd.Timedelta(minutes=CONFIG["late_event_max_minutes"])

    PROM_BATCHES_TOTAL.inc()

    # ── Convert Spark batch to pandas ─────────────────────────────────
    try:
        if batch_df.count() == 0:
            log("INFO", "Empty batch received",
                {"batch_id": batch_id, "is_replay": is_replay})
            return
        pdf = batch_df.toPandas()
    except Exception as exc:
        log("ERROR", "Failed to convert batch to pandas",
            {"batch_id": batch_id, "is_replay": is_replay, "error": str(exc)})
        send_alert(f"Batch conversion FAILED | batch_id={batch_id} | error={str(exc)}")
        PROM_FAILURES_TOTAL.inc()
        return

    # ── Step 1: Deduplicate by timestamp (pure data logic) ─────────────
    original_count = len(pdf)
    pdf = pdf.drop_duplicates(
        subset=["ticker", "timestamp"]
    )
    deduped_count  = len(pdf)
    metrics["events_deduped"] = original_count - deduped_count

    if deduped_count == 0:
        log("INFO", "All events in batch were duplicates",
            {"batch_id": batch_id, "original_count": original_count})
        metrics["batch_latency_s"] = round(time.monotonic() - batch_start, 3)
        log_batch_metrics(str(batch_id), metrics)
        return

    # ── Step 2: Late event filtering ──────────────────────────────────
    # LIVE MODE : filter events older than late_event_max_minutes to prevent
    #             stale data from corrupting rolling metrics.
    # REPLAY MODE: late filtering is DISABLED because historical events are
    #              always older than late_event_max_minutes by wall clock.
    #              Filtering during replay would drop ALL events.
    if not is_replay and "timestamp" in pdf.columns:
        pdf["_evt_dt"] = (
            pd.to_datetime(
                pdf["timestamp"],
                utc=True,
                errors="coerce"
            )
            .dt.tz_convert("America/New_York")
        )
        late_events = pdf[pdf["_evt_dt"] < late_cutoff]
        pdf = pdf[pdf["_evt_dt"] >= late_cutoff]
        metrics["events_late"] = len(late_events)

        if not late_events.empty:
            for _, late_row in late_events.iterrows():
                error_payload = {
                    "error":           (
                        f"Late event skipped (older than "
                        f"{CONFIG['late_event_max_minutes']} minutes)"
                    ),
                    "ticker":          late_row.get("ticker", ""),
                    "timestamp":      late_row.get("timestamp", ""),
                    "current_time":    current_time.isoformat(),
                    "kafka_offset":    int(late_row.get("offset", -1)),
                    "kafka_partition": int(late_row.get("partition", -1)),
                    "consumer_pipeline": CONFIG["consumer_pipeline_name"],
                    "batch_id":        str(batch_id),
                    "pipeline_run_id": pipeline_run_id,
                }
                # Kafka DLQ only in live mode
                send_to_kafka_dlq(error_payload)
                dlq_buffer.append(error_payload)

        pdf = pdf.drop(columns=["_evt_dt"])

    if len(pdf) == 0:
        log("INFO", "All events filtered",
            {"batch_id": batch_id,
             "late_count": metrics["events_late"],
             "is_replay":  is_replay})
        PROM_DLQ_TOTAL.inc(len(dlq_buffer))
        flush_consumer_dlq_to_s3(dlq_buffer, str(batch_id))
        if not is_replay and _dlq_producer is not None:
            _dlq_producer.flush()
        metrics["batch_latency_s"] = round(time.monotonic() - batch_start, 3)
        log_batch_metrics(str(batch_id), metrics)
        _push_metrics()
        return

    metrics["events_received"] = len(pdf)
   
    snapshot_rows: list = []

    for _, row in pdf.iterrows():
        try:
            ticker   = row["ticker"]

            # ── Guard: Data validation ───────────────────────────────
            if row["close"] <= 0 or row["high"] < row["low"]:
                raise ValueError(
                    f"Invalid market data: close={row['close']}, "
                    f"high={row['high']}, low={row['low']}"
                )



            # ── Buffer update (with safety guards) ───────────────────
            row_dict = row.to_dict()
            update_ticker_buffer(ticker, row_dict)

            # ── NOW get the buffer and add prev OHLCV values ───────────
            buffer = ticker_buffers[ticker]
            if len(buffer) >= 2:
                prev = buffer[-2]
                row_dict.update({
                    "prev_open":   prev.get("open",   np.nan),
                    "prev_high":   prev.get("high",   np.nan),
                    "prev_low":    prev.get("low",    np.nan),
                    "prev_close":  prev.get("close",  np.nan),
                    "prev_volume": prev.get("volume", np.nan),
                })
            else:
                row_dict.update({
                    "prev_open": np.nan,
                    "prev_high": np.nan,
                    "prev_low": np.nan,
                    "prev_close": np.nan,
                    "prev_volume": np.nan,
                })

            # ── Feature computation ──────────────────────────────────
            computed = compute_metrics(ticker_buffers[ticker])
            row_dict.update(computed)

            # ── Lineage stamping ─────────────────────────────────────
            row_dict = add_lineage(
                row_dict             = row_dict,
                batch_id             = str(batch_id),
                pipeline_run_id      = pipeline_run_id,
                processing_timestamp = processing_timestamp,
                is_replay            = is_replay,
            )
            row_dict["kafka_offset"]           = int(row["offset"])
            row_dict["kafka_partition"]        = int(row["partition"])
            row_dict["producer_pipeline_name"] = row["producer_pipeline_name"]
            row_dict["producer_run_id"] = row["producer_run_id"]
            row_dict["batch_id"] = row["batch_id"]
            row_dict["source_fetch_time"] = row["source_fetch_time"]

            snapshot_rows.append(row_dict)
            metrics["events_processed"] += 1
            PROM_RECORDS_PROCESSED_TOTAL.inc()

        except Exception as exc:
            metrics["events_failed"] += 1
            error_payload = {
                "error":            str(exc),
                "ticker":           row.get("ticker", ""),
                "kafka_offset":     int(row.get("offset", -1)),
                "kafka_partition":  int(row.get("partition", -1)),
                "original_event":   row.to_dict(),
                "failed_at":        pendulum.now("America/New_York").to_iso8601_string(),
                "consumer_pipeline": CONFIG["consumer_pipeline_name"],
                "batch_id":         str(batch_id),
                "pipeline_run_id":  pipeline_run_id,
                "is_replay":        is_replay,
            }
            # Kafka DLQ only in live mode — replay routes to S3 DLQ only
            if not is_replay:
                send_to_kafka_dlq(error_payload)
            dlq_buffer.append(error_payload)

    if not snapshot_rows:
        PROM_DLQ_TOTAL.inc(len(dlq_buffer))
        flush_consumer_dlq_to_s3(dlq_buffer, str(batch_id))
        if not is_replay and _dlq_producer is not None:
            _dlq_producer.flush()
        metrics["batch_latency_s"] = round(time.monotonic() - batch_start, 3)
        log_batch_metrics(str(batch_id), metrics)
        _push_metrics()
        return
    
    log("INFO", "Processing events", {"batch_id": batch_id, "event_count": len(snapshot_rows)})
    snapshot_df = pd.DataFrame(snapshot_rows)

    # ── Enrichment ───────────────────────────────────────────────────
    snapshot_df = enrich_intraday_with_positions(snapshot_df, positions_df)
    snapshot_df = snapshot_df.where(pd.notnull(snapshot_df), None)

    # ── S3 write: replay -> equity/replay/, live -> equity/data/ ─────
    save_to_parquet(snapshot_df, str(batch_id), is_replay=is_replay)

    # ── Redis snapshot: LIVE ONLY ─────────────────────────────────────
    # is_replay guard is enforced here — replay never touches Redis.
    # Redis represents live state only; historical replay must not
    # overwrite the current live snapshot with stale historical data.
    if not is_replay and not IS_BACKFILL:
        save_latest_snapshot_all_tickers(ticker_buffers, positions_df)

    # ── Flush DLQ ────────────────────────────────────────────────────
    PROM_DLQ_TOTAL.inc(len(dlq_buffer))
    flush_consumer_dlq_to_s3(dlq_buffer, str(batch_id))
    if not is_replay and _dlq_producer is not None:
        _dlq_producer.flush()

    PROM_LAST_SUCCESS_TIMESTAMP.set(time.time())
    metrics["batch_latency_s"] = round(time.monotonic() - batch_start, 3)
    PROM_DURATION_SECONDS.observe(time.monotonic() - batch_start)
    if not is_replay and not IS_BACKFILL:
        _push_metrics()

# =============================================================
# S3 REPLAY  (REPLAY_MODE = "s3")
# =============================================================

def load_s3_replay_partitions(
    date_str:     str,
    hour_str:     str = "",
    positions_df: pd.DataFrame = None,
) -> None:
    """
    Read raw event parquet files from S3 and process them through the same
    pipeline as the Kafka stream. Used when REPLAY_MODE = "s3".

    Output isolation:
      All output is written to equity/replay/ — NEVER to equity/data/.
      process_batch is called with is_replay=True on every partition.

    Redis:
      NEVER touched during replay. process_batch enforces this via is_replay=True.

    State rebuild:
      NEVER called during replay. Replay starts with empty ticker_buffers.
      State warms naturally from the replay records themselves, in order.

    Path pattern:
        s3://risk-platform-pushparag-analytics/kafka_raw/equity/
            year=Y/month=MM/day=DD/<optional hour filter>/batch_*.parquet

    Each parquet file is treated as one "batch" for consistency.
    Late event filtering is DISABLED for replay (historical events are always
    older than late_event_max_minutes by wall clock; without this fix every
    event would be dropped).

    Prometheus replay metrics are incremented here:
        replay_jobs_total              — once per replay job invocation
        replay_records_processed_total — per-record across all partitions
        replay_failures_total          — per failed partition
        replay_duration_seconds        — total replay wall-clock time
    """
    if not date_str:
        log("ERROR", "REPLAY_DATE must be set for s3 replay mode.")
        return

    replay_start = time.monotonic()
    PROM_REPLAY_JOBS_TOTAL.inc()

    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        base_prefix = CONFIG["raw_replay_prefix"]
        if not base_prefix.endswith("/"):
            base_prefix += "/"
        prefix = (
            f"{base_prefix}"
            f"year={dt.year}/month={dt.month:02d}/day={dt.day:02d}/"
        )
        if hour_str:
            prefix = f"{prefix}hour={int(hour_str):02d}/"

    except ValueError as exc:
        log("ERROR", "Invalid REPLAY_DATE format — expected YYYY-MM-DD",
            {"replay_date": date_str, "error": str(exc)})
        PROM_REPLAY_FAILURES_TOTAL.inc()
        return

    log("INFO", "Starting S3 replay",
        {"date":          date_str,
         "hour":          hour_str,
         "prefix":        prefix,
         "output_prefix": CONFIG["replay_output_prefix"]})

    s3 = get_s3()
    try:
        paginator = s3.get_paginator("list_objects_v2")
        pages     = paginator.paginate(Bucket=CONFIG["write_bucket"], Prefix=prefix)
        keys      = [
            obj["Key"]
            for page in pages
            for obj in page.get("Contents", [])
            if obj["Key"].endswith(".parquet")
        ]
    except Exception as exc:
        log("ERROR", "S3 replay listing failed",
            {"prefix": prefix, "error": str(exc)})
        PROM_REPLAY_FAILURES_TOTAL.inc()
        return

    if not keys:
        log("WARNING", "No raw parquet files found for replay",
            {"prefix": prefix})
        return

    log("INFO", "Replay partitions found", {"count": len(keys)})

    for batch_num, key in enumerate(keys, start=1):
        try:
            obj = s3.get_object(Bucket=CONFIG["write_bucket"], Key=key)
            pdf = pd.read_parquet(BytesIO(obj["Body"].read()))

            if pdf.empty:
                continue

            # Synthesize Kafka metadata columns if absent (S3 raw files
            # were written by the producer and don't carry Kafka offsets)
            if "topic" not in pdf.columns:
                pdf["topic"] = CONFIG["topic"]
            if "partition" not in pdf.columns:
                pdf["partition"] = 0
            if "offset" not in pdf.columns:
                pdf["offset"] = batch_num * 10000  # synthetic, non-colliding

            log("INFO", "Replaying S3 partition",
                {"key": key, "rows": len(pdf), "batch_num": batch_num})

            PROM_REPLAY_RECORDS_TOTAL.inc(len(pdf))

            class _MockBatch:
                """Thin wrapper so replay reuses process_batch unchanged."""
                def __init__(self, df):
                    self._df = df
                def count(self):
                    return len(self._df)
                def toPandas(self):
                    return self._df.copy()

            # is_replay=True: writes to equity/replay/, skips Redis, skips Kafka DLQ
            process_batch(_MockBatch(pdf), batch_num, positions_df, is_replay=True)

        except Exception as exc:
            log("ERROR", "S3 replay partition failed",
                {"key": key, "error": str(exc)})
            PROM_REPLAY_FAILURES_TOTAL.inc()

    replay_duration = time.monotonic() - replay_start
    PROM_REPLAY_DURATION_SECONDS.observe(replay_duration)
    _push_metrics()

    log("INFO", "S3 replay complete",
        {"date":            date_str,
         "files_processed": len(keys),
         "duration_s":      round(replay_duration, 3)})

# =============================================================
# SPARK SESSION
# =============================================================

def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName(CONFIG["spark_app_name"])
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
        )
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.sql.shuffle.partitions", CONFIG["spark_shuffle_partitions"])
        .getOrCreate()
    )

# =============================================================
# KAFKA EVENT SCHEMA
# =============================================================

EVENT_SCHEMA = StructType([
    StructField("producer_run_id", StringType(), True),
    StructField("batch_id", StringType(), True),
    StructField("source_fetch_time", StringType(), True),
    StructField("producer_pipeline_name", StringType(), True),
    StructField("batch_ts", StringType(), True),
    StructField(
        "tickers",
        ArrayType(
            StructType([
                StructField("ticker", StringType(), True),
                StructField("open", FloatType(), True),
                StructField("high", FloatType(), True),
                StructField("low", FloatType(), True),
                StructField("close", FloatType(), True),
                StructField("volume", FloatType(), True),
                StructField("timestamp", StringType(), True),
            ])
        ),
        True,
    ),
])

# =============================================================
# ENTRY POINT
# =============================================================

if __name__ == "__main__":
    import sys

    run_id = str(uuid.uuid4())
    _producer_run_id = run_id   # used by _push_metrics for Pushgateway job label

    # Start /metrics HTTP server (background thread — live mode only)
    if not IS_REPLAY and not IS_BACKFILL:
        _start_metrics_server()

    # Load static positions once at startup (read-only)
    positions_df = load_positions()

    replay_mode = CONFIG["replay_mode"].lower()
    log("INFO", "Consumer starting",
        {"replay_mode": replay_mode,
         "run_id":      run_id,
         "run_mode":    CONFIG["run_mode"]})

    if replay_mode == "s3":
        # ── S3 REPLAY MODE ───────────────────────────────────────────
        # State rebuild is NEVER called in replay mode.
        # ticker_buffers start empty; state warms from replay records.
        log("INFO", "Starting replay — state rebuild skipped (replay mode)",
            {"replay_date": CONFIG["replay_date"],
             "replay_hour": CONFIG["replay_hour"]})

        load_s3_replay_partitions(
            date_str     = CONFIG["replay_date"],
            hour_str     = CONFIG["replay_hour"],
            positions_df = positions_df,
        )

    else:
        # ── LIVE KAFKA STREAM MODE ───────────────────────────────────
        log("INFO", "Starting Kafka stream pipeline")

        # State rebuild — live recovery only.
        # Skipped automatically for: replay, backfill, fresh session starts,
        # pre-market startups, and weekends.
        # Previous trading session data is NEVER loaded.
        rebuild_state_from_s3()

        spark = build_spark_session()
        spark.sparkContext.setLogLevel("WARN")

        raw_df = (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", CONFIG["kafka_broker"])
            .option("subscribe", CONFIG["topic"])
            .option("startingOffsets", "latest")
            .option("failOnDataLoss", "false")
            .load()
        )

        parsed_df = (
            raw_df
            .selectExpr(
                "CAST(value AS STRING) as json_str",
                "topic",
                "partition",
                "offset",
                "timestamp as kafka_timestamp"
            )
            .select(
                from_json(col("json_str"), EVENT_SCHEMA).alias("event"),
                "topic",
                "partition",
                "offset",
                "kafka_timestamp"
            )
            .select(
                explode(col("event.tickers")).alias("ticker_data"),
                col("event.batch_id").alias("batch_id"),
                col("event.producer_run_id").alias("producer_run_id"),
                col("event.source_fetch_time").alias("source_fetch_time"),
                col("event.producer_pipeline_name").alias("producer_pipeline_name"),
                "topic",
                "partition",
                "offset",
                "kafka_timestamp"
            )
            .select(
                col("ticker_data.ticker").alias("ticker"),
                col("ticker_data.open").alias("open"),
                col("ticker_data.high").alias("high"),
                col("ticker_data.low").alias("low"),
                col("ticker_data.close").alias("close"),
                col("ticker_data.volume").alias("volume"),
                col("ticker_data.timestamp").alias("timestamp"),
                "batch_id",
                "producer_run_id",
                "source_fetch_time",
                "producer_pipeline_name",
                "topic",
                "partition",
                "offset",
                "kafka_timestamp"
            )
            .filter(col("volume") > 0)
        )

        query = (
            parsed_df.writeStream
            .foreachBatch(lambda batch_df, batch_id:
                          process_batch(batch_df, batch_id, positions_df, is_replay=False))
            .outputMode("append")
            .option("checkpointLocation", CONFIG["checkpoint_dir"])
            .start()
        )

        # Market-shutdown loop
        while True:
            now = pendulum.now('America/New_York')

            if (
                now.hour == CONFIG["market_shutdown_hour"]
                and now.minute >= CONFIG["market_shutdown_minute"]
            ):
                log(
                    "INFO",
                    "Market shutdown time reached - stopping consumer cleanly",
                    {
                        "shutdown_time":
                            f"{CONFIG['market_shutdown_hour']}:"
                            f"{CONFIG['market_shutdown_minute']:02d}"
                    },
                )

                try:
                    _push_metrics()
                except Exception:
                    pass

                query.stop()
                break

            time.sleep(30)