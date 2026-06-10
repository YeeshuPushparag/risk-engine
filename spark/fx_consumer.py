"""
fx_consumer.py
=====================
Production-grade Spark Structured Streaming consumer for real-time FX ticks.
Architecture mirrors equity_kafka_consumer.py exactly, adapted for FX.

Storage rules
-------------
READ-ONLY  : s3://yeeshu-fx-bucket       (FX universe / positions)
ALL WRITES : s3://risk-platform-pushparag-analytics  (parquet output, DLQ)

Storage layout (writes)
-----------------------
Processed output — LIVE (parquet, partitioned):
    s3://risk-platform-pushparag-analytics/fx/data/
        date=YYYY-MM-DD/hour=HH/batch_<batch_id>.parquet

Processed output — REPLAY (parquet, partitioned):
    s3://risk-platform-pushparag-analytics/fx/replay/
        date=YYYY-MM-DD/hour=HH/batch_<batch_id>.parquet

    REPLAY output is ALWAYS isolated from LIVE output.
    REPLAY_MODE=s3 NEVER writes to fx/data/.

Consumer DLQ (S3, parquet, partitioned by day):
    s3://risk-platform-pushparag-analytics/kafka_dlq/fx/consumer/
        year=Y/month=MM/day=DD/dlq_<batch_id>.parquet

Consumer DLQ (Kafka topic, for stream-level routing) — LIVE MODE ONLY:
    topic: fx_stream_dlq
    Not used during replay (Kafka not needed in S3 replay mode).

Raw Kafka storage (for replay):
    s3://risk-platform-pushparag-analytics/kafka_raw/fx/
        year=YYYY/month=MM/day=DD/hour=HH/batch_<batch_id>.parquet

Replay modes
------------
REPLAY_MODE = "kafka"  -> normal Spark Structured Streaming from Kafka
REPLAY_MODE = "s3"     -> reads raw parquet from kafka_raw/fx/ directly,
                         processes through the same pipeline, writes to fx/replay/.
                         Replay NEVER touches Redis or overwrites fx/data/.

FX Replay uses a date range (no hour concept):
    FX_REPLAY_START_DATE=YYYY-MM-DD
    FX_REPLAY_END_DATE=YYYY-MM-DD

Redis policy
------------
Redis represents LIVE state only.
    Live mode  : Redis snapshot published after every batch.
    Backfill   : Redis is NEVER written or published to.
    Replay mode: Redis is NEVER written or published to.

State rebuild policy
--------------------
State rebuild runs ONLY in live mode (run_mode=live, replay_mode!=s3).
State rebuild is NEVER run during replay or backfill.
State rebuild is NEVER run at or near FX weekly session open — buffers
  start empty for every new weekly session.
State rebuild loads only the recent recovery window (state_rebuild_minutes).
State rebuild NEVER loads Friday session data on Sunday (previous week).

Late Event Handling
-------------------
Events older than 5 minutes from current time are skipped in live mode.
Late filtering is DISABLED during replay (historical events are always older
than 5 minutes by wall clock — filtering would drop every event).
Prevents stale data from corrupting rolling metrics in live mode.

Lineage on every output row
----------------------------
    event_id               — from producer (SHA-256, idempotent)
    kafka_offset           — exact offset in source partition
    kafka_partition        — source partition
    producer_pipeline_name — value from event envelope
    consumer_pipeline_name — "fx_stream_consumer"
    pipeline_run_id        — UUID per batch (constant within one batch)
    processing_timestamp   — ET timestamp when batch was processed

Idempotency
-----------
    event_id deduplication using pandas drop_duplicates (pure data logic)
    No Redis dependency for dedup — simpler, faster, deterministic
    Spark checkpoints handle offset tracking

Buffer safety
-------------
    MAX_BUFFER_PER_PAIR  — rolling window cap per currency pair
    MAX_TOTAL_PAIRS      — evict oldest pairs when total count exceeds cap

Prometheus metrics
------------------
    Exposed on /metrics (port 8000 by default, configurable via METRICS_PORT).
    Prometheus scrapes every 60 s.
    Run-level counters only — no pair / batch_id labels.

    consumer_batches_total           — total micro-batches processed
    consumer_failures_total          — batches that raised an unhandled exception
    consumer_dlq_total               — total events routed to DLQ
    consumer_processed_s3_writes_total — successful S3 output writes
    consumer_redis_writes_total      — successful Redis snapshot publishes (live only)
    consumer_records_processed_total — total records processed
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
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType, StructField, StringType, FloatType
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
    "kafka_broker":          os.getenv("KAFKA_BROKER", "kafka:9092"),
    "topic":                 "fx_ohlc_1m",
    "dlq_topic":             "fx_stream_dlq",

    # Run mode
    "run_mode": os.getenv("FX_MODE", "live").lower(),

    # Replay
    "replay_mode":           os.getenv("FX_REPLAY_MODE", "kafka"),   # "kafka" | "s3"
    "replay_start_date":     os.getenv("FX_REPLAY_START_DATE", ""),   # "YYYY-MM-DD"
    "replay_end_date":       os.getenv("FX_REPLAY_END_DATE", ""),     # "YYYY-MM-DD"

    # S3
    "read_bucket":           "yeeshu-fx-bucket",
    "write_bucket":          "risk-platform-pushparag-analytics",
    "universe_key":          "historical-fx/final_merged.parquet",
    # Live output — never touched by replay
    "output_prefix":         "fx/data",
    # Replay output — never touches live output
    "replay_output_prefix":  "fx/replay",
    "consumer_dlq_prefix":   "kafka_dlq/fx/consumer",
    "raw_replay_prefix":     "kafka_raw/fx/backfill",
    "checkpoint_dir":        (
        os.getenv("CHECKPOINT_DIR", "s3a://risk-platform-pushparag-analytics")
        + "/fx/checkpoints"
    ),

    # Late event handling
    "late_event_max_minutes":   5,

    # Buffer safety (single definition)
    "max_buffer_per_pair":      60,    # FX 24/5 — larger buffer than equity
    "max_total_pairs":          50,    # FX universe is small — conservative cap

    # Redis (snapshots only — dedup removed; only used in live mode)
    "redis_host":            os.getenv("REDIS_HOST", "localhost"),
    "redis_port":            int(os.getenv("REDIS_PORT", 6379)),
    "redis_db_stream":       int(os.getenv("REDIS_DB_STREAM", 1)),

    # Exposure constants
    "z_95":                  1.65,
    "trading_days_per_q":    63,

    # Sector -> currency pair mapping (same as fx_exposure_pipeline)
    "sector_currency_map": {
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
    },
    "foreign_ratio": {
        "USDEUR": 0.45, "USDCHF": 0.30, "USDJPY": 0.25,
        "USDCAD": 0.33, "USDGBP": 0.10, "USDAUD": 0.40, "USDCNY": 0.35,
    },

    # Lineage
    "consumer_pipeline_name":  "fx_stream_consumer",
    "data_source":              "kafka_fx_stream",
    "transformation":           "fx_intraday_metrics_v1",

    # Spark
    "spark_app_name":        "FXKafkaStreaming_Production",
    "spark_shuffle_partitions": "4",

    # Market Hours — FX shuts down at 17:03 (Friday)
    "market_shutdown_hour":   17,
    "market_shutdown_minute": 3,

    # FX Weekly session open: Sunday 17:00 ET.
    # The FX market opens Sunday ~17:00 ET and closes Friday ~17:00 ET.
    # session_open_weekday: 6 = Sunday (datetime.weekday() -> Mon=0 … Sun=6)
    # session_open_hour_et: 17. Adjust via FX_SESSION_OPEN_ET_HOUR if needed.
    "session_open_weekday":  6,   # Sunday
    "session_open_hour_et":  int(os.getenv("FX_SESSION_OPEN_ET_HOUR", "17")),
    "session_open_minute":   0,

    # State rebuild — live mode recovery only.
    # Number of minutes of raw S3 data to load when recovering mid-session.
    # Set to 15 so fx_vol_15m can warm up immediately after a restart.
    "state_rebuild_minutes": 15,

    # Grace period after weekly session open during which rebuild is SKIPPED.
    # If startup occurs within session_open + state_rebuild_grace_minutes,
    # buffers start empty (new session, nothing to recover).
    "state_rebuild_grace_minutes": 15,

    # Prometheus
    "pushgateway_url":       os.getenv("PUSHGATEWAY_URL"),
    "metrics_port":          int(os.getenv("METRICS_PORT", "8000")),
}

# =============================================================
# REPLAY / BACKFILL MODE DETECTION  —  consulted throughout the module
# =============================================================

IS_REPLAY   = CONFIG["replay_mode"] == "s3"
IS_BACKFILL = CONFIG["run_mode"] == "backfill"

# =============================================================
# PROMETHEUS  — run-level counters, no pair/batch labels
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
_consumer_run_id: str = "unknown"


def _push_metrics() -> None:
    """
    Push accumulated Prometheus metrics to Pushgateway.
    Mirrors the Airflow pattern: push_to_gateway(url, job=<pipeline>_<run_id>, registry).
    Non-fatal — a Pushgateway failure must never stop the consumer.
    """
    try:
        push_to_gateway(
            CONFIG["pushgateway_url"],
            job=f"{CONFIG['consumer_pipeline_name']}_{_consumer_run_id}",
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
        text = f"* [CRITICAL] fx_kafka_consumer *\n{message}"
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
# NUMERIC HELPERS
# =============================================================

def nz(x) -> float:
    """Return 0.0 for None / NaN / Inf — safe for downstream arithmetic."""
    if x is None:
        return 0.0
    try:
        if isinstance(x, float) and (np.isnan(x) or np.isinf(x)):
            return 0.0
        return float(x)
    except Exception:
        return 0.0


def json_safe(x):
    """Return None for NaN / Inf — safe for JSON serialisation."""
    if x is None:
        return None
    if isinstance(x, float) and (np.isnan(x) or np.isinf(x)):
        return None
    return x

# =============================================================
# REDIS  —  snapshots ONLY (live mode only; replay/backfill never touch Redis)
# =============================================================

# redis_client is initialised lazily so that replay/backfill mode can start
# even if Redis is unavailable (they have no dependency on Redis).
_redis_client = None


def _get_redis():
    """
    Return the shared Redis client, creating it on first call.
    Only called from publish_fx_snapshot_to_redis which is already
    guarded by IS_REPLAY and IS_BACKFILL — so this is never reached during
    replay or backfill.
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
    LIVE MODE ONLY — never called during replay or backfill.
    """
    try:
        _get_dlq_producer().send(CONFIG["dlq_topic"], payload)
    except Exception as exc:
        log("ERROR", "Kafka FX DLQ send failed",
            {"error": str(exc),
             "event_id": payload.get("event_id", "unknown")})

# =============================================================
# S3 HELPERS  —  atomic writes
# =============================================================

def get_s3():
    return boto3.client("s3")


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
    """Paginate S3 to list all parquet files under a prefix."""
    s3 = get_s3()
    try:
        paginator = s3.get_paginator("list_objects_v2")
        pages     = paginator.paginate(Bucket=bucket, Prefix=prefix)
        return [
            obj["Key"]
            for page in pages
            for obj in page.get("Contents", [])
            if obj["Key"].endswith(".parquet")
        ]
    except Exception as exc:
        log("ERROR", "Failed to list S3 files",
            {"bucket": bucket, "prefix": prefix, "error": str(exc)})
        return []


def read_parquet_from_s3(bucket: str, key: str) -> pd.DataFrame:
    """Read a single parquet file from S3. Returns empty DataFrame on error."""
    try:
        obj = get_s3().get_object(Bucket=bucket, Key=key)
        return pd.read_parquet(BytesIO(obj["Body"].read()))
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

    Path: s3://risk-platform-pushparag-analytics/kafka_dlq/fx/consumer/
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
        log("WARNING", "FX consumer DLQ batch flushed to S3",
            {"batch_id": batch_id, "failed_rows": len(df), "key": key})
    except Exception as exc:
        log("ERROR", "FX consumer DLQ S3 flush failed — dumping to stdout",
            {"batch_id": batch_id, "error": str(exc)})
        send_alert(f"DLQ S3 flush FAILED | batch_id={batch_id} | error={str(exc)}")
        for entry in dlq_buffer:
            print(json.dumps({"FX_CONSUMER_DLQ_FALLBACK": entry}, default=str))

# =============================================================
# LOAD FX UNIVERSE  (read-only from fx-bucket)
# =============================================================

def load_fx_universe() -> tuple:
    """
    Load the FX-exposed equity universe from S3.
    Returns (universe_df, pair_groups_dict).

    universe_df contains: ticker, sector, revenue, currency_pair,
                          foreign_revenue_ratio, daily_exposure
    pair_groups_dict maps: currency_pair -> subset DataFrame
    """
    log("INFO", "Loading FX universe",
        {"bucket": CONFIG["read_bucket"], "key": CONFIG["universe_key"]})
    try:
        obj  = get_s3().get_object(
            Bucket=CONFIG["read_bucket"],
            Key=CONFIG["universe_key"],
        )
        df   = pd.read_parquet(BytesIO(obj["Body"].read()))
        df   = df[["ticker", "sector", "revenue"]].drop_duplicates()

        df["currency_pair"]         = df["sector"].map(CONFIG["sector_currency_map"])
        df["foreign_revenue_ratio"] = df["currency_pair"].map(CONFIG["foreign_ratio"])
        df = df.dropna(subset=["currency_pair", "foreign_revenue_ratio", "revenue"])

        df["daily_exposure"] = (
            df["revenue"] / CONFIG["trading_days_per_q"]
        ) * df["foreign_revenue_ratio"]

        pair_groups = {pair: sub for pair, sub in df.groupby("currency_pair")}

        log("INFO", "FX universe loaded",
            {"tickers":   len(df),
             "pairs":     len(pair_groups),
             "pair_list": list(pair_groups.keys())})
        return df, pair_groups

    except Exception as exc:
        log("ERROR", "Failed to load FX universe", {"error": str(exc)})
        raise

# =============================================================
# PAIR BUFFER MANAGEMENT
# =============================================================

# Global pair buffer — maintained across batches within the same executor.
# Each key is a currency_pair; value is a list of OHLC dicts.
pair_buffers: dict = {}


def update_pair_buffer(pair: str, row_dict: dict) -> None:
    """
    Add a bar to the per-pair rolling OHLC buffer.

    Safety guards:
    1. max_total_pairs:     evict oldest pair if total count exceeds cap.
    2. max_buffer_per_pair: trim oldest rows within a pair if over cap.
    """
    global pair_buffers

    # Guard 1: evict oldest pair symbol if total cap exceeded
    if (pair not in pair_buffers and
            len(pair_buffers) >= CONFIG["max_total_pairs"]):
        evict_key = next(iter(pair_buffers))
        del pair_buffers[evict_key]
        log("WARNING", "Pair buffer evicted — max_total_pairs reached",
            {"evicted": evict_key, "total_pairs": len(pair_buffers)})

    if pair not in pair_buffers:
        pair_buffers[pair] = []

    pair_buffers[pair].append(row_dict)

    # Guard 2: trim per-pair buffer if over cap
    cap = CONFIG["max_buffer_per_pair"]
    if len(pair_buffers[pair]) > cap:
        overflow = len(pair_buffers[pair]) - cap
        pair_buffers[pair] = pair_buffers[pair][overflow:]
        log("DEBUG", "Pair buffer trimmed",
            {"pair": pair, "trimmed": overflow, "cap": cap})

# =============================================================
# STATE REBUILD  —  LIVE MODE RECOVERY ONLY
# =============================================================

def _get_now_et():
    """
    Return current time in Eastern Time (US/Eastern).
    Handles Daylight Saving Time automatically.
    """
    return pendulum.now('America/New_York')


def _fx_weekly_session_open_et(now_et: datetime) -> datetime:
    """
    Return the most recent FX weekly session open time in Eastern Time.

    The FX market opens every Sunday at 17:00 ET (5:00 PM Eastern Time).
    This function locates the most recent Sunday at 17:00 ET that is at or
    before now_et, which defines the boundary between the current trading
    week and the previous week.

    We walk back through calendar days (max 7) to find the most recent
    Sunday with a session open time <= now_et. If today IS Sunday but
    now_et is before 17:00 ET, we go back to the previous Sunday.

    This ensures:
      - Monday restart: session open = last Sunday 17:00 ET
      - Wednesday restart: session open = last Sunday 17:00 ET
      - Sunday restart after 17:00 ET: session open = today 17:00 ET
      - Sunday restart before 17:00 ET: session open = previous Sunday 17:00 ET

    Args:
        now_et: Current time in Eastern Time (timezone-aware)

    Returns:
        Session open datetime in Eastern Time (timezone-aware)
    """
    # Start from today and walk back up to 7 days
    for days_back in range(8):
        candidate_date = now_et - timedelta(days=days_back)
        if candidate_date.weekday() == 6:  # Sunday (weekday 6)
            candidate_open = candidate_date.replace(
                hour=17, minute=0, second=0, microsecond=0
            )
            if candidate_open <= now_et:
                return candidate_open

    # Should never reach here — FX market is open Sun-Fri; fall back to 7 days ago
    fallback = now_et - timedelta(days=7)
    return fallback.replace(hour=17, minute=0, second=0, microsecond=0)


def _is_fx_market_open(now_et: datetime) -> bool:
    """
    Return True if the FX market is currently open.

    FX market hours (Eastern Time):
        Opens:   Sunday 17:00 ET (5:00 PM)
        Closes:  Friday 17:00 ET (5:00 PM)
        Trading: 24/5 from Sunday 5 PM to Friday 5 PM ET

    Closed periods:
        - Friday 17:00 ET through Sunday 17:00 ET
        - Saturday all day

    Weekday mapping: Mon=0, Tue=1, Wed=2, Thu=3, Fri=4, Sat=5, Sun=6

    Args:
        now_et: Current time in Eastern Time (timezone-aware)

    Returns:
        True if market is open, False otherwise
    """
    weekday = now_et.weekday()
    current_hour = now_et.hour

    # Saturday: always closed
    if weekday == 5:
        return False

    # Sunday: open only at/after 17:00 ET
    if weekday == 6:
        return current_hour >= 17

    # Friday: closed at/after 17:00 ET
    if weekday == 4:
        return current_hour < 17

    # Monday - Thursday: 24-hour trading, always open
    return True


def _should_skip_fx_state_rebuild(now_et: datetime) -> bool:
    """
    Return True if state rebuild must be skipped.

    Skip rebuild when any of the following are true:
    1. The FX market is currently closed (weekend, pre-open Sunday, or post-close Friday)
    2. Current time is within the grace window after the weekly session open
       (session_open + state_rebuild_grace_minutes) — fresh session start;
       pair_buffers must start empty so previous week state is not loaded

    The grace window prevents loading stale Friday data into a fresh Sunday session.

    Args:
        now_et: Current time in Eastern Time (timezone-aware)

    Returns:
        True if rebuild should be skipped, False otherwise
    """
    # Skip if market is closed
    if not _is_fx_market_open(now_et):
        return True

    # Get the most recent session open time (last Sunday at 17:00 ET)
    session_open = _fx_weekly_session_open_et(now_et)

    # Calculate grace cutoff (session open + grace minutes)
    grace_cutoff = session_open + timedelta(minutes=CONFIG["state_rebuild_grace_minutes"])

    # Skip if within grace window (fresh session, buffers should start empty)
    if now_et < grace_cutoff:
        return True

    # Market is open and outside grace window → allow rebuild
    return False


def rebuild_state_from_s3() -> None:
    """
    Populate pair_buffers with recent raw S3 data for live mode recovery.

    PURPOSE
    -------
    When the FX consumer restarts mid-session due to a crash, pod restart, or
    Kafka/Spark failure, the in-memory pair_buffers are lost. Without rebuild,
    rolling metrics (fx_return_5m, fx_vol_15m) produce NaN for several minutes
    after recovery.

    This function restores enough intraday history to allow those metrics
    to compute immediately after restart.

    WHEN IT RUNS
    ------------
    ONLY when ALL of the following are true:
      - run_mode == "live"
      - replay_mode != "s3"   (not IS_REPLAY)
      - IS_BACKFILL is False
      - FX market is currently open
      - Startup is NOT within the grace window after weekly session open

    NEVER runs during:
      - Replay (IS_REPLAY = True)
      - Backfill (IS_BACKFILL = True)
      - FX market closed (weekend, Saturday, pre-open Sunday, post-close Friday)
      - Near-open startups within grace window (fresh weekly session)

    DST HANDLING
    ------------
    This function uses Eastern Time (US/Eastern) for all market hour decisions.
    pendulum is used to handle Daylight Saving Time transitions automatically.
    Sunday 17:00 ET is always 17:00 ET regardless of DST.

    WHAT IT LOADS
    -------------
    Scans kafka_raw/fx/ for partitions that fall within:
        [now - state_rebuild_minutes, now]

    Only the current calendar day is scanned. The function determines
    which hour partitions overlap with the rebuild window and requests
    only those — it never scans full date ranges or previous days.


    PREVIOUS SESSION ISOLATION
    --------------------------
    The prefix always includes the current day's date:
        kafka_raw/fx/backfill/year=Y/month=MM/day=DD/
    This guarantees:
      - Friday data is NEVER loaded on Sunday (new weekly session)
      - Previous day data is NEVER loaded on the current day

    The _should_skip_fx_state_rebuild() guard additionally ensures that
    a Sunday startup near the session open receives empty buffers, not
    Friday's data.

    BUFFER POPULATION
    -----------------
    Only clean OHLC bars (the same structure stored by the live pipeline)
    are inserted into pair_buffers. Each bar is validated before insertion
    (close > 0, high >= low) mirroring live pipeline validation.

    Bars are sorted ascending by event_time before insertion so buffer
    order matches live ingestion order.

    FAILURE HANDLING
    ----------------
    Any S3 error during rebuild is logged and swallowed. A rebuild failure
    must never prevent the consumer from starting — it simply means metrics
    will warm from live data (same behaviour as if rebuild had been skipped).
    """
    # ── Guard: only live, non-replay, non-backfill ─────────────────────
    if IS_REPLAY or IS_BACKFILL:
        log("INFO", "FX state rebuild skipped — not in live mode",
            {"IS_REPLAY": IS_REPLAY, "IS_BACKFILL": IS_BACKFILL})
        return

    if CONFIG["run_mode"] != "live":
        log("INFO", "FX state rebuild skipped — run_mode is not live",
            {"run_mode": CONFIG["run_mode"]})
        return

    # ── Get current time in Eastern Time ─────────
    now_et = _get_now_et()

    # ── Guard: skip rebuild for fresh session starts or closed market ───
    if _should_skip_fx_state_rebuild(now_et):
        log("INFO", "FX state rebuild skipped — fresh session start or market closed",
            {"now_et":        now_et.isoformat(),
             "weekday":       now_et.weekday(),
             "grace_minutes": CONFIG["state_rebuild_grace_minutes"]})
        return

    rebuild_minutes = CONFIG["state_rebuild_minutes"]
    rebuild_start_et = now_et - timedelta(minutes=rebuild_minutes)

    log("INFO", "Starting FX state rebuild",
        {"rebuild_start_et": rebuild_start_et.isoformat(),
         "rebuild_end_et":   now_et.isoformat(),
         "window_minutes":   rebuild_minutes,
         "current_et":       now_et.isoformat()})

    # ── Determine which hour partitions to scan (using ET) ────────────
    # Only scan the current calendar day in ET.
    # Build the set of ET hours that overlap with [rebuild_start_et, now_et]
    hours_to_scan = set()
    cursor = rebuild_start_et
    while cursor <= now_et:
        hours_to_scan.add(cursor.hour)
        cursor += timedelta(hours=1)

    base_prefix = CONFIG["raw_replay_prefix"]
    if not base_prefix.endswith("/"):
        base_prefix += "/"

    today_prefix = (
        f"{base_prefix}"
        f"year={now_et.year}/month={now_et.month:02d}/day={now_et.day:02d}/"
    )

    # Collect keys only from the required hour partitions (current day only)
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
            log("WARNING", "FX state rebuild: failed to list S3 prefix",
                {"prefix": hour_prefix, "error": str(exc)})

    if not all_keys:
        log("INFO", "FX state rebuild: no raw S3 files found in recovery window",
            {"today_prefix": today_prefix, "hours": sorted(hours_to_scan)})
        return

    log("INFO", "FX state rebuild: raw S3 files found",
        {"file_count": len(all_keys), "hours": sorted(hours_to_scan)})

    # ── Read, filter to rebuild window, and populate pair_buffers ───────
    frames: List[pd.DataFrame] = []
    for key in all_keys:
        try:
            obj = s3.get_object(Bucket=CONFIG["write_bucket"], Key=key)
            df = pd.read_parquet(BytesIO(obj["Body"].read()))
            if df.empty:
                continue
            frames.append(df)
        except Exception as exc:
            log("WARNING", "FX state rebuild: failed to read S3 file",
                {"key": key, "error": str(exc)})

    if not frames:
        log("INFO", "FX state rebuild: all files were empty or unreadable")
        return

    combined = pd.concat(frames, ignore_index=True)

    # Filter to only events within the rebuild window (using ET)
    if "event_time" not in combined.columns:
        log("WARNING", "FX state rebuild: event_time column missing — skipping rebuild")
        return

    combined["_evt_dt"] = (
        pd.to_datetime(
            combined["event_time"],
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
        log("INFO", "FX state rebuild: no events in rebuild window after filtering",
            {"rebuild_start_et": rebuild_start_et.isoformat(),
             "now_et":           now_et.isoformat()})
        return

    # Sort chronologically so buffer order matches live ingestion order
    combined = combined.sort_values("_evt_dt").reset_index(drop=True)
    combined = combined.drop(columns=["_evt_dt"])

    # Required OHLC columns for compute_fx_metrics
    required_cols = {"currency_pair", "open", "high", "low", "close"}
    missing = required_cols - set(combined.columns)
    if missing:
        log("WARNING", "FX state rebuild: required columns missing — skipping rebuild",
            {"missing_columns": list(missing)})
        return

    records_loaded = 0
    for _, row in combined.iterrows():
        pair = row.get("currency_pair")
        if not pair:
            continue

        # Validate the bar before buffering (mirrors live pipeline validation)
        try:
            close = float(row["close"])
            high = float(row["high"])
            low = float(row["low"])
        except (TypeError, ValueError):
            continue

        if close <= 0 or high < low:
            continue

        # Build a clean OHLC bar — identical structure to what the live
        # pipeline stores via update_pair_buffer
        clean_bar = {
            "currency_pair": pair,
            "open": float(row["open"]),
            "high": high,
            "low": low,
            "close": close,
            "event_time": row.get("event_time", ""),
            "ingested_at": row.get("ingested_at", ""),
        }

        update_pair_buffer(pair, clean_bar)
        records_loaded += 1

    log("INFO", "FX state rebuild complete",
        {"records_loaded": records_loaded,
         "pairs_in_buffer": len(pair_buffers),
         "rebuild_window_minutes": rebuild_minutes})

# =============================================================
# BATCH METRICS
# =============================================================

def make_batch_metrics() -> dict:
    return {
        "events_received":  0,
        "events_processed": 0,
        "events_failed":    0,
        "events_deduped":   0,
        "events_late":      0,
        "output_rows":      0,
        "batch_latency_s":  0.0,
    }


def log_batch_metrics(batch_id: str, metrics: dict) -> None:
    log("INFO", "FX batch metrics", {"batch_id": batch_id, **metrics})

# =============================================================
# FX FEATURE ENGINEERING  (pure — no I/O)
# =============================================================

def compute_fx_metrics(buffer: list) -> dict:
    """
    Compute rolling FX metrics from the per-pair OHLC buffer.
    Business logic preserved from original FX consumer.

    Metrics:
        fx_return_1m   — 1-bar log return
        fx_return_5m   — 5-bar log return
        fx_vol_15m     — 15-bar log-return standard deviation
    """
    closes = np.array([b["close"] for b in buffer], dtype=float)

    metrics = {
        "fx_return_1m": np.nan,
        "fx_return_5m": np.nan,
        "fx_vol_15m":   np.nan,
    }

    if len(closes) >= 2:
        metrics["fx_return_1m"] = (closes[-1] - closes[-2]) / closes[-2]

    if len(closes) >= 6:
        metrics["fx_return_5m"] = (closes[-1] - closes[-6]) / closes[-6]

    if len(closes) >= 16:
        r     = np.diff(closes) / closes[:-1]
        log_r = np.log1p(r)
        metrics["fx_vol_15m"] = float(np.std(log_r[-15:]))

    return metrics

# =============================================================
# EXPOSURE ENRICHMENT  (fan-out: FX bar -> per-ticker rows)
# =============================================================

def compute_exposure_rows(
    bar:         dict,
    pair:        str,
    fx_metrics:  dict,
    pair_groups: dict,
) -> list:
    """
    Fan out a single FX OHLC bar into one output row per exposed ticker.

    For each ticker in pair_groups[pair]:
        position_size = daily_exposure (revenue-weighted, quarter normalised)
        fx_pnl        = position_size x fx_return_1m
        VaR_95_15m    = position_size x fx_vol_15m x Z_95
    """
    if pair not in pair_groups:
        return []

    rows = []
    z95  = CONFIG["z_95"]

    for _, ticker_row in pair_groups[pair].iterrows():
        position_size = nz(ticker_row["daily_exposure"])
        fx_return_1m  = nz(fx_metrics["fx_return_1m"])
        fx_vol_15m    = nz(fx_metrics["fx_vol_15m"])

        rows.append({
            # FX bar
            "currency_pair":     pair,
            "open":              bar.get("open"),
            "high":              bar.get("high"),
            "low":               bar.get("low"),
            "close":             bar.get("close"),
            "event_time":        bar.get("event_time"),
            "ingested_at":       bar.get("ingested_at"),

            # Ticker exposure
            "ticker":            ticker_row["ticker"],
            "sector":            ticker_row.get("sector", ""),
            "revenue":           nz(ticker_row.get("revenue")),
            "foreign_revenue_ratio": nz(ticker_row.get("foreign_revenue_ratio")),
            "position_size":     position_size,

            # FX metrics
            "fx_return_1m":      nz(fx_metrics["fx_return_1m"]),
            "fx_return_5m":      nz(fx_metrics["fx_return_5m"]),
            "fx_vol_15m":        fx_vol_15m,

            # Risk metrics
            "fx_pnl":            nz(position_size * fx_return_1m),
            "VaR_95_15m":        nz(position_size * fx_vol_15m * z95),
        })

    return rows

# =============================================================
# SAVE OUTPUT  (atomic S3 write, partitioned by date + hour)
# =============================================================

def save_to_parquet(df: pd.DataFrame, batch_id: str, is_replay: bool = False) -> None:
    """
    Write processed FX batch to S3 as parquet using atomic write.
    Partitioned by date and hour — never overwrites existing batches.

    LIVE mode  : writes to fx/data/date=.../hour=.../batch_<id>.parquet
    REPLAY mode: writes to fx/replay/date=.../hour=.../batch_<id>.parquet

    The is_replay flag ensures replay output is ALWAYS isolated from live output.
    Replay can never overwrite fx/data/ regardless of configuration.
    """
    if df.empty:
        return

    # Select the correct output prefix based on mode — never mix them
    output_prefix = CONFIG["replay_output_prefix"] if is_replay else CONFIG["output_prefix"]

    try:
        ts             = pd.to_datetime(df["event_time"].iloc[0])
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
# REDIS SNAPSHOT  (LIVE MODE ONLY)
# =============================================================

def publish_fx_snapshot_to_redis(snapshot_rows: list) -> None:
    """
    Publish the latest FX snapshot to Redis for real-time consumers.
    Mirrors the equity pattern: SET + PUBLISH.

    LIVE MODE ONLY — this function must never be called during replay or backfill.
    The call site (process_batch) enforces this via the is_replay parameter
    and the IS_BACKFILL flag.
    """
    if not snapshot_rows:
        return

    try:
        safe_rows     = [{k: json_safe(v) for k, v in r.items()} for r in snapshot_rows]
        snapshot_json = json.dumps(safe_rows, default=str)

        rc = _get_redis()
        rc.set("fx_latest_snapshot", snapshot_json)
        rc.publish("fx_stream", snapshot_json)

        PROM_REDIS_WRITES_TOTAL.inc()

    except Exception as exc:
        log("ERROR", "Redis FX snapshot publish failed", {"error": str(exc)})
        send_alert(f"Redis snapshot FAILED | error={str(exc)}")

# =============================================================
# ADD LINEAGE TO OUTPUT ROWS
# =============================================================

def add_lineage(
    row_dict:             dict,
    batch_id:             str,
    pipeline_run_id:      str,
    processing_timestamp: str,
    kafka_offset:         int,
    kafka_partition:      int,
    producer_pipeline:    str,
    is_replay:            bool,
) -> dict:
    """
    Stamp all required lineage fields onto an output row.
    Constant fields (pipeline_run_id, processing_timestamp) are set here.
    Per-row fields (kafka_offset, kafka_partition) are passed in from the source row.
    """
    row_dict["consumer_pipeline_name"]  = CONFIG["consumer_pipeline_name"]
    row_dict["pipeline_run_id"]         = pipeline_run_id
    row_dict["processing_timestamp"]    = processing_timestamp
    row_dict["data_source"]             = CONFIG["data_source"]
    row_dict["transformation"]          = CONFIG["transformation"]
    row_dict["record_created_at"]       = processing_timestamp
    row_dict["run_mode"]                = ("replay" if is_replay else CONFIG["run_mode"])
    # Per-row Kafka provenance
    row_dict["kafka_offset"]            = kafka_offset
    row_dict["kafka_partition"]         = kafka_partition
    row_dict["producer_pipeline_name"]  = producer_pipeline
    return row_dict

# =============================================================
# PROCESS BATCH  (core consumer logic — live and replay share this)
# =============================================================

def process_batch(
    batch_df,
    batch_id:    int,
    pair_groups: dict,
    is_replay:   bool = False,
) -> None:
    """
    Spark foreachBatch handler AND replay batch processor.
    Called once per micro-batch (live) or per S3 partition (replay).

    is_replay flag controls:
      - Output path: fx/replay/ vs fx/data/
      - Redis: SKIPPED entirely when is_replay=True
      - Kafka DLQ publish: SKIPPED when is_replay=True (S3 DLQ only)
      - Late event filtering: DISABLED when is_replay=True (historical events
        are always older than late_event_max_minutes by wall clock — without
        this guard every replayed event would be dropped)

    Per-batch lifecycle:
      1. Convert Spark -> pandas
      2. Deduplicate by event_id (pure pandas — no Redis)
      3. Filter late events [LIVE ONLY — disabled for replay]
      4. Per-bar: validation -> buffer update -> FX metrics -> fan-out -> lineage
      5. Atomic S3 write (replay -> fx/replay/, live -> fx/data/)
      6. Redis snapshot [LIVE ONLY — skipped when is_replay=True or IS_BACKFILL]
      7. Flush consumer DLQ (S3 always; Kafka topic only in live mode)
      8. Push Prometheus metrics

    Idempotency:
      - event_id dedup via drop_duplicates (deterministic, no external deps)
      - Spark checkpoints handle offset tracking (live mode)
      - Duplicate replay runs are idempotent (parquet append is safe)
    """
    pipeline_run_id      = str(uuid.uuid4())
    processing_timestamp = pendulum.now("America/New_York").to_iso8601_string()
    metrics              = make_batch_metrics()
    dlq_buffer:   list   = []
    batch_start          = time.monotonic()
    current_time         = pd.Timestamp.now(tz="America/New_York")
    late_cutoff          = current_time - pd.Timedelta(minutes=CONFIG["late_event_max_minutes"])

    PROM_BATCHES_TOTAL.inc()

    # ── Convert Spark batch to pandas ─────────────────────────────────
    try:
        if batch_df.count() == 0:
            log("INFO", "Empty FX batch received",
                {"batch_id": batch_id, "is_replay": is_replay})
            return
        pdf = batch_df.toPandas()
        print(
            pdf[["currency_pair", "partition", "offset"]]
                .sort_values(["partition", "offset"])
        )
        print(
            f"[CONSUMER-IN] batch={batch_id} "
            f"rows={len(pdf)} "
            f"pairs={sorted(pdf['currency_pair'].unique().tolist())}"
        )
    except Exception as exc:
        log("ERROR", "Failed to convert FX batch to pandas",
            {"batch_id": batch_id, "is_replay": is_replay, "error": str(exc)})
        send_alert(f"Batch conversion FAILED | batch_id={batch_id} | error={str(exc)}")
        PROM_FAILURES_TOTAL.inc()
        return

    # ── Step 1: Deduplicate by event_id (pure data logic) ─────────────
    original_count            = len(pdf)
    pdf                       = pdf.drop_duplicates(subset=["event_id"])
    deduped_count             = len(pdf)
    metrics["events_deduped"] = original_count - deduped_count

    if deduped_count == 0:
        log("INFO", "All FX events in batch were duplicates",
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
    if not is_replay and "event_time" in pdf.columns:
        pdf["_evt_dt"] = (
            pd.to_datetime(
                pdf["event_time"],
                utc=True,
                errors="coerce"
            )
            .dt.tz_convert("America/New_York")
        )
        late_events    = pdf[pdf["_evt_dt"] < late_cutoff]
        pdf            = pdf[pdf["_evt_dt"] >= late_cutoff].copy()
        metrics["events_late"] = len(late_events)

        for _, late_row in late_events.iterrows():
            dlq_entry = {
                "error":            (
                    f"Late FX event skipped — older than "
                    f"{CONFIG['late_event_max_minutes']} minutes"
                ),
                "event_id":         late_row.get("event_id", ""),
                "currency_pair":    late_row.get("currency_pair", ""),
                "event_time":       late_row.get("event_time", ""),
                "current_time":     current_time.isoformat(),
                "kafka_offset":     int(late_row.get("offset", -1)),
                "kafka_partition":  int(late_row.get("partition", -1)),
                "consumer_pipeline": CONFIG["consumer_pipeline_name"],
                "batch_id":         str(batch_id),
                "pipeline_run_id":  pipeline_run_id,
            }
            # Kafka DLQ only in live mode
            send_to_kafka_dlq(dlq_entry)
            dlq_buffer.append(dlq_entry)

        pdf = pdf.drop(columns=["_evt_dt"])

    if len(pdf) == 0:
        log("INFO", "All FX events filtered",
            {"batch_id":   batch_id,
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
    log(
        "INFO",
        "FX batch processed successfully",
        {"batch_id": str(batch_id)}
    )
    output_rows: list = []

    # ── Step 3: Process each FX bar ───────────────────────────────────
    for _, bar in pdf.iterrows():
        try:
            pair     = bar["currency_pair"]
            event_id = bar["event_id"]

            # Data validation
            if bar["close"] <= 0 or bar["high"] < bar["low"]:
                raise ValueError(
                    f"Invalid FX data: close={bar['close']}, "
                    f"high={bar['high']}, low={bar['low']}"
                )

            if pair not in pair_groups:
                log("DEBUG", "FX pair not in universe — skipping",
                    {"pair": pair, "batch_id": batch_id})
                continue

            # ─── Buffer update: clean OHLC structure ───
            # CRITICAL: Do NOT store full row in buffer.
            # Buffer must contain ONLY OHLC + minimal metadata.
            clean_bar = {
                "currency_pair": pair,
                "open":          float(bar["open"]),
                "high":          float(bar["high"]),
                "low":           float(bar["low"]),
                "close":         float(bar["close"]),
                "event_time":    bar.get("event_time", ""),
                "ingested_at":   bar.get("ingested_at", ""),
            }
            update_pair_buffer(pair, clean_bar)

            # FX rolling metrics (uses clean buffer)
            fx_metrics = compute_fx_metrics(pair_buffers[pair])

            # Fan-out to per-ticker exposure rows (uses clean bar)
            exposure_rows = compute_exposure_rows(
                bar        = clean_bar,
                pair       = pair,
                fx_metrics = fx_metrics,
                pair_groups= pair_groups,
            )

            if not exposure_rows:
                continue

            # Stamp lineage on every output row
            kafka_offset      = int(bar.get("offset", -1))
            kafka_partition   = int(bar.get("partition", -1))
            producer_pipeline = bar.get("source_pipeline", "fx_kafka_producer")

            for out_row in exposure_rows:
                out_row["event_id"] = event_id
                out_row = add_lineage(
                    row_dict             = out_row,
                    batch_id             = str(batch_id),
                    pipeline_run_id      = pipeline_run_id,
                    processing_timestamp = processing_timestamp,
                    kafka_offset         = kafka_offset,
                    kafka_partition      = kafka_partition,
                    producer_pipeline    = producer_pipeline,
                    is_replay            = is_replay,
                )
                output_rows.append(out_row)

            metrics["events_processed"] += 1
            PROM_RECORDS_PROCESSED_TOTAL.inc()

        except Exception as exc:
            metrics["events_failed"] += 1
            dlq_entry = {
                "error":            str(exc),
                "event_id":         bar.get("event_id", ""),
                "currency_pair":    bar.get("currency_pair", ""),
                "kafka_offset":     int(bar.get("offset", -1)),
                "kafka_partition":  int(bar.get("partition", -1)),
                "original_event":   bar.to_dict(),
                "failed_at":        pendulum.now("America/New_York").to_iso8601_string(),
                "consumer_pipeline": CONFIG["consumer_pipeline_name"],
                "batch_id":         str(batch_id),
                "pipeline_run_id":  pipeline_run_id,
                "is_replay":        is_replay,
            }
            # Kafka DLQ only in live mode — replay routes to S3 DLQ only
            if not is_replay:
                send_to_kafka_dlq(dlq_entry)
            dlq_buffer.append(dlq_entry)

    metrics["output_rows"] = len(output_rows)

    if not output_rows:
        PROM_DLQ_TOTAL.inc(len(dlq_buffer))
        flush_consumer_dlq_to_s3(dlq_buffer, str(batch_id))
        if not is_replay and _dlq_producer is not None:
            _dlq_producer.flush()
        metrics["batch_latency_s"] = round(time.monotonic() - batch_start, 3)
        log_batch_metrics(str(batch_id), metrics)
        _push_metrics()
        return

    # ── Step 4: Build output DataFrame, clean NaN ─────────────────────
    df_out = pd.DataFrame(output_rows)
    df_out = df_out.where(pd.notnull(df_out), None)

    # ── Step 5: Atomic S3 write: replay -> fx/replay/, live -> fx/data/ ─
    save_to_parquet(df_out, str(batch_id), is_replay=is_replay)

    # ── Step 6: Redis snapshot: LIVE ONLY ─────────────────────────────
    # is_replay guard and IS_BACKFILL guard enforced here.
    # Redis represents live state only; historical replay/backfill must not
    # overwrite the current live snapshot with stale historical data.
    if not is_replay and not IS_BACKFILL:
        pairs = sorted(set(r["currency_pair"] for r in output_rows))
        print(
            f"[REDIS-OUT] batch={batch_id} "
            f"pairs={pairs}"
        )
        publish_fx_snapshot_to_redis(output_rows)

    # ── Step 7: Flush DLQ ────────────────────────────────────────────
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
    start_date_str: str,
    end_date_str:   str,
    pair_groups:    dict = None,
) -> None:
    """
    Read raw FX event parquet files from S3 for the inclusive date range and
    process them through the same pipeline as the Kafka stream.
    Used when REPLAY_MODE = "s3".

    FX replay uses a date range (FX_REPLAY_START_DATE..FX_REPLAY_END_DATE).
    There is NO hour concept in FX replay.

    Output isolation:
      All output is written to fx/replay/ — NEVER to fx/data/.
      process_batch is called with is_replay=True on every partition.

    Redis:
      NEVER touched during replay. process_batch enforces this via is_replay=True.

    State rebuild:
      NEVER called during replay. Replay starts with empty pair_buffers.
      State warms naturally from the replay records themselves, in order.

    Path pattern:
        s3://risk-platform-pushparag-analytics/kafka_raw/fx/
            year=Y/month=MM/day=DD/hour=HH/batch_*.parquet

    Each parquet file is treated as one "batch" for consistency.
    Event_id dedup still applies — replay is idempotent.
    Late event filtering is DISABLED for replay (historical events are always
    older than late_event_max_minutes by wall clock).

    Prometheus replay metrics:
        replay_jobs_total              — once per replay job invocation
        replay_records_processed_total — per-record across all partitions
        replay_failures_total          — per failed partition
        replay_duration_seconds        — total replay wall-clock time
    """
    if not start_date_str or not end_date_str:
        log("ERROR", "FX_REPLAY_START_DATE and FX_REPLAY_END_DATE must both be set.")
        return

    replay_start = time.monotonic()
    PROM_REPLAY_JOBS_TOTAL.inc()

    try:
        start_dt = datetime.strptime(start_date_str, "%Y-%m-%d")
        end_dt   = datetime.strptime(end_date_str,   "%Y-%m-%d")
    except ValueError as exc:
        log("ERROR", "Invalid replay date format — expected YYYY-MM-DD",
            {"start": start_date_str, "end": end_date_str, "error": str(exc)})
        PROM_REPLAY_FAILURES_TOTAL.inc()
        return

    # Collect all daily prefixes in the date range
    prefixes = []
    current  = start_dt
    while current <= end_dt:
        prefix = (
            f"{CONFIG['raw_replay_prefix']}/"
            f"year={current.year}/month={current.month:02d}/day={current.day:02d}/"
        )
        prefixes.append(prefix)
        current += timedelta(days=1)

    log("INFO", "Starting FX S3 replay",
        {"start_date":    start_date_str,
         "end_date":      end_date_str,
         "date_count":    len(prefixes),
         "output_prefix": CONFIG["replay_output_prefix"]})

    # Collect all keys across the date range
    all_keys = []
    for prefix in prefixes:
        try:
            keys = list_s3_files_with_prefix(CONFIG["write_bucket"], prefix)
            all_keys.extend(keys)
        except Exception as exc:
            log("ERROR", "S3 replay listing failed",
                {"prefix": prefix, "error": str(exc)})
            PROM_REPLAY_FAILURES_TOTAL.inc()

    if not all_keys:
        log("WARNING", "No raw FX parquet files found for replay",
            {"start": start_date_str, "end": end_date_str})
        return

    log("INFO", "FX replay partitions found", {"total_files": len(all_keys)})

    s3 = get_s3()
    for batch_num, key in enumerate(all_keys, start=1):
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

            log("INFO", "Replaying FX S3 partition",
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

            # is_replay=True: writes to fx/replay/, skips Redis, skips Kafka DLQ
            process_batch(_MockBatch(pdf), batch_num, pair_groups, is_replay=True)

        except Exception as exc:
            log("ERROR", "FX S3 replay partition failed",
                {"key": key, "error": str(exc)})
            PROM_REPLAY_FAILURES_TOTAL.inc()

    replay_duration = time.monotonic() - replay_start
    PROM_REPLAY_DURATION_SECONDS.observe(replay_duration)
    _push_metrics()

    log("INFO", "FX S3 replay complete",
        {"start_date":      start_date_str,
         "end_date":        end_date_str,
         "files_processed": len(all_keys),
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
# KAFKA EVENT SCHEMA  (matches fx_kafka_producer event envelope)
# =============================================================
# NOTE: data.timestamp field removed — using ONLY event_time for market time.
# This eliminates duplicate timestamp confusion.

EVENT_SCHEMA = StructType([
    StructField("event_id",          StringType(), True),
    StructField("event_type",        StringType(), True),
    StructField("schema_version",    StringType(), True),
    StructField("pipeline_name",     StringType(), True),
    StructField("data_source",       StringType(), True),
    StructField("producer_run_id",   StringType(), True),
    StructField("batch_id",          StringType(), True),
    StructField("source_fetch_time", StringType(), True),
    StructField("event_time",        StringType(), True),
    StructField("ingested_at",       StringType(), True),
    StructField("data", StructType([
        StructField("currency_pair", StringType(), True),
        StructField("open",          FloatType(),  True),
        StructField("high",          FloatType(),  True),
        StructField("low",           FloatType(),  True),
        StructField("close",         FloatType(),  True),
        StructField("date",          StringType(), True),
        # NOTE: data.timestamp NOT included — event_time is source of truth
    ])),
])

# =============================================================
# ENTRY POINT
# =============================================================

if __name__ == "__main__":
    import sys

    run_id = str(uuid.uuid4())
    _consumer_run_id = run_id   # used by _push_metrics for Pushgateway job label

    # Start /metrics HTTP server (background thread — live mode only)
    if not IS_REPLAY and not IS_BACKFILL:
        _start_metrics_server()

    # Load FX universe once at startup (read-only)
    fx_universe, pair_groups = load_fx_universe()

    replay_mode = CONFIG["replay_mode"].lower()
    log("INFO", "FX consumer starting",
        {"replay_mode": replay_mode,
         "run_id":      run_id,
         "run_mode":    CONFIG["run_mode"]})

    if replay_mode == "s3":
        # ── S3 REPLAY MODE ───────────────────────────────────────────
        # State rebuild is NEVER called in replay mode.
        # pair_buffers start empty; state warms from replay records.
        log("INFO", "Starting FX replay — state rebuild skipped (replay mode)",
            {"replay_start": CONFIG["replay_start_date"],
             "replay_end":   CONFIG["replay_end_date"]})

        load_s3_replay_partitions(
            start_date_str = CONFIG["replay_start_date"],
            end_date_str   = CONFIG["replay_end_date"],
            pair_groups    = pair_groups,
        )

    else:
        # ── LIVE KAFKA STREAM MODE ───────────────────────────────────
        log("INFO", "Starting Kafka stream pipeline")

        # State rebuild — live recovery only.
        # Skipped automatically for: replay, backfill, fresh session starts
        # (within grace window after weekly open), closed market periods.
        # Previous weekly session data is NEVER loaded.
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
                "topic", "partition", "offset",
                "timestamp as kafka_timestamp",
            )
            .select(
                from_json(col("json_str"), EVENT_SCHEMA).alias("event"),
                "topic", "partition", "offset", "kafka_timestamp",
            )
            .select(
                "event.*",
                "topic", "partition", "offset", "kafka_timestamp",
            )
            .select(
                "event_id",
                "producer_run_id",
                "batch_id",
                "source_fetch_time",
                col("pipeline_name").alias("source_pipeline"),
                "data_source",
                "event_time",                       # PRIMARY market timestamp
                "ingested_at",
                col("data.currency_pair").alias("currency_pair"),
                col("data.open").alias("open"),
                col("data.high").alias("high"),
                col("data.low").alias("low"),
                col("data.close").alias("close"),
                col("data.date").alias("date"),
                # NOTE: data.timestamp NOT included — event_time is source of truth
                "topic", "partition", "offset", "kafka_timestamp",
            )
            .filter(col("close") > 0)
        )

        query = (
            parsed_df.writeStream
            .foreachBatch(lambda batch_df, batch_id:
                          # is_replay=False: live mode — Redis enabled, Kafka DLQ enabled
                          process_batch(batch_df, batch_id, pair_groups, is_replay=False))
            .outputMode("append")
            .option("checkpointLocation", CONFIG["checkpoint_dir"])
            .start()
        )

        # Market-shutdown loop — FX closes at 17:03 ET on Fridays
        while True:
            now = pendulum.now('America/New_York')

            if (
                now.weekday() == 4
                and now.hour == CONFIG["market_shutdown_hour"]
                and now.minute >= CONFIG["market_shutdown_minute"]
            ):
                log(
                    "INFO",
                    "Market shutdown time reached - stopping FX consumer cleanly",
                    {
                        "shutdown_time":
                            f"{CONFIG['market_shutdown_hour']}:"
                            f"{CONFIG['market_shutdown_minute']:02d}"
                    },
                )

                query.stop()
                break

            time.sleep(30)