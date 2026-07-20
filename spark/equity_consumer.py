"""
equity_consumer.py
==================
Production-grade Spark Structured Streaming consumer for real-time equity ticks.

Storage rules
-------------
READ-ONLY  : s3://pushpa-equity-bucket                    (static positions)
ALL WRITES : s3://risk-platform-pushpa-analytics        (parquet output, DLQ, snapshots)

Execution modes
---------------
Three mutually exclusive run modes, selected via RUN_MODE environment variable:

  run_mode=live     — consume from live Kafka topic (equity_stream)
  run_mode=backfill — consume from backfill Kafka topic (equity_stream_backfill)
  run_mode=replay   — read historical raw parquet from S3, no Kafka consumption

Replay source selection (replay mode only), selected via REPLAY_PATH:

  replay_path=backfill    — replay historical backfill raw files:
                        kafka_raw/equity/backfill/
  replay_path=live — replay historical live raw files:
                        kafka_raw/equity/live/

Processed output paths (writes, partitioned by date and hour)
--------------------------------------------------------------
  LIVE:
      s3://risk-platform-pushpa-analytics/equity/data/live/
          date=YYYY-MM-DD/hour=HH/batch_<batch_id>.parquet

  BACKFILL:
      s3://risk-platform-pushpa-analytics/equity/data/backfill/
          date=YYYY-MM-DD/hour=HH/batch_<batch_id>.parquet

  REPLAY:
      s3://risk-platform-pushpa-analytics/equity/data/replay/
          date=YYYY-MM-DD/hour=HH/batch_<batch_id>.parquet

  Outputs from different modes are fully isolated and never mixed.

DLQ paths (writes, partitioned by day)
---------------------------------------
  LIVE:
      s3://risk-platform-pushpa-analytics/equity/dlq/live/
          year=Y/month=MM/day=DD/dlq_<batch_id>.parquet

  BACKFILL:
      s3://risk-platform-pushpa-analytics/equity/dlq/backfill/
          year=Y/month=MM/day=DD/dlq_<batch_id>.parquet

  REPLAY:
      s3://risk-platform-pushpa-analytics/equity/dlq/replay/
          year=Y/month=MM/day=DD/dlq_<batch_id>.parquet

  DLQ records from different modes are never mixed. DLQ is S3-only.
  There is no Kafka DLQ topic.

Raw Kafka storage paths (for replay source)
--------------------------------------------
  Backfill raw (replay_path=backfill reads from here):
      s3://risk-platform-pushpa-analytics/kafka_raw/equity/backfill/
          year=YYYY/month=MM/day=DD/hour=HH/batch_<batch_id>.parquet

  Live raw (replay_path=live reads from here):
      s3://risk-platform-pushpa-analytics/kafka_raw/equity/live/
          year=YYYY/month=MM/day=DD/hour=HH/batch_<batch_id>.parquet

Late event handling
-------------------
  LIVE     : Events older than 5 minutes from current wall-clock time are
             dropped and routed to the live DLQ. Prevents stale data from
             corrupting rolling intraday metrics.
  BACKFILL : Late-event filtering is DISABLED. Backfill processes historical
             data by design; filtering would drop all events.
  REPLAY   : Late-event filtering is DISABLED. Historical events are always
             older than the late-event threshold by wall clock.

Redis policy
------------
  LIVE     : Redis snapshot published after every batch.
  BACKFILL : Redis is NEVER written or published to.
  REPLAY   : Redis is NEVER written or published to.
  Redis represents live state only.

Kafka topic routing
-------------------
  LIVE     : topic_live    = equity_stream
  BACKFILL : topic_backfill = equity_stream_backfill
  REPLAY   : no Kafka consumption — reads raw S3 parquet directly

State rebuild policy
--------------------
  State rebuild runs ONLY in live mode.
  State rebuild is NEVER run during backfill or replay.
  State rebuild is NEVER run at or near market open — buffers start empty.
  State rebuild loads only the recent recovery window (state_rebuild_minutes).
  State rebuild NEVER loads previous trading session data (e.g. Friday on Monday).
  State rebuild reads from: kafka_raw/equity/live/ (live raw data only).

Lineage on every output row
----------------------------
  offset           — exact offset in source partition
  partition        — source partition
  producer_pipeline_name — value from event envelope
  consumer_pipeline_name — "equity_stream_consumer"
  pipeline_run_id        — UUID per batch (constant within one batch)
  processing_timestamp   — ET timestamp when batch was processed
  run_mode               — "live", "backfill", or "replay"

Idempotency
-----------
  Spark checkpoints handle offset tracking (live and backfill modes).
  Replay output is append-only. Repeated replay runs create new uniquely
  named parquet files and may produce duplicate historical records.

Buffer safety
-------------
  MAX_BUFFER_PER_TICKER    — rolling window cap per symbol
  MAX_TOTAL_TICKER_SYMBOLS — evict oldest symbols when total count exceeds cap

Prometheus metrics
------------------
  Exposed on /metrics (port 8000 by default, configurable via METRICS_PORT).
  Prometheus scrapes every 60 s.
  Run-level counters only — no ticker / batch_id labels.

  consumer_batches_total              — total micro-batches processed
  consumer_failures_total             — batches that raised an unhandled exception
  consumer_dlq_total                  — total events routed to DLQ
  consumer_processed_s3_writes_total  — successful S3 output writes
  consumer_redis_writes_total         — successful Redis snapshot publishes (live only)
  consumer_records_processed_total    — total records processed
  consumer_last_success_timestamp     — Unix timestamp of last fully successful batch
  consumer_duration_seconds           — histogram of per-batch wall-clock duration

  Replay metrics (incremented in replay mode):
  replay_jobs_total                   — total replay jobs started
  replay_records_processed_total      — total records processed across all replay batches
  replay_failures_total               — replay partitions that failed
  replay_duration_seconds             — total wall-clock time of the replay job
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
from typing import List
import requests
import pendulum
from kafka import KafkaConsumer
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, explode, lit, monotonically_increasing_id
from pyspark.sql.types import (
    StructType, StructField, StringType, FloatType, ArrayType
)
import redis
from prometheus_client import (
    Counter, Gauge, Histogram, CollectorRegistry,
    push_to_gateway, start_http_server,
)

# =============================================================
# CONFIG  —  single source of truth.  No magic numbers in code.
# =============================================================

CONFIG: dict = {
    # Kafka
    "kafka_broker":    os.getenv("KAFKA_BROKER", "kafka:9092"),
    "topic_live":      "equity_stream",
    "topic_backfill":  "equity_stream_backfill",

    # Run mode — "live" | "backfill" | "replay"
    "run_mode":    os.getenv("RUN_MODE", "live").lower(),

    # Only consulted when run_mode == "replay".
    "replay_path": os.getenv("REPLAY_PATH", "live").lower(),

    # Replay date/hour filter (replay mode only)
    "replay_date": os.getenv("REPLAY_DATE", ""),   # "YYYY-MM-DD"
    "replay_hour": os.getenv("REPLAY_HOUR", ""),   # "HH" optional

    # S3
    "read_bucket":   "pushpa-equity-bucket",
    "write_bucket":  "risk-platform-pushpa-analytics",
    "positions_key": "historical-equity/final_merged.parquet",

    # Processed output paths — one per mode, fully isolated
    "output_prefix_live":      "equity/data/live",
    "output_prefix_backfill":  "equity/data/backfill",
    "output_prefix_replay":    "equity/data/replay",

    # DLQ paths — one per mode, fully isolated
    "dlq_prefix_live":      "equity/dlq/live",
    "dlq_prefix_backfill":  "equity/dlq/backfill",
    "dlq_prefix_replay":    "equity/dlq/replay",

    # Raw S3 sources for replay
    "raw_replay_backfill_prefix": "kafka_raw/equity/backfill/",
    "raw_replay_live_prefix":     "kafka_raw/equity/live/",

    # Raw S3 source for state rebuild (live recovery only)
    "raw_state_rebuild_prefix": "kafka_raw/equity/live/",

    # Spark checkpoint (live and backfill)
    "checkpoint_dir": (
        os.getenv("CHECKPOINT_DIR", "s3a://risk-platform-pushpa-analytics")
         + f"/equity/checkpoints/{os.getenv('RUN_MODE', 'live').lower()}"
    ),

    # Late event handling (live only)
    "late_event_max_minutes": 5,

    # Buffer safety (single definition)
    "max_buffer_per_ticker":    120,
    "max_total_ticker_symbols": 500,

    # Redis (live only)
    "redis_host":      os.getenv("REDIS_HOST", "localhost"),
    "redis_port":      int(os.getenv("REDIS_PORT", 6379)),
    "redis_db_stream": int(os.getenv("REDIS_DB_STREAM", 1)),

    # Lineage
    "consumer_pipeline_name": "equity_stream_consumer",
    "data_source":             "kafka_equity_stream",
    "transformation":          "intraday_metrics_v1",

    # Spark
    "spark_app_name":           "EquityKafkaStreaming_Production",
    "spark_shuffle_partitions":  "4",

    # Positions columns to drop
    "positions_drop_cols": [
        "option_type", "Discretion", "other_manager",
        "Sole", "Shared", "None", "beta",
        "dividendYield", "gdp", "unrate", "cpi", "fedfunds", "date"
    ],

    # Prometheus
    "pushgateway_url": os.getenv("PUSHGATEWAY_URL"),
    "metrics_port":    int(os.getenv("METRICS_PORT", "8000")),

    # Market hours (shutdown with 4-minute extension)
    "market_shutdown_hour":   16,
    "market_shutdown_minute":  4,

    # State rebuild — live mode recovery only.
    # Number of minutes of raw S3 data to load when recovering mid-session.
    # Set to 15 to ensure vol_15m can warm up immediately after recovery.
    "state_rebuild_minutes": 15,

    # Grace period after market open during which state rebuild is SKIPPED.
    # If startup occurs within market_open + state_rebuild_grace_minutes,
    # buffers start empty (new session, nothing to recover).
    "state_rebuild_grace_minutes": 15,
}

# Set Kafka topic based on run_mode (replay never consumes Kafka directly)
CONFIG["topic"] = (
    CONFIG["topic_backfill"]
    if CONFIG["run_mode"] == "backfill"
    else CONFIG["topic_live"]
)

# =============================================================
# MODE DETECTION  —  single source of truth, consulted throughout
# =============================================================

def is_live_mode() -> bool:
    """Return True when run_mode == "live"."""
    return CONFIG["run_mode"] == "live"


def is_backfill_mode() -> bool:
    """Return True when run_mode == "backfill"."""
    return CONFIG["run_mode"] == "backfill"


def is_replay_mode() -> bool:
    """Return True when run_mode == "replay"."""
    return CONFIG["run_mode"] == "replay"


# Module-level booleans for guards that run at import time
IS_LIVE     = is_live_mode()
IS_BACKFILL = is_backfill_mode()
IS_REPLAY   = is_replay_mode()


def _output_prefix_for_mode() -> str:
    """Return the S3 output prefix for the current run mode."""
    if IS_LIVE:
        return CONFIG["output_prefix_live"]
    if IS_BACKFILL:
        return CONFIG["output_prefix_backfill"]
    return CONFIG["output_prefix_replay"]


def _dlq_prefix_for_mode() -> str:
    """Return the S3 DLQ prefix for the current run mode."""
    if IS_LIVE:
        return CONFIG["dlq_prefix_live"]
    if IS_BACKFILL:
        return CONFIG["dlq_prefix_backfill"]
    return CONFIG["dlq_prefix_replay"]


def _replay_raw_prefix() -> str:
    """
    Return the S3 raw-data prefix for replay source selection.

    replay_path=backfill    -> kafka_raw/equity/backfill/  (historical backfill data)
    replay_path=live -> kafka_raw/equity/live/      (historical live data)
    """
    if CONFIG["replay_path"] == "backfill":
        return CONFIG["raw_replay_backfill_prefix"]
    return CONFIG["raw_replay_live_prefix"]

# =============================================================
# PROMETHEUS  — run-level counters, no ticker/batch labels
# =============================================================

_prom_registry = CollectorRegistry()

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
    "Total replay jobs started (run_mode=replay)",
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
PROM_LATE_EVENTS = Gauge(
    "consumer_late_events_count",
    "Number of late events filtered in current batch (LIVE mode only)",
    registry=_prom_registry,
)

PROM_FAILED_EVENTS = Gauge(
    "consumer_failed_events_count",
    "Number of events that failed processing in current batch",
    registry=_prom_registry,
)

PROM_DEDUPED_EVENTS = Gauge(
    "consumer_deduped_events_count",
    "Number of duplicate events removed in current batch",
    registry=_prom_registry,
)

PROM_BUFFER_SIZE = Gauge(
    "consumer_buffer_size_total",
    "Total number of bars across all ticker buffers",
    registry=_prom_registry,
)

PROM_TICKERS_IN_BUFFER = Gauge(
    "consumer_tickers_in_buffer_count",
    "Number of tickers currently in buffer",
    registry=_prom_registry,
)



# Shared run_id for Pushgateway job label (set in __main__)
_consumer_run_id: str = "unknown"


def _push_metrics() -> None:
    """
    Push accumulated Prometheus metrics to Pushgateway.
    Non-fatal — a Pushgateway failure must never stop the consumer.

    Push behavior by mode:
      LIVE     : called at market shutdown.
      BACKFILL : called once at job completion.
      REPLAY   : called once at replay job completion.
    """
    try:
        job_name = f"{CONFIG['consumer_pipeline_name']}_{CONFIG['run_mode']}_{_consumer_run_id}"
        
        # ── LOG success (no Slack) ──────────────────────────────────────
        log("INFO", "Pushing metrics to Pushgateway",
            {"job": job_name, "mode": CONFIG["run_mode"]})
        
        push_to_gateway(
            CONFIG["pushgateway_url"],
            job=job_name,
            registry=_prom_registry,
        )
        
        # ── Log success after push ──────────────────────────────────────
        log("INFO", "Metrics pushed successfully",
            {"job": job_name, "mode": CONFIG["run_mode"]})
        
    except Exception as exc:
        log("WARNING", "Prometheus push_to_gateway failed",
            {"error": str(exc), "mode": CONFIG["run_mode"]})
        
        # ─── Send Slack alert (message only) ────────────────────────────
        send_alert(
            f"❌ Metrics Push FAILED | mode={CONFIG['run_mode']} | error={str(exc)}"
        )        


def _start_metrics_server() -> None:
    """
    Expose /metrics on a background HTTP server for Prometheus scraping.
    Called only in live mode.
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

def send_alert(message: str) -> None:
    MAX_SLACK_LEN = 100
    log("CRITICAL", f"[ALERT] {message}")

    webhook = os.getenv("SLACK_WEBHOOK_URL")
    if not webhook:
        return

    try:
        if len(message) > MAX_SLACK_LEN:
            message = message[:MAX_SLACK_LEN] + "..."
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
# REDIS  —  snapshots ONLY (live mode only)
# =============================================================

# Initialised lazily so backfill and replay never attempt a Redis connection.
_redis_client = None


def _get_redis():
    """
    Return the shared Redis client, creating it on first call.
    Only called from save_latest_snapshot_all_tickers, which is guarded
    by is_live_mode() at the call site — never reached in backfill or replay.
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



def get_latest_file_per_partition(bucket: str, prefix: str) -> List[str]:
    """
    For each partition folder (date/hour), return only the most recent parquet file.
    
    This prevents processing duplicate data from multiple backfill runs.
    """
    s3 = get_s3()
    paginator = s3.get_paginator("list_objects_v2")
    
    # Group files by their partition path (everything before the filename)
    partitions: dict[str, list[dict]] = {}
    
    try:
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
        for page in pages:
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if not key.endswith(".parquet"):
                    continue
                
                # Extract partition path (everything except the filename)
                parts = key.split("/")
                partition_path = "/".join(parts[:-1])
                
                if partition_path not in partitions:
                    partitions[partition_path] = []
                partitions[partition_path].append({
                    "key": key,
                    "last_modified": obj["LastModified"]
                })
    except Exception as exc:
        log("ERROR", "Failed to list S3 files for latest file filter",
            {"bucket": bucket, "prefix": prefix, "error": str(exc)})
        return []
    
    # Keep only the most recent file per partition
    latest_files = []
    for partition_path, files in partitions.items():
        files.sort(key=lambda x: x["last_modified"], reverse=True)
        latest_files.append(files[0]["key"])
        
        if len(files) > 1:
            log("INFO", "Multiple files found in partition - using latest only", {
                "partition": partition_path,
                "selected": files[0]["key"],
                "skipped": len(files) - 1,
                "latest_modified": files[0]["last_modified"].isoformat()
            })
    
    return latest_files


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


def flush_dlq_to_s3(
    dlq_buffer: list,
    batch_id:   str,
) -> None:
    """
    Batch-flush DLQ records to the mode-specific S3 DLQ path as parquet.
    Called once per batch — not per failed event.

    Path:
      LIVE:     s3://risk-platform-pushpa-analytics/equity/dlq/live/
      BACKFILL: s3://risk-platform-pushpa-analytics/equity/dlq/backfill/
      REPLAY:   s3://risk-platform-pushpa-analytics/equity/dlq/replay/
          year=Y/month=MM/day=DD/dlq_<batch_id>.parquet

    DLQ is S3-only across all modes. Records from different modes are
    written to separate paths and are never mixed.
    """
    if not dlq_buffer:
        return

    dlq_prefix = _dlq_prefix_for_mode()
    df  = pd.DataFrame(dlq_buffer)
    now = pendulum.now("America/New_York")
    key = (
        f"{dlq_prefix}/"
        f"year={now.year}/month={now.month:02d}/day={now.day:02d}/"
        f"dlq_{batch_id}.parquet"
    )

    try:
        atomic_write_parquet_to_s3(df, CONFIG["write_bucket"], key)
        log("WARNING", "DLQ batch flushed to S3",
            {"batch_id": batch_id, "failed_rows": len(df),
             "key": key, "run_mode": CONFIG["run_mode"]})
    except Exception as exc:
        log("ERROR", "DLQ S3 flush failed — dumping to stdout",
            {"batch_id": batch_id, "error": str(exc)})
        send_alert(f"DLQ S3 flush FAILED | batch_id={batch_id} | error={str(exc)}")
        for entry in dlq_buffer:
            print(json.dumps({"DLQ_FALLBACK": entry}, default=str))

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
# WAIT FOR KAFKA TOPIC
# =============================================================
def wait_for_topic(timeout_seconds: int = 600) -> None:
    start = time.time()

    while True:
        consumer = None
        try:
            consumer = KafkaConsumer(
                bootstrap_servers=CONFIG["kafka_broker"],
            )

            if CONFIG["topic"] in consumer.topics():
                log(
                    "INFO",
                    "Kafka topic is available",
                    {"topic": CONFIG["topic"]},
                )

                consumer.close()
                return

        except Exception as exc:
            log(
                "WARNING",
                "Unable to connect to Kafka",
                {"error": str(exc)},
            )

        finally:
            if consumer is not None:
                consumer.close()

        if time.time() - start >= timeout_seconds:
            log(
                "ERROR",
                "Kafka topic not found within timeout",
                {
                    "topic": CONFIG["topic"],
                    "timeout_seconds": timeout_seconds,
                },
            )
            send_alert(
                f"Kafka topic '{CONFIG['topic']}' not found after {timeout_seconds} seconds"
            )
            raise RuntimeError(
                f"Kafka topic '{CONFIG['topic']}' not found after {timeout_seconds} seconds"
            )

        log(
            "WARNING",
            "Kafka topic does not exist yet. Retrying in 10 seconds...",
            {"topic": CONFIG["topic"]},
        )

        time.sleep(10)

# =============================================================
# STATE REBUILD  —  LIVE MODE RECOVERY ONLY
# =============================================================

def _get_now_et():
    """
    Return current time in Eastern Time (US/Eastern).
    Handles Daylight Saving Time automatically.
    """
    return pendulum.now("America/New_York")


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

    market_open  = _equity_session_open_et(now_et)
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
      - run_mode == "live"  (IS_LIVE = True)
      - Market is currently open (between 9:30 ET and 16:03 ET)
      - Startup is NOT within the grace window after market open

    NEVER runs during:
      - Backfill (IS_BACKFILL = True)
      - Replay   (IS_REPLAY   = True)
      - Pre-market or near-open startups (fresh session)
      - Weekends or after-market close

    WHAT IT LOADS
    -------------
    Scans kafka_raw/equity/live/ for partitions that fall within:
        [now - state_rebuild_minutes, now]

    Only the current trading day is scanned. The function determines which
    hour partitions overlap with the rebuild window and requests only those.
    Previous trading session data is NEVER loaded.

    DST HANDLING
    ------------
    Uses Eastern Time (America/New_York) with pendulum for automatic DST handling.
    S3 paths use ET for partitioning.
    """
    # Guard: only live mode
    if not IS_LIVE:
        log("INFO", "State rebuild skipped — not in live mode",
            {"run_mode": CONFIG["run_mode"]})
        return

    now_et = _get_now_et()

    # Guard: skip rebuild for fresh session starts or closed market
    if _should_skip_equity_state_rebuild(now_et):
        log("INFO", "State rebuild skipped — fresh session start or market closed",
            {"now_et":        now_et.isoformat(),
             "weekday":       now_et.weekday(),
             "grace_minutes": CONFIG["state_rebuild_grace_minutes"]})
        return

    rebuild_minutes  = CONFIG["state_rebuild_minutes"]
    rebuild_start_et = now_et - timedelta(minutes=rebuild_minutes)

    log("INFO", "Starting equity state rebuild",
        {"rebuild_start_et": rebuild_start_et.isoformat(),
         "rebuild_end_et":   now_et.isoformat(),
         "window_minutes":   rebuild_minutes,
         "current_et":       now_et.isoformat()})

    # Determine which hour partitions to scan
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

    # Collect keys only from required hour partitions - ONLY latest file per hour
    all_keys: List[str] = []
    s3 = get_s3()
    for hour in sorted(hours_to_scan):
        hour_prefix = f"{today_prefix}hour={hour:02d}/"
        try:
            #  Loads ALL files in the hour
            all_files = list_s3_files_with_prefix(CONFIG["write_bucket"], hour_prefix)
            all_keys.extend(all_files)
        except Exception as exc:
            log("WARNING", "State rebuild: failed to list S3 prefix",
                {"prefix": hour_prefix, "error": str(exc)})

    if not all_keys:
        log("INFO", "State rebuild: no raw S3 files found in recovery window",
            {"today_prefix": today_prefix, "hours": sorted(hours_to_scan)})
        return

    log("INFO", "State rebuild: raw S3 files found",
        {"file_count": len(all_keys), "hours": sorted(hours_to_scan)})

    # Read, filter to rebuild window, and populate ticker_buffers
    frames: List[pd.DataFrame] = []
    for key in all_keys:
        try:
            obj = s3.get_object(Bucket=CONFIG["write_bucket"], Key=key)
            df  = pd.read_parquet(BytesIO(obj["Body"].read()))
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

    if "timestamp" not in combined.columns:
        log("WARNING", "State rebuild: timestamp column missing — skipping rebuild")
        return

    combined["_evt_dt"] = pd.to_datetime(
        combined["timestamp"], utc=True, errors="coerce"
    ).dt.tz_convert("America/New_York")
    combined = combined.dropna(subset=["_evt_dt"])

    # Compare entirely in ET — no UTC intermediate — so rebuild
    # filtering matches the ET-based partitioning used everywhere else.
    rebuild_start_et_ts = pd.Timestamp(rebuild_start_et)
    now_et_ts           = pd.Timestamp(now_et)

    combined = combined[
        (combined["_evt_dt"] >= rebuild_start_et_ts)
        & (combined["_evt_dt"] <= now_et_ts)
    ]

    if combined.empty:
        log("INFO", "State rebuild: no events in rebuild window after filtering",
            {"rebuild_start_et": rebuild_start_et.isoformat(),
             "now_et":           now_et.isoformat()})
        return

    combined = combined.sort_values("_evt_dt").reset_index(drop=True)
    combined = combined.drop(columns=["_evt_dt"])

    required_cols = {"ticker", "open", "high", "low", "close", "volume"}
    missing = required_cols - set(combined.columns)
    if missing:
        log("WARNING", "State rebuild: required columns missing — skipping rebuild",
            {"missing_columns": list(missing)})
        return

    records_loaded  = 0
    records_deduped = 0
    for _, row in combined.iterrows():
        ticker = row.get("ticker")
        if not ticker:
            continue

        try:
            close  = float(row["close"])
            high   = float(row["high"])
            low    = float(row["low"])
            volume = float(row["volume"])  # noqa: F841
        except (TypeError, ValueError):
            continue

        if close <= 0 or high < low:
            continue

        row_ts = pd.to_datetime(row.get("timestamp"), utc=True, errors="coerce")
        if pd.isna(row_ts):
            continue
        row_ts = row_ts.tz_convert("America/New_York")

        # ─── Same persistent ordering/dedup guard the live streaming
        # loop uses. Raw S3 files written by the live producer can
        # legitimately contain the same (ticker, timestamp) row across
        # two different cycle files (no producer-side dedup by design).
        # list_s3_files_with_prefix above intentionally reads ALL files
        # in the scan window (correct for normal live cycles), so
        # without this check a restart could push a duplicate row into
        # the buffer twice, corrupting the exact rolling-metric window
        # the live path already protects.
        existing_buffer = ticker_buffers.get(ticker)
        if existing_buffer:
            last_ts = pd.to_datetime(
                existing_buffer[-1].get("timestamp"), utc=True, errors="coerce"
            )
            if pd.notna(last_ts):
                last_ts = last_ts.tz_convert("America/New_York")
                if row_ts <= last_ts:
                    records_deduped += 1
                    continue

        row_dict = row.to_dict()
        update_ticker_buffer(ticker, row_dict)
        records_loaded += 1

    log("INFO", "Equity state rebuild complete",
        {"records_loaded":        records_loaded,
         "records_deduped":       records_deduped,
         "tickers_in_buffer":     len(ticker_buffers),
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

    PERFORMANCE NOTE (backfill/replay fast path):
    If a buffer entry already carries a precomputed integer "_ts_epoch_min"
    key (set by run_ticker_engine() for backfill/replay), that integer is
    used directly for window comparisons instead of re-parsing the
    timestamp string with pd.to_datetime() on every call. This avoids
    millions of redundant scalar datetime parses when this function is
    called once per row against a buffer of up to max_buffer_per_ticker
    entries.

    LIVE MODE IS UNAFFECTED: live-mode buffer dicts never contain
    "_ts_epoch_min", so the exact original pd.to_datetime()-based logic
    below still runs for live mode, unchanged, with identical output.
    """
    closes  = np.array([r["close"]  for r in buffer])
    highs   = np.array([r["high"]   for r in buffer])  # noqa: F841
    lows    = np.array([r["low"]    for r in buffer])   # noqa: F841
    volumes = np.array([r["volume"] for r in buffer])   # noqa: F841

    use_fast_path = "_ts_epoch_min" in buffer[-1]

    window_1m  = []
    window_5m  = []
    window_15m = []

    if use_fast_path:
        # ── FAST PATH: integer minute comparisons, no datetime parsing ──
        latest_min = buffer[-1]["_ts_epoch_min"]

        if latest_min is not None:
            cutoff_1m  = latest_min - 1
            cutoff_5m  = latest_min - 5
            cutoff_15m = latest_min - 15

            for r in buffer:
                m = r.get("_ts_epoch_min")
                if m is None:
                    continue
                if m >= cutoff_1m:
                    window_1m.append(r)
                if m >= cutoff_5m:
                    window_5m.append(r)
                if m >= cutoff_15m:
                    window_15m.append(r)
    else:
        # ── ORIGINAL PATH (live mode) — untouched, byte-for-byte ────────
        try:
            latest_ts = pd.to_datetime(
                buffer[-1].get("timestamp"), utc=True, errors="coerce"
            ).floor("min")

            if pd.isna(latest_ts):
                latest_ts = None
        except Exception:
            latest_ts = None

        if latest_ts is not None:
            cutoff_1m  = latest_ts - pd.Timedelta(minutes=1)
            cutoff_5m  = latest_ts - pd.Timedelta(minutes=5)
            cutoff_15m = latest_ts - pd.Timedelta(minutes=15)

            for r in buffer:
                ts = pd.to_datetime(
                    r.get("timestamp"), utc=True, errors="coerce"
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
        metrics["close_diff"] = closes[-1] - closes[-2]

    if len(window_1m) >= 2:
        first_close = window_1m[0]["close"]
        last_close  = window_1m[-1]["close"]

        if pd.notna(first_close) and first_close != 0:
            metrics["return_1m"] = (last_close - first_close) / first_close

            latest_high = window_1m[-1]["high"]
            latest_low  = window_1m[-1]["low"]
            metrics["range_pct_1m"] = (latest_high - latest_low) / first_close

    if len(window_5m) >= 2:
        first_close = window_5m[0]["close"]
        last_close  = window_5m[-1]["close"]

        if pd.notna(first_close) and first_close != 0:
            metrics["return_5m"]      = (last_close - first_close) / first_close
            metrics["trend_slope_5m"] = (last_close - first_close) / 5

    if len(window_15m) >= 2:
        closes_15m = np.array([r["close"] for r in window_15m], dtype=float)
        returns    = (closes_15m[1:] - closes_15m[:-1]) / closes_15m[:-1]
        returns    = returns[np.isfinite(returns)]

        if len(returns) > 0:
            metrics["vol_15m"] = float(np.std(np.log1p(returns)))

    if len(window_5m) > 0:
        closes_5m  = np.array([r["close"]  for r in window_5m], dtype=float)
        highs_5m   = np.array([r["high"]   for r in window_5m], dtype=float)
        lows_5m    = np.array([r["low"]    for r in window_5m], dtype=float)
        volumes_5m = np.array([r["volume"] for r in window_5m], dtype=float)

        volume_sum = np.sum(volumes_5m)
        if volume_sum > 0:
            metrics["rolling_vwap_5m"] = float(
                np.sum(closes_5m * volumes_5m) / volume_sum
            )

        metrics["rolling_high_5m"] = float(np.max(highs_5m))
        metrics["rolling_low_5m"]  = float(np.min(lows_5m))

        high5 = np.max(highs_5m)
        low5  = np.min(lows_5m)
        rng   = high5 - low5

        if rng > 0:
            metrics["breakout_strength"] = (closes_5m[-1] - low5) / rng

    if len(window_5m) >= 2:
        vols       = np.array([r["volume"] for r in window_5m], dtype=float)
        avg_volume = np.mean(vols[:-1])

        if avg_volume > 0:
            metrics["volume_burst"] = float(vols[-1] / avg_volume)

    return metrics


# Canonical list of columns run_ticker_engine() adds on top of whatever
# input columns it receives. Used in two places that must stay in sync:
#   1. run_ticker_engine()'s own reindex step (guarantees these columns
#      always exist in its output, even if a whole ticker-partition has
#      zero valid rows).
#   2. build_backfill_output_schema() (tells Spark's applyInPandas what
#      schema to expect back from run_ticker_engine()).
BACKFILL_ENGINE_EXTRA_COLUMNS = [
    "prev_open", "prev_high", "prev_low", "prev_close", "prev_volume",
    "return_1m", "return_5m", "vol_15m", "range_pct_1m",
    "rolling_vwap_5m", "close_diff", "rolling_high_5m",
    "rolling_low_5m", "trend_slope_5m", "breakout_strength",
    "volume_burst", "_engine_error",
]


def update_ticker_buffer_local(buffer: list, row: dict, cap: int) -> None:
    """
    Same trim semantics as update_ticker_buffer(), but operates on a
    plain local list instead of the global ticker_buffers dict.

    Used ONLY by run_ticker_engine() below, for backfill/replay.
    Each backfill/replay ticker-partition gets its own fresh local buffer
    that starts empty and is discarded when the partition finishes —
    this matches the documented behavior that backfill/replay buffers
    always start empty (no state carried between runs or across tickers).
    """
    buffer.append(row)
    if len(buffer) > cap:
        del buffer[: len(buffer) - cap]


def run_ticker_engine(pdf: pd.DataFrame) -> pd.DataFrame:
    """
    BACKFILL/REPLAY ONLY. Never called from live mode.

    Given all rows for ONE ticker (Spark guarantees this via
    groupBy("ticker").applyInPandas(run_ticker_engine, ...)), rebuilds
    that ticker's rolling buffer from scratch and calls the exact same
    compute_metrics() function that live mode uses — same business
    logic, same output columns, no Spark SQL window functions involved.

    This function runs on a Spark executor. Spark's groupBy() only
    controls WHICH rows land together (all rows for this ticker) and
    WHERE they run (in parallel with every other ticker's partition) —
    it does not compute any metric itself.

    Two performance changes vs. the live-mode row loop, both purely
    mechanical (no business-logic change):
      1. pdf.to_dict("records") instead of pdf.iterrows() — avoids
         constructing a pandas Series per row.
      2. Timestamps are parsed ONCE for the whole ticker (vectorized),
         producing an integer "_ts_epoch_min" column, instead of once
         per buffer element per row. compute_metrics() then uses its
         fast integer path automatically because that key is present.
    """
    pdf = pdf.sort_values("timestamp").reset_index(drop=True)
    
    # Get ticker name for logging (will be the same for all rows in this partition)
    ticker = pdf.iloc[0]["ticker"] if len(pdf) > 0 else "unknown"
    total_rows = len(pdf)
    
    # ── Log the start of processing for this ticker (backfill/replay only).
    # This function is never called from live mode.
    if total_rows > 0:
        log("INFO", "TICKER_START", {
            "ticker": ticker,
            "total_rows": total_rows,
            "run_mode": CONFIG["run_mode"]
        })

    # Vectorized timestamp parse — ONE call for this ticker's entire
    # history, instead of one scalar pd.to_datetime() call per buffer
    # element per row (which is what made the original loop slow).
    parsed = pd.to_datetime(pdf["timestamp"], utc=True, errors="coerce")
    epoch_min = (parsed.view("int64") // 60_000_000_000)
    epoch_min = epoch_min.where(parsed.notna(), other=None)
    pdf = pdf.assign(_ts_epoch_min=epoch_min)

    cap = CONFIG["max_buffer_per_ticker"]
    buffer: list = []
    out_rows: list = []

    for i, row in enumerate(pdf.to_dict("records"), start=1):
        # ── Log progress every 1000 rows while processing this ticker.
        # Applies to both backfill and replay. Live mode never reaches here.
        if total_rows >= 1000 and i % 1000 == 0:
            pct_complete = round((i / total_rows) * 100, 1)
            log("INFO", "PROGRESS", {
                "ticker": ticker,
                "processed": i,
                "total": total_rows,
                "pct": pct_complete,
                "run_mode": CONFIG["run_mode"]
            })

        ticker = row.get("ticker")

        # Same validation as the live-mode loop — invalid rows are
        # skipped here and handled by the caller's DLQ pass separately
        # (see run_ticker_engine's caller in process_batch).
        try:
            if row["close"] <= 0 or row["high"] < row["low"]:
                row["_engine_error"] = (
                    f"Invalid market data: close={row['close']}, "
                    f"high={row['high']}, low={row['low']}"
                )
                out_rows.append(row)
                continue
        except Exception as exc:
            row["_engine_error"] = f"Validation error: {exc}"
            out_rows.append(row)
            continue

        update_ticker_buffer_local(buffer, row, cap)

        if len(buffer) >= 2:
            prev = buffer[-2]
            row.update({
                "prev_open":   prev.get("open",   np.nan),
                "prev_high":   prev.get("high",   np.nan),
                "prev_low":    prev.get("low",    np.nan),
                "prev_close":  prev.get("close",  np.nan),
                "prev_volume": prev.get("volume", np.nan),
            })
        else:
            row.update({
                "prev_open":   np.nan,
                "prev_high":   np.nan,
                "prev_low":    np.nan,
                "prev_close":  np.nan,
                "prev_volume": np.nan,
            })

        # ── SAME FUNCTION live mode uses — no duplicated business logic ──
        computed = compute_metrics(buffer)
        row.update(computed)
        row["_engine_error"] = None

        out_rows.append(row)

    # ── Log completion for this ticker (backfill/replay only).
    # Includes counts of successful and failed rows.
    if total_rows > 0:
        valid_rows = len([r for r in out_rows if r.get("_engine_error") is None])
        error_rows = len([r for r in out_rows if r.get("_engine_error") is not None])

        log("INFO", "TICKER_DONE", {
            "ticker": ticker,
            "total_rows": total_rows,
            "valid_rows": valid_rows,
            "error_rows": error_rows,
            "output_rows": len(out_rows),
            "run_mode": CONFIG["run_mode"]
        })

    result = pd.DataFrame(out_rows)
    if "_ts_epoch_min" in result.columns:
        result = result.drop(columns=["_ts_epoch_min"])

    # Guarantee every expected output column exists, even if this
    # ticker-partition had zero valid rows (all _engine_error) or was
    # otherwise empty. Spark's applyInPandas requires the returned
    # DataFrame to match the declared schema exactly — missing columns
    # would raise at the executor rather than degrade gracefully.
    for col in BACKFILL_ENGINE_EXTRA_COLUMNS:
        if col not in result.columns:
            result[col] = None

    return result


def build_backfill_output_schema(input_schema: StructType) -> StructType:
    """
    Build the Spark schema required by applyInPandas() for
    run_ticker_engine()'s output: the input batch's columns plus
    BACKFILL_ENGINE_EXTRA_COLUMNS. Derived from the input schema rather
    than hardcoded so it stays correct if upstream Kafka/S3 columns
    change. BACKFILL/REPLAY ONLY — live mode never calls this.
    """
    fields = list(input_schema.fields)
    existing_names = {f.name for f in fields}
    for name in BACKFILL_ENGINE_EXTRA_COLUMNS:
        if name not in existing_names:
            dtype = StringType() if name == "_engine_error" else FloatType()
            fields.append(StructField(name, dtype, True))
    return StructType(fields)


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
# SAVE OUTPUT  (atomic S3 write, mode-specific path, partitioned by date + hour)
# =============================================================

def save_to_parquet(df: pd.DataFrame, batch_id: str) -> None:
    """
    Write processed batch to S3 as parquet using atomic write.
    Partitioned by date and hour based on each row's timestamp.

    CRITICAL: Uses each row's individual timestamp for partitioning.
    This handles batches that span multiple hours (backfill/replay mode)
    correctly, instead of putting all rows into the first row's hour.

    Output path is selected based on run_mode:
      LIVE     : equity/data/live/date=.../hour=.../batch_<timestamp>.parquet
      BACKFILL : equity/data/backfill/date=.../hour=.../batch_<timestamp>.parquet
      REPLAY   : equity/data/replay/date=.../hour=.../batch_<timestamp>.parquet

    Backfill/Replay batch example:
        Input batch contains events from hour 09, 10, and 11 on same date.
        This function writes 3 separate parquet files:
            date=2024-01-15/hour=09/batch_20250115_143022_123.parquet
            date=2024-01-15/hour=10/batch_20250115_143023_456.parquet
            date=2024-01-15/hour=11/batch_20250115_143024_789.parquet

    Each run generates unique filenames using current timestamp (millisecond precision).
    This prevents file conflicts when running replay/backfill multiple times for the
    same date/hour partition.

    Outputs from different modes are fully isolated and never mixed.
    """
    if df.empty:
        return

    output_prefix = _output_prefix_for_mode()
    
    # Create temporary partition columns from each row's timestamp.
    # Always normalize to America/New_York explicitly here — parsing
    # with utc=True first (so both offset-aware and naive/UTC strings
    # are handled the same way) and then converting to ET guarantees
    # every partition folder is computed in ET, regardless of what the
    # source string's original offset was.
    ts_et = pd.to_datetime(df['timestamp'], utc=True).dt.tz_convert("America/New_York")
    df['_partition_date'] = ts_et.dt.strftime('%Y-%m-%d')
    df['_partition_hour'] = ts_et.dt.strftime('%H')
    
    # Group by date and hour, writing each group to its own partitioned file
    for (date, hour), group in df.groupby(['_partition_date', '_partition_hour']):
        # Derive batch_id from the group (all rows in this hour have same batch_id)
        batch_id = group.iloc[0]["batch_id"]
        
        consumer_timestamp = pendulum.now("America/New_York").strftime('%Y%m%d_%H%M%S_%f')[:-3]
        key = (
            f"{output_prefix}/"
            f"date={date}/hour={hour}/"
            f"batch_{batch_id}_{consumer_timestamp}.parquet"
        )
        
        # Drop temporary columns before writing
        group_to_write = group.drop(['_partition_date', '_partition_hour'], axis=1)
        
        try:
            atomic_write_parquet_to_s3(group_to_write, CONFIG["write_bucket"], key)
            PROM_S3_WRITES_TOTAL.inc()
        except Exception as exc:
            log("ERROR", "save_to_parquet failed for partition",
                {"batch_id": batch_id, "date": date, "hour": hour,
                 "run_mode": CONFIG["run_mode"], "error": str(exc)})
            send_alert(f"S3 write FAILED | batch_id={batch_id} | date={date} | hour={hour} | error={str(exc)}")
            raise
    
    # Log summary of what was written
    log("INFO", "save_to_parquet completed",
        {"batch_id": batch_id, "run_mode": CONFIG["run_mode"],
         "partitions_written": len(df.groupby(['_partition_date', '_partition_hour'])),
         "total_rows": len(df)})

# =============================================================
# SNAPSHOT TO REDIS  (LIVE MODE ONLY)
# =============================================================

def save_latest_snapshot_all_tickers(
    ticker_buffers: dict,
    positions_df:   pd.DataFrame,
    late_cutoff,
) -> None:
    """
    Build and publish the "never lose a ticker" snapshot to Redis #2
    ("equity_latest_snapshot" / "equity_stream" — pre-existing key
    names, kept unchanged for backward compatibility with whatever is
    already subscribed to them).

    LIVE MODE ONLY — this function must never be called during backfill or replay.
    The call site (process_batch) enforces this via is_live_mode().

    EVERY ticker that has ever produced a bar is included, using that
    ticker's own latest buffered row, regardless of whether it updated
    in the current batch or how long ago its last real row arrived.
    Each row is tagged is_stale (True when that ticker's last row is
    older than late_event_max_minutes) so the UI can render freshness
    without a ticker ever disappearing from the view.

    Business logic otherwise preserved exactly from original pipeline.
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
                "prev_open":   np.nan,
                "prev_high":   np.nan,
                "prev_low":    np.nan,
                "prev_close":  np.nan,
                "prev_volume": np.nan,
            })

        row_ts = pd.to_datetime(latest_row.get("timestamp"), utc=True, errors="coerce")
        latest_row["is_stale"] = bool(
            pd.isna(row_ts) or row_ts.tz_convert("America/New_York") < late_cutoff
        )

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


def publish_equity_synced_snapshot(synced_rows: list) -> None:
    """
    Publish the SYNCHRONIZED snapshot to Redis #1
    ("equity_latest_snapshot_synced" / "equity_stream_synced" — new
    key names, additive, does not replace the pre-existing store above).

    The call site (process_batch) passes only rows that are (a)
    genuinely new for their ticker — passed the persistent cross-batch
    ordering/dedup check, (b) within the live freshness window
    (late_event_max_minutes), and (c) all floored to the SAME minute as
    the batch's max minute — so every ticker in this snapshot is
    guaranteed as-of the identical instant. Use this store for anything
    needing cross-ticker consistency (e.g. a portfolio-level aggregate).

    LIVE MODE ONLY — same guard as save_latest_snapshot_all_tickers.
    """
    if not synced_rows:
        return

    try:
        df_synced     = pd.DataFrame(synced_rows)
        df_synced     = df_synced.where(pd.notnull(df_synced), None)
        snapshot_json = df_synced.to_json(orient="records")

        rc = _get_redis()
        rc.set("equity_latest_snapshot_synced", snapshot_json)
        rc.publish("equity_stream_synced", snapshot_json)

        PROM_REDIS_WRITES_TOTAL.inc()

    except Exception as exc:
        log("ERROR", "Redis synced snapshot publish failed", {"error": str(exc)})
        send_alert(f"Redis synced snapshot FAILED | error={str(exc)}")

# =============================================================
# ADD LINEAGE TO OUTPUT ROWS
# =============================================================

def add_lineage(
    row_dict:             dict,
    batch_id:             str,
    pipeline_run_id:      str,
    processing_timestamp: str,
) -> dict:
    """
    Stamp lineage fields onto an output row dict.
    All required lineage fields are set here; values are constant
    within a batch (pipeline_run_id, processing_timestamp) or per-row
    (offset, partition, producer_pipeline_name).

    run_mode is always set from CONFIG["run_mode"] directly —
    "live", "backfill", or "replay" — never derived from replay_path.
    """
    row_dict["consumer_pipeline_name"] = CONFIG["consumer_pipeline_name"]
    row_dict["pipeline_run_id"]        = pipeline_run_id
    row_dict["processing_timestamp"]   = processing_timestamp
    row_dict["data_source"]            = CONFIG["data_source"]
    row_dict["transformation"]         = CONFIG["transformation"]
    row_dict["record_created_at"]      = processing_timestamp
    row_dict["run_mode"]               = CONFIG["run_mode"]
    row_dict["batch_id"]               = batch_id
    return row_dict


# =============================================================
# BACKFILL / REPLAY — TRUE SPARK-DISTRIBUTED PATH
# =============================================================
#
# BACKFILL/REPLAY ONLY. Live mode never calls this function, and this
# function never touches ticker_buffers (the live-mode global dict) or
# any live-only behavior (Redis, late-event filtering, state rebuild).
#
# WHY THIS IS A SEPARATE FUNCTION, NOT A BRANCH INSIDE process_batch's
# EXISTING BODY:
# process_batch()'s live-mode path calls batch_df.toPandas() almost
# immediately — that's fine for live mode, where each micro-batch is
# small. For backfill/replay, doing that FIRST and only distributing
# work afterward (an earlier version of this fix did exactly that) means
# everything still collapses onto the driver before any distribution
# happens — the 10 executor pods requested in the deploy YAML would sit
# idle. This function is called BEFORE any toPandas(), directly on the
# Spark DataFrame, so groupBy("ticker").applyInPandas(...) actually ships
# each ticker's full row history to a separate executor task.
#
# NO SPARK SQL WINDOW FUNCTIONS ARE USED HERE. groupBy("ticker") is a
# shuffle/routing key only — all rolling-window math still happens
# inside compute_metrics(), the exact same function live mode calls,
# executed via run_ticker_engine() as plain Python/Pandas on whichever
# executor that ticker's partition lands on.
def process_batch_backfill_replay(
    batch_df,
    batch_id,
    positions_df:          pd.DataFrame,
    pipeline_run_id:       str,
    processing_timestamp:  str,
    batch_start:           float,
    metrics:               dict,
    dlq_buffer:            list,
) -> None:
    # Step 1: Deduplicate by (ticker, timestamp) — at the Spark level,
    # not pandas, so this itself is distributed rather than driver-local.
    original_count = batch_df.count()
    deduped_df     = batch_df.dropDuplicates(["ticker", "timestamp"])
    deduped_count  = deduped_df.count()
    metrics["events_deduped"] = original_count - deduped_count
    PROM_DEDUPED_EVENTS.set(metrics["events_deduped"])

    if deduped_count == 0:
        log("INFO", "All events in batch were duplicates",
            {"batch_id": batch_id, "original_count": original_count})
        metrics["batch_latency_s"] = round(time.monotonic() - batch_start, 3)
        log_batch_metrics(str(batch_id), metrics)
        return

    metrics["events_received"] = deduped_count

    log("INFO", "BACKFILL_REPLAY_ENGINE_START",
        {"batch_id": str(batch_id), "rows": deduped_count,
         "run_mode": CONFIG["run_mode"]})

    output_schema = build_backfill_output_schema(deduped_df.schema)

    # THE ACTUAL DISTRIBUTION. This is what spark.executor.instances=10
    # (set in the deploy YAML for backfill/replay) is actually used for —
    # Spark ships each ticker's full row history to a separate executor
    # task, in parallel, and run_ticker_engine() runs there unchanged.
    result_sdf = deduped_df.groupBy("ticker").applyInPandas(
        run_ticker_engine, schema=output_schema
    )

    # ONE collection to the driver — on the already-computed, much
    # smaller output (post-dedup, post-validation), not the raw input.
    result_pdf = result_sdf.toPandas()

    snapshot_rows: list = []
    for row_dict in result_pdf.to_dict("records"):
        if row_dict.get("_engine_error"):
            metrics["events_failed"] += 1
            dlq_buffer.append({
                "error":             row_dict["_engine_error"],
                "ticker":            row_dict.get("ticker", ""),
                "offset":            int(row_dict.get("offset", -1) or -1),
                "partition":         int(row_dict.get("partition", -1) or -1),
                "original_event":    row_dict,
                "failed_at":         pendulum.now("America/New_York").to_iso8601_string(),
                "consumer_pipeline": CONFIG["consumer_pipeline_name"],
                "batch_id":          str(batch_id),
                "pipeline_run_id":   pipeline_run_id,
                "run_mode":          CONFIG["run_mode"],
            })
            continue

        row_dict.pop("_engine_error", None)
        row_dict = add_lineage(
            row_dict             = row_dict,
            batch_id             = str(batch_id),
            pipeline_run_id      = pipeline_run_id,
            processing_timestamp = processing_timestamp,
        )
        snapshot_rows.append(row_dict)
        metrics["events_processed"] += 1
        PROM_RECORDS_PROCESSED_TOTAL.inc()

    PROM_FAILED_EVENTS.set(metrics["events_failed"])
    log("INFO", "BACKFILL_REPLAY_ENGINE_DONE",
        {"batch_id": str(batch_id), "rows_out": len(snapshot_rows)})

    if not snapshot_rows:
        PROM_DLQ_TOTAL.inc(len(dlq_buffer))
        flush_dlq_to_s3(dlq_buffer, str(batch_id))
        metrics["batch_latency_s"] = round(time.monotonic() - batch_start, 3)
        log_batch_metrics(str(batch_id), metrics)
        return

    snapshot_df = pd.DataFrame(snapshot_rows)
    snapshot_df = enrich_intraday_with_positions(snapshot_df, positions_df)
    snapshot_df = snapshot_df.where(pd.notnull(snapshot_df), None)

    save_to_parquet(snapshot_df, str(batch_id))

    PROM_DLQ_TOTAL.inc(len(dlq_buffer))
    flush_dlq_to_s3(dlq_buffer, str(batch_id))

    PROM_LAST_SUCCESS_TIMESTAMP.set(time.time())
    metrics["batch_latency_s"] = round(time.monotonic() - batch_start, 3)
    PROM_DURATION_SECONDS.observe(time.monotonic() - batch_start)
    log_batch_metrics(str(batch_id), metrics)


# =============================================================
# PROCESS BATCH  (core consumer logic — all modes share this)
# =============================================================

def process_batch(
    batch_df,
    batch_id:     int,
    positions_df: pd.DataFrame,
) -> None:
    """
    Spark foreachBatch handler AND replay batch processor.
    Called once per micro-batch (live/backfill) or per S3 partition (replay).

    Mode-specific behavior (enforced via IS_LIVE / IS_BACKFILL / IS_REPLAY):
      Output path  : equity/data/live/ | equity/data/backfill/ | equity/data/replay/
      DLQ path     : equity/dlq/live/  | equity/dlq/backfill/  | equity/dlq/replay/
      Redis        : two independent stores, LIVE ONLY (see below)
      Metrics push : at market end (LIVE ONLY); at job end (BACKFILL/REPLAY)

    Late/duplicate event handling (LIVE ONLY):
        A row only reaches S3/Redis#1 after a PERSISTENT, cross-batch
        ordering check: is its timestamp newer than the last timestamp
        already accepted into that ticker's buffer? This replaces
        relying on same-batch drop_duplicates alone, which has no
        memory across batches and would let an unchanged row re-fetched
        next cycle (or a batch straggling in late) re-enter the buffer
        as if it were new.
          - Not newer (duplicate)                    -> dropped, buffer untouched.
          - Newer AND within late_event_max_minutes   -> full processing:
            buffer update, rolling metrics, feeds S3 + Redis #1.
          - Newer BUT older than late_event_max_minutes -> still updates
            the buffer (it is real, just late) so it is never lost, but
            is excluded from S3 / Redis #1; NOT treated as an error and
            NOT routed to DLQ for lateness alone.

    Redis (LIVE ONLY — two separate stores, both read from state this
    function already maintains):
        Redis #2 "equity_latest_snapshot" / "equity_stream" (unchanged
            key names — this is the pre-existing store):
            EVERY ticker ever seen, built straight from ticker_buffers,
            each tagged is_stale. Published every live batch regardless
            of whether this cycle had new data for a given ticker — a
            ticker is never dropped from this view just because its
            feed went quiet.
        Redis #1 "equity_latest_snapshot_synced" / "equity_stream_synced"
            (new): only tickers that are new-this-cycle, within the
            freshness window, AND floored to the SAME minute as the
            batch's max minute — genuinely synchronized as-of one
            instant. Use for anything needing cross-ticker consistency
            (e.g. a portfolio-level aggregate).
        BACKFILL / REPLAY: Redis is NEVER touched, as before.

    Per-batch lifecycle:
      1. Convert Spark -> pandas
      2. Cheap intra-batch drop_duplicates (belt-and-suspenders only)
      3. Per-row: persistent ordering/dedup check -> buffer update ->
         metrics -> freshness branch -> lineage
      4. Enrich with positions
      5. Atomic S3 write to mode-specific output path (fresh rows only)
      6. Build Redis #1's same-minute synchronized subset; publish
         Redis #1 and Redis #2 (all tickers, unconditional)
      7. Flush DLQ to mode-specific S3 DLQ path (lateness alone no
         longer generates a DLQ entry)
      8. Expose metrics (LIVE ONLY — others push at job end)

    Idempotency:
        Spark checkpoints handle offset tracking (live and backfill modes).
        Replay output is append-only. Repeated replay runs create new uniquely
        named parquet files and may produce duplicate historical records.

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
    current_time         = pd.Timestamp.now(tz="America/New_York")
    late_cutoff          = current_time - pd.Timedelta(minutes=CONFIG["late_event_max_minutes"])

    PROM_BATCHES_TOTAL.inc()

    # =========================================================
    # BACKFILL / REPLAY — branch out to the Spark-distributed path
    # =========================================================
    # Must happen BEFORE toPandas() below — that conversion is what
    # collapses everything onto the driver. Live mode (IS_LIVE) never
    # enters this branch. process_batch_backfill_replay() does its own
    # count()==0 / deduped-count checks internally (not identical wording
    # to live mode's, since this is new code — but it does not touch or
    # alter live mode's own check below in any way).
    if IS_BACKFILL or IS_REPLAY:
        process_batch_backfill_replay(
            batch_df              = batch_df,
            batch_id              = batch_id,
            positions_df          = positions_df,
            pipeline_run_id       = pipeline_run_id,
            processing_timestamp  = processing_timestamp,
            batch_start           = batch_start,
            metrics               = metrics,
            dlq_buffer            = dlq_buffer,
        )
        return

    # ── LIVE MODE ──────────────────────────────────────────────────────
    # ── Convert Spark batch to pandas ─────────────────────────────────
    try:
        if batch_df.count() == 0:
            log("INFO", "Empty batch received",
                {"batch_id": batch_id, "run_mode": CONFIG["run_mode"]})
            return
        pdf = batch_df.toPandas()
        log("INFO", f"BACKFILL_ROWS={len(pdf)}")

    except Exception as exc:
        log("ERROR", "Failed to convert batch to pandas",
            {"batch_id": batch_id, "run_mode": CONFIG["run_mode"], "error": str(exc)})
        send_alert(f"Batch conversion FAILED | batch_id={batch_id} | error={str(exc)}")
        PROM_FAILURES_TOTAL.inc()
        return

    # Step 1: Deduplicate by (ticker, timestamp)
    original_count = len(pdf)
    pdf = pdf.drop_duplicates(subset=["ticker", "timestamp"])
    deduped_count  = len(pdf)
    metrics["events_deduped"] = original_count - deduped_count
    PROM_DEDUPED_EVENTS.set(metrics["events_deduped"])

    if deduped_count == 0:
        log("INFO", "All events in batch were duplicates",
            {"batch_id": batch_id, "original_count": original_count})
        metrics["batch_latency_s"] = round(time.monotonic() - batch_start, 3)
        log_batch_metrics(str(batch_id), metrics)
        return

    # Step 2: Annotate ET timestamp — LIVE ONLY.
    # No longer filters or drops rows here. Whether a row is a duplicate
    # or too late is now decided per-row inside the loop below, against
    # the PERSISTENT ticker_buffers state (not just this batch).
    if IS_LIVE and "timestamp" in pdf.columns:
        pdf["_evt_dt"] = (
            pd.to_datetime(pdf["timestamp"], utc=True, errors="coerce")
            .dt.tz_convert("America/New_York")
        )
    else:
        pdf["_evt_dt"] = pd.NaT

    PROM_BUFFER_SIZE.set(sum(len(buf) for buf in ticker_buffers.values()))
    PROM_TICKERS_IN_BUFFER.set(len(ticker_buffers))

    metrics["events_received"] = len(pdf)

    snapshot_rows: list = []
    log("INFO", "ROW_LOOP_START")
    for row in pdf.to_dict("records"):
        try:
            ticker = row["ticker"]

            # Data validation
            if row["close"] <= 0 or row["high"] < row["low"]:
                raise ValueError(
                    f"Invalid market data: close={row['close']}, "
                    f"high={row['high']}, low={row['low']}"
                )

            row_ts_et = row.get("_evt_dt")
            if row_ts_et is None or pd.isna(row_ts_et):
                raise ValueError(f"Unparseable timestamp: {row.get('timestamp')}")

            # ─── Persistent, cross-batch ordering/dedup check ───
            # Compares against the last timestamp already accepted into
            # THIS ticker's buffer — across batches, across a consumer
            # restart (ticker_buffers is repopulated by
            # rebuild_state_from_s3 before live processing resumes) —
            # not just against other rows in this same batch.
            existing_buffer = ticker_buffers.get(ticker)
            if existing_buffer:
                last_ts = pd.to_datetime(
                    existing_buffer[-1].get("timestamp"), utc=True, errors="coerce"
                )
                if pd.notna(last_ts):
                    last_ts = last_ts.tz_convert("America/New_York")
                    if row_ts_et <= last_ts:
                        metrics["events_deduped"] += 1
                        continue

            # Buffer update (with safety guards). `row` is already a
            # plain dict here (pdf.to_dict("records") yields dicts, not
            # Series) — the original `row.to_dict()` call would raise
            # AttributeError on every single row; fixed to just copy it.
            row_dict = dict(row)
            row_dict.pop("_evt_dt", None)
            update_ticker_buffer(ticker, row_dict)

            # Add prev OHLCV values from buffer
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
                    "prev_open":   np.nan,
                    "prev_high":   np.nan,
                    "prev_low":    np.nan,
                    "prev_close":  np.nan,
                    "prev_volume": np.nan,
                })

            # Feature computation — unconditional, same as buffer update
            # above: rolling metrics never depend on freshness.
            computed = compute_metrics(ticker_buffers[ticker])
            row_dict.update(computed)

            # ─── Freshness branch — output-only, buffer already updated ───
            if row_ts_et < late_cutoff:
                # Genuinely new (passed ordering above) but arrived
                # later than the live freshness window. The buffer
                # update above already means ticker_buffers — and
                # therefore Redis #2 — reflect it. Excluded from
                # S3/Redis#1 only. Not discarded, not a DLQ-worthy
                # error — lateness alone is not a failure.
                metrics["events_late"] += 1
                log("INFO", "Row processed (buffer/Redis#2 updated) but "
                             "excluded from S3/Redis#1 — outside freshness window",
                    {"ticker": ticker, "timestamp": row_dict.get("timestamp"),
                     "batch_id": str(batch_id)})
                continue

            # Lineage stamping — run_mode set from CONFIG["run_mode"] directly
            row_dict = add_lineage(
                row_dict             = row_dict,
                batch_id             = str(batch_id),
                pipeline_run_id      = pipeline_run_id,
                processing_timestamp = processing_timestamp,
            )
            row_dict["producer_pipeline_name"] = row["producer_pipeline_name"]
            row_dict["producer_run_id"]        = row["producer_run_id"]
            row_dict["source_fetch_time"]      = row["source_fetch_time"]

            snapshot_rows.append(row_dict)
            metrics["events_processed"] += 1
            PROM_RECORDS_PROCESSED_TOTAL.inc()

        except Exception as exc:
            metrics["events_failed"] += 1
            error_payload = {
                "error":             str(exc),
                "ticker":            row.get("ticker", ""),
                "offset":      int(row.get("offset", -1)),
                "partition":   int(row.get("partition", -1)),
                "original_event":    {k: v for k, v in row.items() if k != "_evt_dt"},
                "failed_at":         pendulum.now("America/New_York").to_iso8601_string(),
                "consumer_pipeline": CONFIG["consumer_pipeline_name"],
                "batch_id":          str(batch_id),
                "pipeline_run_id":   pipeline_run_id,
                "run_mode":          CONFIG["run_mode"],
            }
            dlq_buffer.append(error_payload)
    log("INFO", "ROW_LOOP_DONE")

    PROM_LATE_EVENTS.set(metrics["events_late"])
    PROM_DEDUPED_EVENTS.set(metrics["events_deduped"])
    PROM_FAILED_EVENTS.set(metrics["events_failed"])

    log("INFO", "Processing events",
        {"batch_id": batch_id, "event_count": len(snapshot_rows),
         "run_mode": CONFIG["run_mode"]})

    # ── Build Redis #1's synchronized subset ────────────────────────
    # From the fresh + deduped rows, keep only those floored to the SAME
    # minute as the batch's max minute, so every ticker in this snapshot
    # is genuinely as-of one identical instant.
    synced_rows: list = []
    if snapshot_rows:
        ts_index = pd.to_datetime(
            [r["timestamp"] for r in snapshot_rows], utc=True, errors="coerce"
        ).tz_convert("America/New_York")
        floored_minutes = ts_index.floor("min")
        max_minute      = floored_minutes.max()
        synced_rows = [
            row for row, minute in zip(snapshot_rows, floored_minutes)
            if minute == max_minute
        ]

        log("INFO", "ENRICH_START")
        snapshot_df = pd.DataFrame(snapshot_rows)
        snapshot_df = enrich_intraday_with_positions(snapshot_df, positions_df)
        snapshot_df = snapshot_df.where(pd.notnull(snapshot_df), None)
        log("INFO", "ENRICH_DONE")

        log("INFO", "SAVE_START")
        # S3 write — mode-specific path; ALL fresh+deduped rows, not
        # just the synchronized subset. Outputs are always isolated.
        save_to_parquet(snapshot_df, str(batch_id))
        log("INFO", "SAVE_DONE")

    # ── Redis — LIVE ONLY, two independent stores ──────────────────────
    # Redis #1 "equity_latest_snapshot_synced"/"equity_stream_synced":
    #   only the synchronized subset above.
    # Redis #2 "equity_latest_snapshot"/"equity_stream" (pre-existing
    #   key names, unchanged): EVERY ticker ever seen, rebuilt straight
    #   from ticker_buffers (which the loop above already kept current,
    #   including late-but-real rows) — published every live batch
    #   regardless of whether snapshot_rows is empty this cycle, so a
    #   ticker is never dropped from view just because it didn't have
    #   fresh data this batch.
    if IS_LIVE:
        if synced_rows:
            publish_equity_synced_snapshot(synced_rows)
        save_latest_snapshot_all_tickers(ticker_buffers, positions_df, late_cutoff)

    log("INFO", "DLQ_START")
    # Flush DLQ to mode-specific S3 DLQ path
    PROM_DLQ_TOTAL.inc(len(dlq_buffer))
    flush_dlq_to_s3(dlq_buffer, str(batch_id))
    log("INFO", "DLQ_DONE")

    PROM_LAST_SUCCESS_TIMESTAMP.set(time.time())
    metrics["batch_latency_s"] = round(time.monotonic() - batch_start, 3)
    PROM_DURATION_SECONDS.observe(time.monotonic() - batch_start)
    log("INFO", "PROCESS_BATCH_DONE")

# =============================================================
# S3 REPLAY  (run_mode = "replay")
# =============================================================

def load_s3_replay_partitions(
    spark,
    date_str:     str,
    hour_str:     str = "",
    positions_df: pd.DataFrame = None,
) -> None:
    """
    Read raw event parquet files from S3 via Spark and process them
    through the same pipeline as the Kafka stream. Used when
    run_mode = "replay".

    CHANGED FROM EARLIER VERSION: previously this function read each
    file individually via boto3 + pd.read_parquet(), then called
    process_batch() once per file, sequentially, wrapped in a
    _MockBatch shim — meaning replay never used Spark at all, and the
    per-file loop was itself a sequential bottleneck (paid once per S3
    file, regardless of how few rows each file has).

    Now: ALL matched files are read in ONE spark.read.parquet(*paths)
    call (Spark parallelizes the actual reads across executors), and
    process_batch() is called ONCE on the resulting Spark DataFrame.
    Because IS_REPLAY is True, process_batch() routes internally to
    process_batch_backfill_replay(), which distributes the per-ticker
    metric computation across executors via
    groupBy("ticker").applyInPandas(run_ticker_engine, ...) — this is
    what spark.executor.instances=10 (deploy YAML) is actually used for.

    Replay source (controlled by replay_path):
      replay_path=backfill    -> kafka_raw/equity/backfill/  (historical backfill raw)
      replay_path=live -> kafka_raw/equity/live/      (historical live raw)

    Output isolation:
      All output is written to equity/data/replay/ — never mixed with live
      or backfill output. process_batch always runs as IS_REPLAY.

    Redis:
      NEVER touched during replay. process_batch enforces this via IS_REPLAY.

    State rebuild:
      NEVER called during replay. ticker_buffers (live mode's global dict)
      is never touched by replay at all now — run_ticker_engine() uses
      its own local per-partition buffer, which always starts empty.

    Late event filtering:
      DISABLED during replay — process_batch_backfill_replay() never
      applies it (that logic lives only in live mode's code path).

    Prometheus replay metrics:
        replay_jobs_total              — once per invocation
        replay_records_processed_total — total rows across all files
        replay_failures_total          — on read or processing failure
        replay_duration_seconds        — total wall-clock time
    """
    if not date_str:
        log("ERROR", "REPLAY_DATE must be set for replay mode.")
        return

    replay_start = time.monotonic()
    PROM_REPLAY_JOBS_TOTAL.inc()

    raw_prefix = _replay_raw_prefix()

    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        base_prefix = raw_prefix if raw_prefix.endswith("/") else raw_prefix + "/"
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
         "replay_path":   CONFIG["replay_path"],
         "prefix":        prefix,
         "output_prefix": CONFIG["output_prefix_replay"]})

    # S3 listing is metadata-only (cheap regardless of file count) — still
    # done via boto3, unchanged. Only the actual DATA read changes below.
    keys = get_latest_file_per_partition(CONFIG["write_bucket"], prefix)
    if not keys:
        log("WARNING", "No raw parquet files found for replay",
            {"prefix": prefix})
        return

    log("INFO", "Replay partitions found", {"count": len(keys)})

    s3_paths = [f"s3a://{CONFIG['write_bucket']}/{k}" for k in keys]

    try:
        batch_df = spark.read.parquet(*s3_paths)
    except Exception as exc:
        log("ERROR", "Failed to read replay parquet files via Spark",
            {"error": str(exc), "file_count": len(s3_paths)})
        PROM_REPLAY_FAILURES_TOTAL.inc()
        return

    # Synthesize Kafka metadata columns if absent — same fields as before,
    # now added via Spark withColumn (distributed) instead of pandas.
    existing_cols = set(batch_df.columns)
    if "topic" not in existing_cols:
        batch_df = batch_df.withColumn("topic", lit(CONFIG["topic"]))
    if "partition" not in existing_cols:
        batch_df = batch_df.withColumn("partition", lit(0))
    if "offset" not in existing_cols:
        # synthetic, non-colliding — same intent as the old
        # "batch_num * 10000" scheme, expressed at the Spark level
        batch_df = batch_df.withColumn(
            "offset", (monotonically_increasing_id() + 1) * 10000
        )

    try:
        total_rows = batch_df.count()
    except Exception as exc:
        log("ERROR", "Failed to count replay batch", {"error": str(exc)})
        PROM_REPLAY_FAILURES_TOTAL.inc()
        return

    if total_rows == 0:
        log("INFO", "Replay dataset is empty after read", {"prefix": prefix})
        return

    log("INFO", "Replaying S3 partitions via Spark",
        {"files": len(keys), "rows": total_rows})
    PROM_REPLAY_RECORDS_TOTAL.inc(total_rows)

    # ONE call. IS_REPLAY routes process_batch() to
    # process_batch_backfill_replay(), which does its own
    # groupBy("ticker").applyInPandas(...) distribution across the
    # entire dataset at once — this also removes the old sequential
    # per-file Python loop entirely.
    try:
        process_batch(batch_df, 1, positions_df)
    except Exception as exc:
        log("ERROR", "S3 replay batch failed", {"error": str(exc)})
        PROM_REPLAY_FAILURES_TOTAL.inc()

    replay_duration = time.monotonic() - replay_start
    PROM_REPLAY_DURATION_SECONDS.observe(replay_duration)
    _push_metrics()

    send_alert(
        f"EQUITY REPLAY COMPLETE: {date_str} | replay_path={CONFIG['replay_path']} "
        f"| {len(keys)} files | {round(replay_duration, 3)}s"
    )

    log("INFO", "S3 replay complete",
        {"date":            date_str,
         "replay_path":     CONFIG["replay_path"],
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
    StructField("producer_run_id",       StringType(), True),
    StructField("batch_id",              StringType(), True),
    StructField("source_fetch_time",     StringType(), True),
    StructField("producer_pipeline_name", StringType(), True),
    StructField("batch_ts",              StringType(), True),
    StructField(
        "tickers",
        ArrayType(
            StructType([
                StructField("ticker",    StringType(), True),
                StructField("open",      FloatType(),  True),
                StructField("high",      FloatType(),  True),
                StructField("low",       FloatType(),  True),
                StructField("close",     FloatType(),  True),
                StructField("volume",    FloatType(),  True),
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
    run_id = str(uuid.uuid4())
    _consumer_run_id = run_id  # used by _push_metrics for Pushgateway job label

    log("INFO", "Consumer starting",
        {"run_mode":    CONFIG["run_mode"],
         "replay_path": CONFIG["replay_path"],
         "run_id":      run_id})

    # Start /metrics HTTP server (background thread — live mode only)
    if IS_LIVE:
        _start_metrics_server()

    # Load static positions once at startup (read-only bucket)
    positions_df = load_positions()

    # SparkSession is now built ONCE here, before the mode branch, so
    # replay mode can use it too (previously replay never created a
    # SparkSession at all — it read via boto3/pandas only, meaning
    # spark.executor.instances configured in the deploy YAML was never
    # actually used). Live/backfill's own spark.readStream setup below
    # is unaffected — it's the same build_spark_session() call, just
    # relocated earlier instead of duplicated.
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    if IS_REPLAY:
        # ── REPLAY MODE ──────────────────────────────────────────────
        # Reads raw S3 parquet via Spark (spark.read.parquet, inside
        # load_s3_replay_partitions) — no Kafka consumption, no
        # streaming. Spark here is used purely as a distributed batch
        # compute engine for run_ticker_engine() via applyInPandas.
        # State rebuild is NEVER called; buffers warm from replay records.
        # Replay source: replay_path=backfill -> backfill raw, replay_path=live -> live raw
        log("INFO", "Starting replay — state rebuild skipped (replay mode)",
            {"replay_date": CONFIG["replay_date"],
             "replay_hour": CONFIG["replay_hour"],
             "replay_path": CONFIG["replay_path"]})

        load_s3_replay_partitions(
            spark        = spark,
            date_str     = CONFIG["replay_date"],
            hour_str     = CONFIG["replay_hour"],
            positions_df = positions_df,
        )
        # Sleep 10 minutes after replay completes before exiting
        log("INFO", "Replay complete - waiting 10 minutes before final exit")
        time.sleep(600)  # 10 minutes = 600 seconds
        log("INFO", "Exiting replay consumer")

    else:
        # ── LIVE or BACKFILL — Kafka Structured Streaming ────────────
        log("INFO", "Starting Kafka stream pipeline",
            {"run_mode": CONFIG["run_mode"], "topic": CONFIG["topic"]})

        # State rebuild — live recovery only.
        # Skipped automatically for: backfill, replay, fresh session starts,
        # pre-market startups, and weekends. Previous trading session data
        # is NEVER loaded.
        rebuild_state_from_s3()
        wait_for_topic()

        if IS_LIVE:   
            raw_df = (
                spark.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", CONFIG["kafka_broker"])
                .option("subscribe", CONFIG["topic"])
                .option("startingOffsets", "latest")
                .option("failOnDataLoss", "false")
                .load()
            )
        else:
            raw_df = (
                spark.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", CONFIG["kafka_broker"])
                .option("subscribe", CONFIG["topic"])
                .option("startingOffsets", "earliest")
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

        if IS_LIVE: 
            query = (
                parsed_df.writeStream
                .foreachBatch(
                    lambda batch_df, batch_id:
                        process_batch(batch_df, batch_id, positions_df)
                )
                .outputMode("append")
                .option("checkpointLocation", CONFIG["checkpoint_dir"])
                .start()
            )

           # Market-shutdown loop (live mode only)
            while True:
                now = pendulum.now("America/New_York")

                if (
                    now.hour   == CONFIG["market_shutdown_hour"]
                    and now.minute >= CONFIG["market_shutdown_minute"]
                ):
                    log(
                        "INFO",
                        "Market shutdown time reached - stopping consumer cleanly",
                        {
                            "shutdown_time": (
                                f"{CONFIG['market_shutdown_hour']}:"
                                f"{CONFIG['market_shutdown_minute']:02d}"
                            ),
                            "run_mode": CONFIG["run_mode"],
                        },
                    )
                    
                    query.stop()

                    try:
                        _push_metrics()
                    except Exception:
                        pass

                    # Sleep for minutes when market closes
                    log("INFO", "Waiting 2 minutes before final exit")
                    time.sleep(120)  # Only executes during shutdown

                    break
                time.sleep(30)
        
        else:
            query = (
                parsed_df.writeStream
                .foreachBatch(
                    lambda batch_df, batch_id:
                        process_batch(batch_df, batch_id, positions_df)
                )
                .outputMode("append")
                .option("checkpointLocation", CONFIG["checkpoint_dir"])
                .trigger(availableNow=True)
                .start()
            )
            # BACKFILL mode - await termination and push metrics at end
            log("INFO", "Backfill mode - awaiting stream termination")
            query.awaitTermination()
            
            # Push metrics once at completion
            try:
                _push_metrics()
            except Exception:
                pass
 
            send_alert(
                f"EQUITY BACKFILL COMPLETE | "
                f"date={CONFIG['backfill_date']}"
            )
            # Sleep 10 minutes after backfill completes before exiting
            log("INFO", "Backfill complete - waiting 10 minutes before final exit")
            time.sleep(600)  # 10 minutes = 600 seconds
            log("INFO", "Exiting Backfill consumer")