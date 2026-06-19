"""
fx_consumer.py
=====================
Production-grade Spark Structured Streaming consumer for real-time FX ticks.

Storage rules
-------------
READ-ONLY  : s3://yeeshu-fx-bucket                      (FX universe / positions)
ALL WRITES : s3://risk-platform-pushparag-analytics      (parquet output, DLQ)

Storage layout (writes)
-----------------------
Processed output — LIVE (parquet, partitioned by date + hour):
    s3://risk-platform-pushparag-analytics/fx/data/live/
        date=YYYY-MM-DD/hour=HH/batch_<batch_id>.parquet

Processed output — BACKFILL (parquet, partitioned by date + hour):
    s3://risk-platform-pushparag-analytics/fx/data/backfill/
        date=YYYY-MM-DD/hour=HH/batch_<batch_id>.parquet

Processed output — REPLAY (parquet, partitioned by date + hour):
    s3://risk-platform-pushparag-analytics/fx/data/replay/
        date=YYYY-MM-DD/hour=HH/batch_<batch_id>.parquet

    Each mode writes to its own isolated prefix. Modes never share output paths.
    run_mode=replay NEVER writes to fx/data/live/ or fx/data/backfill/.

Consumer DLQ (S3, parquet, partitioned by day) — mode-isolated:
    LIVE:
        s3://risk-platform-pushparag-analytics/fx/dlq/live/
            year=Y/month=MM/day=DD/dlq_<batch_id>.parquet
    BACKFILL:
        s3://risk-platform-pushparag-analytics/fx/dlq/backfill/
            year=Y/month=MM/day=DD/dlq_<batch_id>.parquet
    REPLAY:
        s3://risk-platform-pushparag-analytics/fx/dlq/replay/
            year=Y/month=MM/day=DD/dlq_<batch_id>.parquet

    DLQ is S3-only. There is NO Kafka DLQ topic.

Raw Kafka storage (for state rebuild and replay source):
    LIVE state rebuild:
        s3://risk-platform-pushparag-analytics/kafka_raw/fx/live/
            year=YYYY/month=MM/day=DD/hour=HH/batch_<batch_id>.parquet
    REPLAY source (replay_path=backfill — historical backfill raw):
        s3://risk-platform-pushparag-analytics/kafka_raw/fx/backfill/
            year=YYYY/month=MM/day=DD/hour=HH/batch_<batch_id>.parquet
    REPLAY source (replay_path=live — historical live raw):
        s3://risk-platform-pushparag-analytics/kafka_raw/fx/live/
            year=YYYY/month=MM/day=DD/hour=HH/batch_<batch_id>.parquet

Kafka topics
------------
Live mode    : fx_ohlc_1m
Backfill mode: fx_ohlc_1m_backfill
Topic routing is determined at startup from the run_mode environment variable.

Replay modes
------------
run_mode=replay, replay_path=live -> reads raw parquet from kafka_raw/fx/live/ directly,
                         processes through the same pipeline, writes to fx/data/replay/.
run_mode=replay, replay_path=backfill   -> reads raw parquet from kafka_raw/fx/backfill/ directly,
                         processes through the same pipeline, writes to fx/data/replay/.
                         Replay NEVER touches Redis and writes only to fx/data/replay/.

FX Replay uses a date range (no hour concept):
    REPLAY_START_DATE=YYYY-MM-DD
    REPLAY_END_DATE=YYYY-MM-DD

Redis policy
------------
Redis represents LIVE state only.
    Live mode    : Redis snapshot published after every batch.
    Backfill mode: Redis is NEVER written or published to.
    Replay mode  : Redis is NEVER written or published to.

State rebuild policy
--------------------
State rebuild runs ONLY in live mode (run_mode=live).
State rebuild is NEVER run during replay or backfill.
State rebuild is NEVER run at or near FX weekly session open — buffers
  start empty for every new weekly session.
State rebuild loads only the recent recovery window (state_rebuild_minutes).
State rebuild NEVER loads Friday session data on Sunday (previous week).

Late Event Handling
-------------------
LIVE mode    : Events older than 5 minutes from current time are skipped.
               Late events are routed to the live DLQ (S3).
BACKFILL mode: Late-event filtering is DISABLED. No late events are written to DLQ.
REPLAY mode  : Late-event filtering is DISABLED. Historical events are always
               older than 5 minutes by wall clock — filtering would drop every event.
               No late events are written to DLQ.

Lineage on every output row
----------------------------
    offset           — exact offset in source partition
    partition        — source partition
    producer_pipeline_name — value from event envelope
    consumer_pipeline_name — "fx_stream_consumer"
    pipeline_run_id        — UUID per batch (constant within one batch)
    processing_timestamp   — ET timestamp when batch was processed

Idempotency
-----------
    Spark checkpoints handle offset tracking (live/backfill).
    Replay output is append-only; repeated replay runs create new parquet
    files and may produce duplicate historical records.

Buffer safety
-------------
    MAX_BUFFER_PER_PAIR  — rolling window cap per currency pair
    MAX_TOTAL_PAIRS      — evict oldest pairs when total count exceeds cap

Prometheus metrics
------------------
    Exposed on /metrics (port 8000 by default, configurable via METRICS_PORT).
    Prometheus scrapes every 60 s.
    Run-level counters only — no pair / batch_id labels.

    consumer_batches_total             — total micro-batches processed
    consumer_failures_total            — batches that raised an unhandled exception
    consumer_dlq_total                 — total events routed to DLQ (S3)
    consumer_processed_s3_writes_total — successful S3 output writes
    consumer_redis_writes_total        — successful Redis snapshot publishes (live only)
    consumer_records_processed_total   — total records processed
    consumer_last_success_timestamp    — Unix timestamp of last fully successful batch
    consumer_duration_seconds          — histogram of per-batch wall-clock duration

    Metrics push behavior by mode:
    LIVE     : /metrics server is started; metrics pushed after each successful batch
               and at market shutdown.
    BACKFILL : /metrics server is NOT started; metrics pushed at end of backfill
              
    REPLAY   : /metrics server is NOT started; metrics pushed at end of replay job.

    Replay metrics (incremented when run_mode=replay):
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
    "topic_live":            "fx_ohlc_1m",
    "topic_backfill":        "fx_ohlc_1m_backfill",

    # Run mode
    "run_mode":              os.getenv("RUN_MODE", "live").lower(),

    # Replay
    "replay_path":           os.getenv("REPLAY_PATH", "live"),   # "live" | "backfill"
    "replay_start_date":     os.getenv("REPLAY_START_DATE", ""),   # "YYYY-MM-DD"
    "replay_end_date":       os.getenv("REPLAY_END_DATE", ""),     # "YYYY-MM-DD"

    # S3
    "read_bucket":           "yeeshu-fx-bucket",
    "write_bucket":          "risk-platform-pushparag-analytics",
    "universe_key":          "historical-fx/final_merged.parquet",

    # Mode-isolated output prefixes — NEVER mix modes
    "output_prefix_live":      "fx/data/live",
    "output_prefix_backfill":  "fx/data/backfill",
    "output_prefix_replay":    "fx/data/replay",

    # Mode-isolated DLQ prefixes — S3-only, no Kafka DLQ
    "dlq_prefix_live":         "fx/dlq/live",
    "dlq_prefix_backfill":     "fx/dlq/backfill",
    "dlq_prefix_replay":       "fx/dlq/replay",

    # Raw Kafka storage (for state rebuild — live recovery only)
    "raw_state_rebuild_prefix": "kafka_raw/fx/live/",

    # Raw Kafka storage (replay sources — selected by replay_path)
    # replay_path=backfill    -> read historical backfill raw files
    # replay_path=live -> read historical live raw files
    "raw_replay_prefix_backfill": "kafka_raw/fx/backfill",
    "raw_replay_prefix_live":     "kafka_raw/fx/live",

    # Spark checkpoint (live and backfill)
    "checkpoint_dir": (
        os.getenv("CHECKPOINT_DIR", "s3a://risk-platform-pushparag-analytics")
         + f"/fx/checkpoints/{os.getenv('RUN_MODE', 'live').lower()}"
    ),

    # Late event handling — enforced in LIVE mode only
    "late_event_max_minutes":   5,

    # Buffer safety (single definition)
    "max_buffer_per_pair":      60,    # FX 24/5 
    "max_total_pairs":          50,    # FX universe is small — conservative cap

    # Redis (snapshots only; only used in live mode)
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

    # Market Hours — FX shuts down at 17:00 ET on Fridays (4 minutes extension)
    "market_shutdown_hour":   17,
    "market_shutdown_minute": 4,



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

# Set topic dynamically based on run_mode.
# Replay mode does not consume Kafka directly — it reads raw S3 parquet files.
if CONFIG["run_mode"] == "backfill":
    CONFIG["topic"] = CONFIG["topic_backfill"]
elif CONFIG["run_mode"] == "live":
    CONFIG["topic"] = CONFIG["topic_live"]
else:
    # replay — no Kafka topic consumed
    CONFIG["topic"] = None

# =============================================================
# MODE DETECTION  —  single source of truth consulted throughout
# =============================================================

def is_live_mode() -> bool:
    """Return True when the consumer is processing the live Kafka stream."""
    return CONFIG["run_mode"] == "live"


def is_backfill_mode() -> bool:
    """Return True when the consumer is processing the backfill Kafka topic."""
    return CONFIG["run_mode"] == "backfill"


def is_replay_mode() -> bool:
    """Return True when the consumer is replaying from raw S3 parquet files."""
    return CONFIG["run_mode"] == "replay"


# Module-level booleans derived from the mode helpers — used as guards
# throughout the module to avoid repeated function calls.
IS_LIVE     = is_live_mode()
IS_BACKFILL = is_backfill_mode()
IS_REPLAY   = is_replay_mode()

# =============================================================
# MODE-AWARE ROUTING HELPERS  —  resolve S3 prefixes by mode
# =============================================================

def _output_prefix_for_mode() -> str:
    """
    Return the correct S3 output prefix for the current run mode.

    LIVE     -> fx/data/live/
    BACKFILL -> fx/data/backfill/
    REPLAY   -> fx/data/replay/

    Modes are fully isolated — no cross-writes are possible.
    """
    if IS_REPLAY:
        return CONFIG["output_prefix_replay"]
    if IS_BACKFILL:
        return CONFIG["output_prefix_backfill"]
    return CONFIG["output_prefix_live"]


def _dlq_prefix_for_mode() -> str:
    """
    Return the correct S3 DLQ prefix for the current run mode.

    LIVE     -> fx/dlq/live/
    BACKFILL -> fx/dlq/backfill/
    REPLAY   -> fx/dlq/replay/

    DLQ records from different modes are never mixed.
    """
    if IS_REPLAY:
        return CONFIG["dlq_prefix_replay"]
    if IS_BACKFILL:
        return CONFIG["dlq_prefix_backfill"]
    return CONFIG["dlq_prefix_live"]


# =============================================================
# PROMETHEUS  — run-level counters, no pair/batch labels
# =============================================================

_prom_registry = CollectorRegistry()

# Consumer metrics (all modes share these)
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
    "Total events routed to DLQ (S3)",
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
    "Total number of bars across all pair buffers",
    registry=_prom_registry,
)

PROM_PAIRS_IN_BUFFER = Gauge(
    "consumer_pairs_in_buffer_count",
    "Number of currency pairs currently in buffer",
    registry=_prom_registry,
)


# Replay-specific metrics
PROM_REPLAY_JOBS_TOTAL = Counter(
    "replay_jobs_total",
    "Total replay jobs started (REPLAY_PATH=backfill)",
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

    Called:
        LIVE     : after each successful batch and at market shutdown.
        BACKFILL : once after the backfill stream completes.
        REPLAY   : once after the replay job completes.
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
    Passes _prom_registry explicitly so all custom metrics are visible at /metrics.
    The same registry is used for push_to_gateway, ensuring consistency between
    scraped and pushed metric values.

    Started in LIVE mode only. Backfill and replay do not start the HTTP server.
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
    Only called from publish_fx_snapshot_to_redis, which is already
    guarded by IS_LIVE — so this is never reached during replay or backfill.
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


def get_latest_file_per_partition(bucket: str, prefix: str) -> List[str]:
    """
    For each partition folder (date=YYYY-MM-DD/), return only the most recent parquet file.
    
    This prevents processing duplicate data from multiple backfill runs.
    """
    s3 = get_s3()
    paginator = s3.get_paginator("list_objects_v2")
    
    # Group files by their partition path (everything before the last '/')
    partitions: Dict[str, List[dict]] = {}
    
    try:
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
        for page in pages:
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if not key.endswith(".parquet"):
                    continue
                
                # Extract partition path (date=YYYY-MM-DD/ folder)
                # Example: "kafka_raw/fx/live/year=2024/month=01/day=15/batch_xxx.parquet"
                parts = key.split("/")
                # Everything except the filename (last part)
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
    DLQ is S3-only across all modes — there is no Kafka DLQ topic.

    Output path is mode-isolated:
        LIVE     : fx/dlq/live/year=Y/month=MM/day=DD/dlq_<batch_id>.parquet
        BACKFILL : fx/dlq/backfill/year=Y/month=MM/day=DD/dlq_<batch_id>.parquet
        REPLAY   : fx/dlq/replay/year=Y/month=MM/day=DD/dlq_<batch_id>.parquet
    """
    if not dlq_buffer:
        return

    df  = pd.DataFrame(dlq_buffer)
    now = pendulum.now("America/New_York")
    dlq_prefix = _dlq_prefix_for_mode()
    key = (
        f"{dlq_prefix}/"
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
    Load the FX-exposed from S3.
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
      - run_mode == "live"   (IS_LIVE is True, IS_REPLAY and IS_BACKFILL are both False)
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
    Scans kafka_raw/fx/live/ for partitions that fall within:
        [now - state_rebuild_minutes, now]

    Only the current calendar day is scanned. The function determines
    which hour partitions overlap with the rebuild window and requests
    only those — it never scans full date ranges or previous days.

    PREVIOUS SESSION ISOLATION
    --------------------------
    The prefix always includes the current day's date:
        kafka_raw/fx/live/year=Y/month=MM/day=DD/
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

    Bars are sorted ascending by timestamp before insertion so buffer
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

    base_prefix = CONFIG["raw_state_rebuild_prefix"]
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
            #  Loads ALL files in the hour
            all_files = list_s3_files_with_prefix(CONFIG["write_bucket"], hour_prefix)
            all_keys.extend(all_files)
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

    # Filter to only events within the rebuild window
    if "timestamp" not in combined.columns:
        log("WARNING", "FX state rebuild: timestamp column missing — skipping rebuild")
        return

    combined["_evt_dt"] = pd.to_datetime(
        combined["timestamp"],
        utc=True,
        errors="coerce"
    )

    combined = combined.dropna(subset=["_evt_dt"])

    rebuild_start_utc = pd.Timestamp(rebuild_start_et).tz_convert("UTC")
    now_utc           = pd.Timestamp(now_et).tz_convert("UTC")

    combined = combined[
        (combined["_evt_dt"] >= rebuild_start_utc)
        & (combined["_evt_dt"] <= now_utc)
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
            high  = float(row["high"])
            low   = float(row["low"])
        except (TypeError, ValueError):
            continue

        if close <= 0 or high < low:
            continue

        # Build a clean OHLC bar — identical structure to what the live
        # pipeline stores via update_pair_buffer
        clean_bar = {
            "currency_pair": pair,
            "open":          float(row["open"]),
            "high":          high,
            "low":           low,
            "close":         close,
            "timestamp":     row.get("timestamp", ""),
            "ingested_at":   row.get("ingested_at", ""),
        }

        update_pair_buffer(pair, clean_bar)
        records_loaded += 1

    log("INFO", "FX state rebuild complete",
        {"records_loaded":        records_loaded,
         "pairs_in_buffer":       len(pair_buffers),
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

    Metrics are calculated using timestamp-based windows rather than
    row counts. All timestamps are normalized to minute granularity
    before window construction so second-level differences do not
    affect metric calculations.

    Metrics:
        fx_return_1m
            Return between the oldest and newest observations within
            the last 1-minute window.

        fx_return_5m
            Return between the oldest and newest observations within
            the last 5-minute window.

        fx_vol_15m
            Standard deviation of log returns within the last
            15-minute window.
    """
    metrics = {
        "fx_return_1m": np.nan,
        "fx_return_5m": np.nan,
        "fx_vol_15m":   np.nan,
    }

    if not buffer:
        return metrics

    latest_ts = pd.to_datetime(
        buffer[-1].get("timestamp"),
        utc=True,
        errors="coerce",
    )

    if pd.isna(latest_ts):
        return metrics

    latest_ts  = latest_ts.floor("min")
    cutoff_1m  = latest_ts - pd.Timedelta(minutes=1)
    cutoff_5m  = latest_ts - pd.Timedelta(minutes=5)
    cutoff_15m = latest_ts - pd.Timedelta(minutes=15)

    window_1m  = []
    window_5m  = []
    window_15m = []

    for row in buffer:
        ts = pd.to_datetime(
            row.get("timestamp"),
            utc=True,
            errors="coerce",
        )

        if pd.isna(ts):
            continue

        ts = ts.floor("min")

        if ts >= cutoff_1m:
            window_1m.append(row)

        if ts >= cutoff_5m:
            window_5m.append(row)

        if ts >= cutoff_15m:
            window_15m.append(row)

    # ---------------------------------------------------------
    # 1-minute return
    # ---------------------------------------------------------
    if len(window_1m) >= 2:
        first_close = window_1m[0]["close"]
        last_close  = window_1m[-1]["close"]

        if pd.notna(first_close) and first_close != 0:
            metrics["fx_return_1m"] = (last_close - first_close) / first_close

    # ---------------------------------------------------------
    # 5-minute return
    # ---------------------------------------------------------
    if len(window_5m) >= 2:
        first_close = window_5m[0]["close"]
        last_close  = window_5m[-1]["close"]

        if pd.notna(first_close) and first_close != 0:
            metrics["fx_return_5m"] = (last_close - first_close) / first_close

    # ---------------------------------------------------------
    # 15-minute volatility
    # ---------------------------------------------------------
    if len(window_15m) >= 2:
        closes_15m  = np.array([row["close"] for row in window_15m], dtype=float)
        returns     = np.diff(closes_15m) / closes_15m[:-1]
        log_returns = np.log1p(returns)

        if len(log_returns) > 0:
            metrics["fx_vol_15m"] = float(np.std(log_returns))

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
            "currency_pair":         pair,
            "open":                  bar.get("open"),
            "high":                  bar.get("high"),
            "low":                   bar.get("low"),
            "close":                 bar.get("close"),
            "timestamp":             bar.get("timestamp"),
            "ingested_at":           bar.get("ingested_at"),

            # Ticker exposure
            "ticker":                ticker_row["ticker"],
            "sector":                ticker_row.get("sector", ""),
            "revenue":               nz(ticker_row.get("revenue")),
            "foreign_revenue_ratio": nz(ticker_row.get("foreign_revenue_ratio")),
            "position_size":         position_size,

            # Previous FX bar values
            "prev_open":             bar.get("prev_open",  np.nan),
            "prev_high":             bar.get("prev_high",  np.nan),
            "prev_low":              bar.get("prev_low",   np.nan),
            "prev_close":            bar.get("prev_close", np.nan),

            # FX metrics
            "fx_return_1m":          nz(fx_metrics["fx_return_1m"]),
            "fx_return_5m":          nz(fx_metrics["fx_return_5m"]),
            "fx_vol_15m":            fx_vol_15m,

            # Risk metrics
            "fx_pnl":                nz(position_size * fx_return_1m),
            "VaR_95_15m":            nz(position_size * fx_vol_15m * z95),
        })

    return rows

# =============================================================
# SAVE OUTPUT  (atomic S3 write, partitioned by date + hour)
# =============================================================

def save_to_parquet(df: pd.DataFrame, batch_id: str) -> None:
    """
    Write processed FX batch to S3 as parquet using atomic write.
    Partitioned by date and hour based on each row's timestamp.

    CRITICAL: Uses each row's individual timestamp for partitioning.
    This handles batches that span multiple hours (backfill/replay mode)
    correctly, instead of putting all rows into the first row's hour.

    Output path is determined by the current run mode via _output_prefix_for_mode():
        LIVE     : fx/data/live/date=.../hour=.../batch_<timestamp>.parquet
        BACKFILL : fx/data/backfill/date=.../hour=.../batch_<timestamp>.parquet
        REPLAY   : fx/data/replay/date=.../hour=.../batch_<timestamp>.parquet

    Backfill/Replay batch example:
        Input batch contains events from hour 09, 10, and 11 on same date.
        This function writes 3 separate parquet files:
            date=2024-01-15/hour=09/batch_20250115_143022_123.parquet
            date=2024-01-15/hour=10/batch_20250115_143023_456.parquet
            date=2024-01-15/hour=11/batch_20250115_143024_789.parquet

    Each run generates unique filenames using current timestamp (millisecond precision).
    This prevents file conflicts when running replay/backfill multiple times for the
    same date/hour partition.

    Modes are fully isolated — no cross-writes are possible.
    """
    if df.empty:
        return

    output_prefix = _output_prefix_for_mode()
    
    # Create temporary partition columns from each row's timestamp
    df['_partition_date'] = pd.to_datetime(df['timestamp']).dt.strftime('%Y-%m-%d')
    df['_partition_hour'] = pd.to_datetime(df['timestamp']).dt.strftime('%H')
    
    # Group by date and hour, writing each group to its own partitioned file
    for (date, hour), group in df.groupby(['_partition_date', '_partition_hour']):
        # Derive batch_id from the group (all rows in this hour have same batch_id)
        batch_id = group.iloc[0]["batch_id"]
        
        consumer_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S_%f')[:-3]
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
                 "mode": CONFIG["run_mode"], "error": str(exc)})
            send_alert(f"S3 write FAILED | batch_id={batch_id} | date={date} | hour={hour} | error={str(exc)}")
            raise
    
    # Log summary of what was written
    log("INFO", "save_to_parquet completed",
        {"batch_id": batch_id, "mode": CONFIG["run_mode"], 
         "partitions_written": len(df.groupby(['_partition_date', '_partition_hour'])),
         "total_rows": len(df)})

# =============================================================
# REDIS SNAPSHOT  (LIVE MODE ONLY)
# =============================================================

def publish_fx_snapshot_to_redis(snapshot_rows: list) -> None:
    """
    Publish the latest FX snapshot to Redis for real-time consumers.
    SET + PUBLISH.

    LIVE MODE ONLY — this function must never be called during replay or backfill.
    The call site (process_batch) enforces this via IS_LIVE.
    Redis represents live market state only; historical modes must never
    overwrite the current live snapshot with stale data.
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
    producer_pipeline:    str,
) -> dict:
    """
    Stamp all required lineage fields onto an output row.
    Constant fields (pipeline_run_id, processing_timestamp) are set here.
    Per-row fields (offset, partition) are passed in from the source row.
    run_mode reflects the actual execution mode (live / backfill / replay).
    """
    # Determine the lineage run_mode label
    if IS_REPLAY:
        run_mode_label = "replay"
    elif IS_BACKFILL:
        run_mode_label = "backfill"
    else:
        run_mode_label = "live"

    row_dict["consumer_pipeline_name"] = CONFIG["consumer_pipeline_name"]
    row_dict["pipeline_run_id"]        = pipeline_run_id
    row_dict["processing_timestamp"]   = processing_timestamp
    row_dict["data_source"]            = CONFIG["data_source"]
    row_dict["transformation"]         = CONFIG["transformation"]
    row_dict["record_created_at"]      = processing_timestamp
    row_dict["run_mode"]               = run_mode_label
    row_dict["producer_pipeline_name"] = producer_pipeline
    return row_dict


def add_prev_ohlcv_to_bar(row_dict: dict, buffer: list) -> dict:
    """
    Add previous OHLCV values to an FX bar dict from the pair buffer.

    If buffer has at least 2 rows, prev values come from the second-to-last row.
    If buffer has only 1 row (first bar for pair), prev values are set to NaN.

    Args:
        row_dict: The current bar dict (latest FX data)
        buffer:   The pair buffer list (all historical bars for this pair)

    Returns:
        Updated row_dict with prev_open, prev_high, prev_low, prev_close fields
    """
    if len(buffer) >= 2:
        prev = buffer[-2]  # second-to-last bar
        row_dict.update({
            "prev_open":  prev.get("open",  np.nan),
            "prev_high":  prev.get("high",  np.nan),
            "prev_low":   prev.get("low",   np.nan),
            "prev_close": prev.get("close", np.nan),
        })
    else:
        # First bar for this pair — no previous values available
        row_dict.update({
            "prev_open":  np.nan,
            "prev_high":  np.nan,
            "prev_low":   np.nan,
            "prev_close": np.nan,
        })

    return row_dict


# =============================================================
# PROCESS BATCH  (core consumer logic — all modes share this)
# =============================================================

def process_batch(
    batch_df,
    batch_id:    int,
    pair_groups: dict,
) -> None:
    """
    Spark foreachBatch handler AND replay batch processor.
    Called once per micro-batch (live/backfill) or per S3 partition (replay).

    Mode behavior is determined by the IS_LIVE / IS_BACKFILL / IS_REPLAY
    module-level flags derived from CONFIG at startup.

    Output path routing:
        LIVE     -> fx/data/live/
        BACKFILL -> fx/data/backfill/
        REPLAY   -> fx/data/replay/

    DLQ routing (S3-only, no Kafka DLQ):
        LIVE     -> fx/dlq/live/
        BACKFILL -> fx/dlq/backfill/
        REPLAY   -> fx/dlq/replay/

    Late event filtering:
        LIVE     : Events older than late_event_max_minutes are filtered and
                   routed to the live S3 DLQ.
        BACKFILL : Late-event filtering is DISABLED. No late DLQ writes.
        REPLAY   : Late-event filtering is DISABLED. Historical events are always
                   older than late_event_max_minutes by wall clock — filtering
                   would drop every event. No late DLQ writes.

    Redis:
        LIVE     : Snapshot published after every successful batch.
        BACKFILL : Redis is NEVER touched.
        REPLAY   : Redis is NEVER touched.

    Prometheus metrics:
        LIVE     : Pushed after each successful batch.
        BACKFILL : Not pushed from process_batch; pushed once when the
                backfill stream completes.
        REPLAY   : Not pushed from process_batch; pushed once when the
                replay job completes.

    Per-batch lifecycle:
      1. Convert Spark -> pandas
      2. Deduplicate by (currency_pair, timestamp)
      3. Filter late events [LIVE ONLY]
      4. Per-bar: validation -> buffer update -> FX metrics -> fan-out -> lineage
      5. Atomic S3 write (mode-isolated output prefix)
      6. Redis snapshot [LIVE ONLY]
      7. Flush consumer DLQ to S3 (mode-isolated DLQ prefix)
      8. Push Prometheus metrics [LIVE and BACKFILL — replay pushes at job end]

    Idempotency:
    - Spark checkpoints handle offset tracking (live/backfill mode)
    - Replay output is append-only; repeated replay runs create new uniquely
        named parquet files and may produce duplicate historical records.
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
                {"batch_id": batch_id, "run_mode": CONFIG["run_mode"]})
            return
        pdf = batch_df.toPandas()

        print(
            f"[CONSUMER-IN] batch={batch_id} "
            f"rows={len(pdf)} "
            f"pairs={sorted(pdf['currency_pair'].unique().tolist())}"
        )
    except Exception as exc:
        log("ERROR", "Failed to convert FX batch to pandas",
            {"batch_id": batch_id, "run_mode": CONFIG["run_mode"], "error": str(exc)})
        send_alert(f"Batch conversion FAILED | batch_id={batch_id} | error={str(exc)}")
        PROM_FAILURES_TOTAL.inc()
        return

    # ── Step 1: Deduplicate by timestamp (pure data logic) ─────────────
    original_count            = len(pdf)
    pdf                       = pdf.drop_duplicates(subset=["currency_pair", "timestamp"])
    deduped_count             = len(pdf)
    metrics["events_deduped"] = original_count - deduped_count

    if deduped_count == 0:
        log("INFO", "All FX events in batch were duplicates",
            {"batch_id": batch_id, "original_count": original_count})
        metrics["batch_latency_s"] = round(time.monotonic() - batch_start, 3)
        log_batch_metrics(str(batch_id), metrics)
        return

    # ── Step 2: Late event filtering ──────────────────────────────────
    # LIVE MODE ONLY: filter events older than late_event_max_minutes to
    # prevent stale data from corrupting rolling metrics.
    # BACKFILL and REPLAY: late filtering is DISABLED.
    #   - Backfill processes historical data; filtering would cause incorrect
    #     data loss from legitimate historical batches.
    #   - Replay events are always older than late_event_max_minutes by wall
    #     clock — filtering would drop every event.
    # Late DLQ writes only occur in live mode.
    if IS_LIVE and "timestamp" in pdf.columns:
        pdf["_evt_dt"] = (
            pd.to_datetime(pdf["timestamp"], utc=True, errors="coerce")
            .dt.tz_convert("America/New_York")
        )
        late_events            = pdf[pdf["_evt_dt"] < late_cutoff]
        pdf                    = pdf[pdf["_evt_dt"] >= late_cutoff].copy()
        metrics["events_late"] = len(late_events)
    


        for _, late_row in late_events.iterrows():
            dlq_entry = {
                "error":             (
                    f"Late FX event skipped — older than "
                    f"{CONFIG['late_event_max_minutes']} minutes"
                ),
                "currency_pair":     late_row.get("currency_pair", ""),
                "timestamp":         late_row.get("timestamp", ""),
                "current_time":      current_time.isoformat(),
                "offset":            int(late_row.get("offset", -1)),
                "partition":   int(late_row.get("partition", -1)),
                "consumer_pipeline": CONFIG["consumer_pipeline_name"],
                "batch_id":          str(batch_id),
                "pipeline_run_id":   pipeline_run_id,
            }
            dlq_buffer.append(dlq_entry)

        pdf = pdf.drop(columns=["_evt_dt"])

    # ========== GAUGES FOR ALL MODES (OUTSIDE THE IF BLOCK) ==========
    PROM_LATE_EVENTS.set(metrics["events_late"])
    PROM_DEDUPED_EVENTS.set(metrics["events_deduped"])
    PROM_BUFFER_SIZE.set(sum(len(buf) for buf in pair_buffers.values()))
    PROM_PAIRS_IN_BUFFER.set(len(pair_buffers))
    
    if len(pdf) == 0:
        log("INFO", "All FX events filtered",
            {"batch_id":   batch_id,
             "late_count": metrics["events_late"],
             "run_mode":   CONFIG["run_mode"]})
        PROM_DLQ_TOTAL.inc(len(dlq_buffer))
        flush_consumer_dlq_to_s3(dlq_buffer, str(batch_id))
        metrics["batch_latency_s"] = round(time.monotonic() - batch_start, 3)
        log_batch_metrics(str(batch_id), metrics)
        return

    metrics["events_received"] = len(pdf)
    log("INFO", "FX batch processed successfully", {"batch_id": str(batch_id)})

    output_rows: list = []

    # ── Step 3: Process each FX bar ───────────────────────────────────
    for _, bar in pdf.iterrows():
        try:
            pair = bar["currency_pair"]

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
                "timestamp":     bar.get("timestamp", ""),
                "ingested_at":   bar.get("ingested_at", ""),
            }

            update_pair_buffer(pair, clean_bar)

            # ─── Add prev OHLCV values to the clean bar ───
            clean_bar = add_prev_ohlcv_to_bar(clean_bar, pair_buffers[pair])

            # FX rolling metrics (uses clean buffer)
            fx_metrics = compute_fx_metrics(pair_buffers[pair])

            # Fan-out to per-ticker exposure rows (uses clean bar)
            exposure_rows = compute_exposure_rows(
                bar         = clean_bar,
                pair        = pair,
                fx_metrics  = fx_metrics,
                pair_groups = pair_groups,
            )

            if not exposure_rows:
                continue


            producer_pipeline = bar.get("producer_pipeline_name", "fx_kafka_producer")

            for out_row in exposure_rows:
                out_row = add_lineage(
                    row_dict             = out_row,
                    batch_id             = str(batch_id),
                    pipeline_run_id      = pipeline_run_id,
                    processing_timestamp = processing_timestamp,
                    producer_pipeline    = producer_pipeline,
                )
                output_rows.append(out_row)

            metrics["events_processed"] += 1
            PROM_RECORDS_PROCESSED_TOTAL.inc()

        except Exception as exc:
            metrics["events_failed"] += 1
            dlq_entry = {
                "error":             str(exc),
                "currency_pair":     bar.get("currency_pair", ""),
                "offset":            int(bar.get("offset", -1)),
                "partition":   int(bar.get("partition", -1)),
                "original_event":    bar.to_dict(),
                "failed_at":         pendulum.now("America/New_York").to_iso8601_string(),
                "consumer_pipeline": CONFIG["consumer_pipeline_name"],
                "batch_id":          str(batch_id),
                "pipeline_run_id":   pipeline_run_id,
                "run_mode":          CONFIG["run_mode"],
            }
            dlq_buffer.append(dlq_entry)


    # ========== SET FAILED EVENTS GAUGE ==========
    # Count events that failed (those with "original_event" in dlq_buffer)
    metrics["events_failed"] = len([e for e in dlq_buffer if "original_event" in e])
    PROM_FAILED_EVENTS.set(metrics["events_failed"])

    metrics["output_rows"] = len(output_rows)

    if not output_rows:
        PROM_DLQ_TOTAL.inc(len(dlq_buffer))
        flush_consumer_dlq_to_s3(dlq_buffer, str(batch_id))
        metrics["batch_latency_s"] = round(time.monotonic() - batch_start, 3)
        log_batch_metrics(str(batch_id), metrics)
        return

    # ── Step 4: Build output DataFrame, clean NaN ─────────────────────
    df_out = pd.DataFrame(output_rows)
    df_out = df_out.where(pd.notnull(df_out), None)

    # ── Step 5: Atomic S3 write — mode-isolated output prefix ─────────
    # LIVE     -> fx/data/live/
    # BACKFILL -> fx/data/backfill/
    # REPLAY   -> fx/data/replay/
    save_to_parquet(df_out, str(batch_id))

    # ── Step 6: Redis snapshot — LIVE ONLY ────────────────────────────
    # Redis represents live market state only.
    # Backfill and replay must never overwrite the live snapshot with
    # stale historical data.
    if IS_LIVE:
        pairs = sorted(set(r["currency_pair"] for r in output_rows))
        print(f"[REDIS-OUT] batch={batch_id} pairs={pairs}")
        publish_fx_snapshot_to_redis(output_rows)

    # ── Step 7: Flush DLQ to S3 (mode-isolated DLQ prefix) ────────────
    PROM_DLQ_TOTAL.inc(len(dlq_buffer))
    flush_consumer_dlq_to_s3(dlq_buffer, str(batch_id))

    PROM_LAST_SUCCESS_TIMESTAMP.set(time.time())
    metrics["batch_latency_s"] = round(time.monotonic() - batch_start, 3)
    PROM_DURATION_SECONDS.observe(time.monotonic() - batch_start)


# =============================================================
# S3 REPLAY  (run_mode = "replay")
# =============================================================

def load_s3_replay_partitions(
    start_date_str: str,
    end_date_str:   str,
    pair_groups:    dict = None,
) -> None:
    """
    Read raw FX event parquet files from S3 for the inclusive date range and
    process them through the same pipeline as the Kafka stream.
    Used when run_mode = "replay".

    FX replay uses a date range (REPLAY_START_DATE..REPLAY_END_DATE).
    There is NO hour concept in FX replay.

    Output isolation:
      All output is written to fx/data/replay/ — NEVER to fx/data/live/ or
      fx/data/backfill/. process_batch routes via _output_prefix_for_mode()
      which returns fx/data/replay/ when IS_REPLAY is True.

    DLQ isolation:
      All DLQ records are written to fx/dlq/replay/ — never mixed with live
      or backfill DLQ records.

    Redis:
      NEVER touched during replay. IS_LIVE is False so publish_fx_snapshot_to_redis
      is never called.

    Late event filtering:
      DISABLED for replay. Historical events are always older than
      late_event_max_minutes by wall clock — filtering would drop all events.
      No late events are written to DLQ.

    State rebuild:
      NEVER called during replay. Replay starts with empty pair_buffers.
      State warms naturally from the replay records themselves, in order.

    Source path pattern — selected by replay_path:
        replay_path=backfill    (historical backfill raw):
            s3://risk-platform-pushparag-analytics/kafka_raw/fx/backfill/
                year=Y/month=MM/day=DD/batch_*.parquet
        replay_path=live (historical live raw):
            s3://risk-platform-pushparag-analytics/kafka_raw/fx/live/
                year=Y/month=MM/day=DD/batch_*.parquet

    Each parquet file is treated as one "batch" for consistency.

    Prometheus replay metrics:
        replay_jobs_total              — once per replay job invocation
        replay_records_processed_total — per-record across all partitions
        replay_failures_total          — per failed partition
        replay_duration_seconds        — total replay wall-clock time
    Metrics are pushed at the end of the replay job.
    """
    if not start_date_str or not end_date_str:
        log("ERROR", "REPLAY_START_DATE and REPLAY_END_DATE must both be set.")
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

    # Select the raw S3 source prefix based on replay_path:
    #   replay_path=backfill    -> read historical backfill raw files
    #   replay_path=live -> read historical live raw files
    replay_path_lower = CONFIG["replay_path"].lower()
    if replay_path_lower == "backfill":
        raw_replay_prefix = CONFIG["raw_replay_prefix_backfill"]
    else:
        # replay_path=live — read from historical live raw files
        raw_replay_prefix = CONFIG["raw_replay_prefix_live"]

    # Collect all daily prefixes in the date range
    prefixes = []
    current  = start_dt
    while current <= end_dt:
        prefix = (
            f"{raw_replay_prefix}/"
            f"year={current.year}/month={current.month:02d}/day={current.day:02d}/"
        )
        prefixes.append(prefix)
        current += timedelta(days=1)

    log("INFO", "Starting FX S3 replay",
        {"start_date":        start_date_str,
         "end_date":          end_date_str,
         "date_count":        len(prefixes),
         "replay_path":       replay_path_lower,
         "raw_source_prefix": raw_replay_prefix,
         "output_prefix":     CONFIG["output_prefix_replay"]})

    # Collect all keys across the date range - ONLY latest file per partition
    all_keys = []
    for prefix in prefixes:
        try:
            # Use get_latest_file_per_partition to get ONLY the latest file per day
            latest_files = get_latest_file_per_partition(CONFIG["write_bucket"], prefix)
            all_keys.extend(latest_files)
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

            process_batch(_MockBatch(pdf), batch_num, pair_groups)

        except Exception as exc:
            log("ERROR", "FX S3 replay partition failed",
                {"key": key, "error": str(exc)})
            PROM_REPLAY_FAILURES_TOTAL.inc()

    replay_duration = time.monotonic() - replay_start
    PROM_REPLAY_DURATION_SECONDS.observe(replay_duration)
    _push_metrics()

    send_alert(
        f"FX REPLAY COMPLETE: {start_date_str} to {end_date_str} "
        f"| {len(all_keys)} files | {round(replay_duration, 3)}s"
    )

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
# NOTE: data.timestamp field removed — using ONLY timestamp for market time.
# This eliminates duplicate timestamp confusion.

EVENT_SCHEMA = StructType([
    StructField("producer_run_id",        StringType(), True),
    StructField("batch_id",               StringType(), True),
    StructField("source_fetch_time",      StringType(), True),
    StructField("producer_pipeline_name", StringType(), True),
    StructField("batch_ts",               StringType(), True),

    StructField(
        "bars",
        ArrayType(
            StructType([
                StructField("currency_pair", StringType(), True),
                StructField("open",          FloatType(),  True),
                StructField("high",          FloatType(),  True),
                StructField("low",           FloatType(),  True),
                StructField("close",         FloatType(),  True),
                StructField("timestamp",     StringType(), True),
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
    _consumer_run_id = run_id   # used by _push_metrics for Pushgateway job label

    # Start /metrics HTTP server in live mode only.
    # Backfill and replay push metrics directly; no scrape server needed.
    if IS_LIVE:
        _start_metrics_server()

    # Load FX universe once at startup (read-only)
    fx_universe, pair_groups = load_fx_universe()

    replay_path = CONFIG["replay_path"].lower()
    log("INFO", "FX consumer starting",
        {"replay_path": replay_path,
         "run_id":      run_id,
         "run_mode":    CONFIG["run_mode"]})

    if IS_REPLAY:
        # ── REPLAY MODE ──────────────────────────────────────────────
        # run_mode=replay — reads historical raw S3 parquet files.
        # Source prefix is selected by replay_path inside load_s3_replay_partitions:
        #   replay_path=backfill    -> kafka_raw/fx/backfill/
        #   replay_path=live -> kafka_raw/fx/live/
        # State rebuild is NEVER called in replay mode.
        # pair_buffers start empty; state warms from replay records.
        # Output -> fx/data/replay/
        # DLQ    -> fx/dlq/replay/
        log("INFO", "Starting FX replay — state rebuild skipped (replay mode)",
            {"replay_start": CONFIG["replay_start_date"],
             "replay_end":   CONFIG["replay_end_date"],
             "replay_path":  replay_path})

        load_s3_replay_partitions(
            start_date_str = CONFIG["replay_start_date"],
            end_date_str   = CONFIG["replay_end_date"],
            pair_groups    = pair_groups,
        )
        # Sleep 10 minutes after replay completes before exiting
        log("INFO", "Replay complete - waiting 10 minutes before final exit")
        time.sleep(600)  # 10 minutes = 600 seconds
        log("INFO", "Exiting replay consumer")

    else:
        # ── LIVE / BACKFILL KAFKA STREAM MODE ───────────────────────
        # Live     : fx_ohlc_1m topic, Redis enabled, state rebuild enabled.
        # Backfill : fx_ohlc_1m_backfill topic, Redis disabled,
        #            state rebuild disabled.
        log("INFO", "Starting Kafka stream pipeline",
            {"topic": CONFIG["topic"], "run_mode": CONFIG["run_mode"]})

        # State rebuild — live recovery only.
        # Skipped automatically for: backfill, fresh session starts
        # (within grace window after weekly open), closed market periods.
        # Previous weekly session data is NEVER loaded.
        rebuild_state_from_s3()

        spark = build_spark_session()
        spark.sparkContext.setLogLevel("WARN")

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
                "timestamp as kafka_timestamp",
            )
            .select(
                from_json(col("json_str"), EVENT_SCHEMA).alias("event"),
                "topic",
                "partition",
                "offset",
                "kafka_timestamp",
            )
            .select(
                explode(col("event.bars")).alias("bar"),
                col("event.producer_run_id").alias("producer_run_id"),
                col("event.batch_id").alias("batch_id"),
                col("event.source_fetch_time").alias("source_fetch_time"),
                col("event.producer_pipeline_name").alias("producer_pipeline_name"),
                "topic",
                "partition",
                "offset",
                "kafka_timestamp",
            )
            .select(
                col("bar.currency_pair").alias("currency_pair"),
                col("bar.open").alias("open"),
                col("bar.high").alias("high"),
                col("bar.low").alias("low"),
                col("bar.close").alias("close"),
                col("bar.timestamp").alias("timestamp"),

                "producer_run_id",
                "batch_id",
                "source_fetch_time",
                "producer_pipeline_name",

                "topic",
                "partition",
                "offset",
                "kafka_timestamp",
            )
            .filter(col("close") > 0)
        )



        if IS_LIVE:
            query = (
                parsed_df.writeStream
                .foreachBatch(lambda batch_df, batch_id:
                            process_batch(batch_df, batch_id, pair_groups))
                .outputMode("append")
                .option("checkpointLocation", CONFIG["checkpoint_dir"])
                .start()
            )
            # Market-shutdown loop — FX closes at 17:04 ET on Fridays
            while True:
                now = pendulum.now('America/New_York')
                
                if (
                    now.weekday() == 4  # Friday
                    and now.hour == CONFIG["market_shutdown_hour"]  # 17
                    and now.minute >= CONFIG["market_shutdown_minute"]  # 4
                ):
                    log(
                        "INFO",
                        "Market shutdown time reached - stopping FX consumer cleanly",
                        {
                            "shutdown_time": f"{CONFIG['market_shutdown_hour']}:{CONFIG['market_shutdown_minute']:02d}",
                            "run_mode": CONFIG["run_mode"],
                        }
                    )
                    
                    query.stop()
                    log("INFO", "Spark streaming query stopped")
                    
                    try:
                        _push_metrics()
                        log("INFO", "Final metrics pushed to Pushgateway")
                    except Exception as exc:
                        log("WARNING", "Final metrics push failed", {"error": str(exc)})
                                        
                    # Sleep 2 minutes before exiting - prevents rapid restart loop
                    # Pod will exit at ~17:06, giving time for clean shutdown
                    log("INFO", "Waiting 2 minutes before final exit")
                    time.sleep(120)  # 2 minutes
                    
                    log("INFO", "Exiting FX live consumer")
                    break
                
                time.sleep(30)

        else:
            query = (
                parsed_df.writeStream
                .foreachBatch(lambda batch_df, batch_id:
                            process_batch(batch_df, batch_id, pair_groups))
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
                log("INFO", "Final metrics pushed to Pushgateway")
            except Exception as exc:
                log("WARNING", "Final metrics push failed", {"error": str(exc)})
            
            # Sleep 10 minutes after backfill completes before exiting
            log("INFO", "Backfill complete - waiting 10 minutes before final exit")
            time.sleep(600)  # 10 minutes = 600 seconds
            log("INFO", "Exiting Backfill consumer")