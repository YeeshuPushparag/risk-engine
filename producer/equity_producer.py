"""
equity_producer.py
=========================
Production-grade Kafka producer for real-time equity tick data.
Reads OHLCV snapshots from yfinance and publishes to Kafka.

Storage rules
-------------
READ-ONLY  : s3://yeeshu-equity-bucket   (tickers list only)
ALL WRITES : s3://risk-platform-pushparag-analytics  (raw events, DLQ)

Storage layout (writes)
-----------------------
Raw events (parquet, partitioned):
    s3://risk-platform-pushparag-analytics/kafka_raw/equity/
        year=Y/month=MM/day=DD/hour=HH/batch_<batch_id>.parquet

DLQ (parquet, partitioned by day + hour):
    s3://risk-platform-pushparag-analytics/kafka_dlq/equity/mode/
        year=Y/month=MM/day=DD/hour=HH/dlq_<batch_id>.parquet

Lineage on every event
-----------------------
    producer_run_id   — UUID generated once at process start
    batch_id          — UUID generated per fetch cycle
    source_fetch_time — ET timestamp when yfinance data was fetched
    event_id          — SHA-256(ticker + timestamp) — deterministic, idempotent

Kafka guarantees
----------------
    enable_idempotence = True
    acks               = all
    retries            = 10   (broker-level)
    Application-level exponential backoff on top of broker retries.

yfinance resilience
-------------------
    - Retry on fetch failures (3 attempts with exponential backoff)
    - Track missing/empty/invalid tickers with metrics
    - Send fetch failures to DLQ for auditability
    - Detect empty batch anomalies (possible upstream issues)

Graceful shutdown
-----------------
    SIGTERM / SIGINT -> sets running=False -> main loop exits cleanly ->
    remaining DLQ buffer flushed -> producer.close() called.

Backfill mode
-------------
    BACKFILL_MODE=true
    BACKFILL_DATE=YYYY-MM-DD

    Backfill fetches historical OHLCV for the full date range using yfinance
    start/end params instead of the live "period" param.  Events are written
    to kafka_raw/equity/ exactly as in normal mode AND published to Kafka so
    the consumer processes them through the standard live pipeline
    (equity/data/ output).  event_time is set to the historical bar timestamp
    so event_id (SHA-256) is fully deterministic and idempotent across multiple
    backfill runs for the same date.

Prometheus metrics
------------------
    Exposed on /metrics (port 8000 by default, configurable via METRICS_PORT).
    Prometheus scrapes every 60 s.
    Run-level counters only — no ticker-level labels.

    producer_runs_total              — total fetch/send cycles executed
    producer_failures_total          — cycles that raised an unhandled exception
    producer_dlq_total               — total events routed to DLQ
    producer_kafka_messages_total    — total messages successfully delivered to Kafka
    producer_last_success_timestamp  — Unix timestamp of last fully successful cycle
    producer_duration_seconds        — histogram of per-cycle wall-clock duration
"""

import json
import time
import signal
import uuid
import hashlib
import os
import pendulum
from datetime import datetime, timedelta, date
from io import BytesIO, StringIO
from typing import List, Dict, Tuple, Optional
import requests
import yfinance as yf
import pandas as pd
import boto3
from kafka import KafkaProducer
from kafka.errors import KafkaError
from prometheus_client import (
    Counter, Gauge, Histogram, CollectorRegistry,
    push_to_gateway, start_http_server,
)





# =============================================================
# CONFIG  —  single source of truth.  No magic numbers in code.
# =============================================================

CONFIG: dict = {
    # Kafka
    "kafka_broker":               os.getenv("KAFKA_BROKER", "kafka:9092"),
    "topic_name_live":            "equity_stream",
    "topic_name_backfill":        "equity_stream_backfill",
    "kafka_retries":              10,          # broker-level retries
    "kafka_linger_ms":            10,
    "kafka_acks":                 "all",
    "kafka_request_timeout_ms":   30000,
    "kafka_delivery_timeout_ms":  120000,

    # Application-level send retry (on top of broker retries)
    "max_send_retries":           3,
    "send_timeout_s":             10,          # per future.get()
    "retry_backoff_base_s":       1,           # sleep = base * 2^(attempt-1)

    # yfinance fetch retry
    "fetch_max_retries":          3,
    "fetch_retry_backoff_base_s": 2,           # sleep = base * 2^(attempt-1)

    # Fetch (live mode)
    "fetch_interval_s":           60,
    "fetch_period":               "1d",
    "fetch_interval":             "1m",

    # Run mode
    "run_mode":                   os.getenv("RUN_MODE", "live").lower(),

    # Backfill mode
    "backfill_mode":              os.getenv("BACKFILL_MODE", "false").lower() == "true",
    "backfill_date":              os.getenv("BACKFILL_DATE", ""),
    
    # yfinance interval to use for backfill fetches.
    # 1m data is only available for last 7 days in yfinance; use 5m or 1h for older dates.
    "backfill_interval":          os.getenv("BACKFILL_INTERVAL", "1m"),

    # S3
    "read_bucket":                "yeeshu-equity-bucket",
    "write_bucket":               "risk-platform-pushparag-analytics",
    "ticker_key":                 "historical-equity/tickers50.csv",
    "raw_event_prefix":           "kafka_raw/equity/",
    "dlq_prefix_live":            "kafka_dlq/equity/producer/live/",
    "dlq_prefix_backfill":        "kafka_dlq/equity/producer/backfill",

    # Lineage
    "pipeline_name":              "equity_kafka_producer",
    "data_source":                "yfinance",
    "schema_version":             "v1",
    "transformation":             "equity_tick_v1",

    #Market Hours (3 minute extended)
    "market_shutdown_hour":        16,
    "market_shutdown_minute":       3,


    # Observability
    "log_level":                  "INFO",      # DEBUG | INFO | WARNING | ERROR

    # Prometheus
    # push_to_gateway is used (same pattern as Airflow pipelines).
    # start_http_server also exposes /metrics for direct Prometheus scraping.
    "pushgateway_url":            os.getenv("PUSHGATEWAY_URL"),
    "metrics_port":               int(os.getenv("METRICS_PORT", "8000")),
}


# Set topic name dynamically
CONFIG["topic_name"] = CONFIG["topic_name_backfill"] if CONFIG["backfill_mode"] else CONFIG["topic_name_live"]


# =============================================================
# PROMETHEUS  — run-level counters, no ticker-level labels
# =============================================================

_prom_registry = CollectorRegistry()

PROM_RUNS_TOTAL = Counter(
    "producer_runs_total",
    "Total fetch/send cycles executed",
    registry=_prom_registry,
)
PROM_FAILURES_TOTAL = Counter(
    "producer_failures_total",
    "Cycles that raised an unhandled exception",
    registry=_prom_registry,
)
PROM_DLQ_TOTAL = Counter(
    "producer_dlq_total",
    "Total events routed to DLQ across all cycles",
    registry=_prom_registry,
)
PROM_KAFKA_MESSAGES_TOTAL = Counter(
    "producer_kafka_messages_total",
    "Total messages successfully delivered to Kafka",
    registry=_prom_registry,
)
PROM_LAST_SUCCESS_TIMESTAMP = Gauge(
    "producer_last_success_timestamp",
    "Unix timestamp of the last fully successful producer cycle",
    registry=_prom_registry,
)
PROM_DURATION_SECONDS = Histogram(
    "producer_duration_seconds",
    "Wall-clock duration of each producer cycle in seconds",
    buckets=[0.5, 1, 2, 5, 10, 30, 60, 120, 300],
    registry=_prom_registry,
)

# ========== ADD THESE 3 GAUGES ==========
PROM_MISSING_TICKERS = Gauge(
    "producer_missing_tickers_count",
    "Number of tickers missing from yfinance response in current batch",
    registry=_prom_registry,
)

PROM_EMPTY_TICKERS = Gauge(
    "producer_empty_tickers_count", 
    "Number of tickers with empty data in current batch",
    registry=_prom_registry,
)

PROM_INVALID_TICKERS = Gauge(
    "producer_invalid_tickers_count",
    "Number of tickers with invalid data in current batch", 
    registry=_prom_registry,
)


def _push_metrics(producer_run_id: str) -> None:
    """
    Push accumulated Prometheus metrics to Pushgateway.
    Mirrors the Airflow pattern: push_to_gateway(url, job=<pipeline>_<run_id>, registry=registry).
    Non-fatal — a Pushgateway failure must never stop the producer.
    """
    try:
        job_name = f"{CONFIG['pipeline_name']}_{CONFIG['run_mode']}_{producer_run_id}"
        
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
            f"❌ Metrics Push FAILED | mode={CONFIG['run_mode']} | error={str(exc)}",
            level="CRITICAL"
        )

def _start_metrics_server() -> None:
    """
    Expose /metrics on a background HTTP server for direct Prometheus scraping.
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

def send_alert(message: str, level: str = "CRITICAL"):
    """Send alert to Slack for critical issues only."""
    print(f"[{level}] {message}")

    if level != "CRITICAL":
        return

    webhook = os.getenv("SLACK_WEBHOOK_URL")
    if not webhook:
        return

    try:
        text = f"*[{level}] equity_kafka_producer *\n{message}"
        requests.post(webhook, json={"text": text}, timeout=3)
    except Exception as e:
        print(f"[ALERT ERROR] Slack failed: {e}")

# =============================================================
# STRUCTURED LOGGING
# =============================================================

def log(level: str, message: str, context: dict = None) -> None:
    """
    Emit a structured JSON log line.
    In production wire to CloudWatch, Datadog, or ELK by replacing print.
    """
    if context is None:
        context = {}
    record = {
        "ts":       pendulum.now("America/New_York").to_iso8601_string(),
        "level":    level,
        "pipeline": CONFIG["pipeline_name"],
        "msg":      message,
        **context,
    }
    print(json.dumps(record, default=str))

# =============================================================
# GRACEFUL SHUTDOWN
# =============================================================

running = True

def _stop_signal(signum, frame):
    global running
    log("INFO", "Stop signal received — finishing current cycle then shutting down.",
        {"signal": signum})
    running = False

signal.signal(signal.SIGTERM, _stop_signal)
signal.signal(signal.SIGINT,  _stop_signal)

# =============================================================
# S3 HELPERS
# =============================================================

def get_s3():
    """Return a boto3 S3 client. Called per-operation to avoid stale sessions."""
    return boto3.client("s3")


def atomic_write_parquet_to_s3(df: pd.DataFrame, bucket: str, key: str) -> None:
    """
    Atomically write a DataFrame to S3 as parquet.
    Protocol: PUT to _temp/<key> -> server-side COPY -> DELETE temp.
    The final key is either complete or absent — never partially written.
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


def write_json_to_s3(payload: dict, bucket: str, key: str) -> None:
    """Write a JSON object to S3. Used for metadata/status files."""
    try:
        get_s3().put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(payload, default=str, indent=2).encode("utf-8"),
        )
    except Exception as exc:
        log("ERROR", "JSON S3 write failed",
            {"bucket": bucket, "key": key, "error": str(exc)})

# =============================================================
# LOAD TICKERS  (read-only from equity-bucket)
# =============================================================

def load_tickers() -> list[str]:
    """
    Load ticker list from S3 (read-only bucket).
    Raises on failure — cannot run without a ticker list.
    """
    try:
        obj  = get_s3().get_object(
            Bucket=CONFIG["read_bucket"],
            Key=CONFIG["ticker_key"],
        )
        body = obj["Body"].read().decode("utf-8")
        df   = pd.read_csv(StringIO(body))

        if "ticker" not in df.columns:
            raise ValueError("Tickers CSV missing 'ticker' column.")

        tickers = df["ticker"].dropna().str.strip().str.upper().unique().tolist()
        log("INFO", "Tickers loaded", {"count": len(tickers)})
        return tickers

    except Exception as exc:
        log("ERROR", "Failed to load tickers", {"error": str(exc)})
        raise

# =============================================================
# KAFKA PRODUCER
# =============================================================

def build_kafka_producer() -> KafkaProducer:
    """
    Build a KafkaProducer with idempotence, strong delivery guarantees,
    and serializers matching the existing event schema.
    """
    return KafkaProducer(
        bootstrap_servers                     = CONFIG["kafka_broker"],
        value_serializer                      = lambda v: json.dumps(v, default=str).encode("utf-8"),
        acks                                  = CONFIG["kafka_acks"],
        retries                               = CONFIG["kafka_retries"],
        enable_idempotence                    = True,   # exactly-once delivery guarantee
        max_in_flight_requests_per_connection = 1,      # safe with idempotence (Kafka ≥ 2.1)
        linger_ms                             = CONFIG["kafka_linger_ms"],
        request_timeout_ms                    = CONFIG["kafka_request_timeout_ms"],
        delivery_timeout_ms                   = CONFIG["kafka_delivery_timeout_ms"],
    )

# =============================================================
# EVENT HELPERS
# =============================================================

def generate_event_id(ticker: str, timestamp: str) -> str:
    """
    Deterministic SHA-256 event ID — idempotent across retries.
    Same ticker + timestamp always produces the same event_id.
    """
    return hashlib.sha256(f"{ticker}-{timestamp}".encode()).hexdigest()


def build_event(
    record:            dict,
    producer_run_id:   str,
    batch_id:          str,
    source_fetch_time: str,
    is_backfill: bool = False,
) -> dict:
    """
    Wrap a raw market data record in the standard event envelope.
    Adds full lineage so every downstream consumer can trace provenance.
    """
    event_id = generate_event_id(record["ticker"], record["timestamp"])

    return {
        # Identity
        "event_id":          event_id,
        "event_type":        "equity_tick",
        "schema_version":    CONFIG["schema_version"],

        # Lineage
        "producer_run_id":   producer_run_id,
        "batch_id":          batch_id,
        "source_fetch_time": source_fetch_time,

        "original_source": "backfill" if is_backfill else "live",

        # Pipeline metadata
        "pipeline_name":     CONFIG["pipeline_name"],
        "data_source":       CONFIG["data_source"],
        "transformation":    CONFIG["transformation"],

        # Timing
        "event_time":        record["timestamp"],
        "ingested_at":       pendulum.now("America/New_York").to_iso8601_string(),

        # Payload
        "data":              record,
    }

# =============================================================
# RAW EVENT STORAGE  (parquet, atomic, partitioned by date + hour)
# =============================================================

def store_raw_events_parquet(
    events:          list[dict],
    batch_id:        str,
    producer_run_id: str,
    partition_dt:    Optional[datetime] = None,
    source_type: str = "live",
) -> None:
    """
    Persist raw events to S3 as parquet for replay and audit.

    Path: s3://risk-platform-pushparag-analytics/kafka_raw/equity/source_type
              year=Y/month=MM/day=DD/hour=HH/batch_<batch_id>.parquet

    Uses atomic write (temp -> final) to prevent partial files.

    partition_dt: override the partition timestamp (used in backfill so events
    land in the correct historical partition rather than today's partition).
    When None, defaults to ET now (normal live mode behaviour).
    """
    if not events:
        return

    rows = []
    for ev in events:
        row = {
            "event_id":          ev["event_id"],
            "event_type":        ev["event_type"],
            "schema_version":    ev["schema_version"],
            "producer_run_id":   ev["producer_run_id"],
            "batch_id":          ev["batch_id"],
            "source_fetch_time": ev["source_fetch_time"],
            "pipeline_name":     ev["pipeline_name"],
            "data_source":       ev["data_source"],
            "event_time":        ev["event_time"],
            "ingested_at":       ev["ingested_at"],
            # Flatten data payload
            "ticker":            ev["data"]["ticker"],
            "open":              ev["data"]["open"],
            "high":              ev["data"]["high"],
            "low":               ev["data"]["low"],
            "close":             ev["data"]["close"],
            "volume":            ev["data"]["volume"],
            "timestamp":         ev["data"]["timestamp"],
            "date":              ev["data"]["date"],
        }
        rows.append(row)

    df  = pd.DataFrame(rows)
    # Use the override partition time for backfill; ET now for live mode.
    now = partition_dt if partition_dt is not None else pendulum.now("America/New_York")
    key = (
        f"{CONFIG['raw_event_prefix']}"
        f"{source_type}/"
        f"year={now.year}/"
        f"month={now.month:02d}/"
        f"day={now.day:02d}/"
        f"hour={now.hour:02d}/"
        f"batch_{batch_id}.parquet"
    )

    try:
        atomic_write_parquet_to_s3(df, CONFIG["write_bucket"], key)
    except Exception as exc:
        log("ERROR", "Raw batch S3 write failed — events not persisted for replay",
            {"batch_id": batch_id, "error": str(exc)})
        send_alert(f"[PRODUCER] Raw S3 write FAILED | batch={batch_id} | error={str(exc)}", level="CRITICAL")

# =============================================================
# DLQ  (batch write to S3, not per-event)
# =============================================================

def flush_dlq_buffer(
    dlq_buffer:      list[dict],
    batch_id:        str,
    producer_run_id: str,
    mode:            str,
) -> None:
    """
    Write all failed events from the current batch to S3 DLQ in a single
    atomic write. Structured as a parquet file for queryability.

    Path: s3://risk-platform-pushparag-analytics/kafka_dlq/equity/mode/
              year=Y/month=MM/day=DD/hour=HH/dlq_<batch_id>.parquet

    DLQ record structure per row:
        batch_id, failed_at, event_id, ticker, error, stage,
        producer_run_id, event (JSON string for full fidelity)

    DLQ purpose: audit, investigation, root cause analysis.
    Recovery strategy: fix bug -> replay affected date/hour via REPLAY_MODE=s3.
    Do NOT implement individual ticker replay from DLQ.
    """
    if not dlq_buffer:
        return

    rows = []
    now  = pendulum.now("America/New_York")

    for entry in dlq_buffer:
        ev = entry.get("event", {})
        rows.append({
            "batch_id":        batch_id,
            "producer_run_id": producer_run_id,
            "failed_at":       now.isoformat(),
            "stage":           entry.get("stage", "unknown"),
            "error":           entry.get("error", ""),
            "event_id":        ev.get("event_id", ""),
            "ticker":          ev.get("data", {}).get("ticker", ""),
            "batch_size":      len(dlq_buffer),
            "event_time":      ev.get("event_time", ""),
            "event_json":      json.dumps(ev, default=str),
        })

    df  = pd.DataFrame(rows)
    # DLQ now partitioned by hour (consistent with raw_event_prefix)
    
    if mode == "live":
        key = (
            f"{CONFIG['dlq_prefix_live']}"
            f"year={now.year}/month={now.month:02d}/day={now.day:02d}/"
            f"hour={now.hour:02d}/"
            f"dlq_{batch_id}.parquet"
        )
    else:
        key = (
            f"{CONFIG['dlq_prefix_backfill']}"
            f"year={now.year}/month={now.month:02d}/day={now.day:02d}/"
            f"hour={now.hour:02d}/"
            f"dlq_{batch_id}.parquet"
        )

    try:
        atomic_write_parquet_to_s3(df, CONFIG["write_bucket"], key)
        log("WARNING", "DLQ batch flushed to S3",
            {"batch_id": batch_id, "failed_events": len(rows), "key": key})
    except Exception as exc:
        log("ERROR", "DLQ S3 flush failed — dumping to stdout as fallback",
            {"batch_id": batch_id, "error": str(exc)})
        send_alert(f"[PRODUCER] DLQ S3 write FAILED | batch={batch_id} | error={str(exc)}", level="CRITICAL")
        for entry in dlq_buffer:
            print(json.dumps({"DLQ_FALLBACK": entry}, default=str))

# =============================================================
# FETCH MARKET DATA  —  LIVE MODE  (with retry + observability)
# =============================================================

def fetch_snapshot_with_retry(
    tickers:           list[str],
    producer_run_id:   str,
    batch_id:          str,
    source_fetch_time: str,
    dlq_buffer:        list[dict],
) -> Tuple[List[dict], Dict]:
    """
    Fetch 1-minute OHLCV snapshot from yfinance with retry logic.

    Returns:
        events:        List of valid event dicts ready for Kafka
        fetch_metrics: Dict with tracking for missing/empty/invalid tickers

    Handles:
        - Retry on fetch failure (3 attempts with exponential backoff)
        - Tracks missing tickers (not in response)
        - Tracks empty data (no rows returned)
        - Tracks invalid data (NaN close or zero volume)
        - Sends fetch failures to DLQ for auditability
        - Detects empty batch anomalies
    """
    fetch_metrics = {
        "total_tickers":      len(tickers),
        "missing_tickers":    [],
        "empty_tickers":      [],
        "invalid_tickers":    [],
        "successful_tickers": [],
        "fetch_attempts":     0,
        "fetch_success":      False,
    }

    df = None
    for attempt in range(1, CONFIG["fetch_max_retries"] + 1):
        fetch_metrics["fetch_attempts"] = attempt
        try:
            df = yf.download(
                tickers  = " ".join(tickers),
                period   = CONFIG["fetch_period"],
                interval = CONFIG["fetch_interval"],
                group_by = "ticker",
                threads  = True,
                progress = False,
            )

            if df is not None and not df.empty:
                fetch_metrics["fetch_success"] = True
                break
            else:
                raise ValueError("yfinance returned empty DataFrame")

        except Exception as exc:
            log("WARNING", "yfinance download failed — retrying",
                {"batch_id": batch_id, "attempt": attempt, "error": str(exc)})

            if attempt < CONFIG["fetch_max_retries"]:
                backoff = CONFIG["fetch_retry_backoff_base_s"] * (2 ** (attempt - 1))
                time.sleep(backoff)
            else:
                log("ERROR", "yfinance fetch failed after all retries",
                    {"batch_id": batch_id, "attempts": attempt, "error": str(exc)})
                send_alert(
                    f"[PRODUCER] yfinance fetch FAILED after {CONFIG['fetch_max_retries']} attempts | "
                    f"batch={batch_id} | error={str(exc)}",
                    level="CRITICAL",
                )
                log("WARNING", "Batch fully failed — all tickers sent to DLQ",
                    {"batch_id": batch_id, "total_tickers": len(tickers),
                     "attempts": CONFIG["fetch_max_retries"], "error": str(exc)})

                for ticker in tickers:
                    dlq_buffer.append({
                        "event": {"data": {"ticker": ticker}},
                        "error": (
                            f"yfinance fetch failed after "
                            f"{CONFIG['fetch_max_retries']} attempts: {str(exc)}"
                        ),
                        "stage": "yfinance_fetch",
                    })

                return [], fetch_metrics

    if df is None or df.empty:
        log("ERROR", "No data returned from yfinance after retries",
            {"batch_id": batch_id})
        return [], fetch_metrics

    events = _extract_events_from_df(
        df, tickers, producer_run_id, batch_id, source_fetch_time,
        dlq_buffer, fetch_metrics, exclude_in_progress_bar=True,
    )
    return events, fetch_metrics


# =============================================================
# FETCH MARKET DATA  —  BACKFILL MODE
# =============================================================

def fetch_backfill_for_date(
    tickers:        list[str],
    target_date:    date,
    producer_run_id: str,
    batch_id:        str,
    dlq_buffer:      list[dict],
) -> Tuple[List[dict], Dict]:
    """
    Fetch historical OHLCV for a single calendar date using yfinance start/end params.

    Uses BACKFILL_INTERVAL (default "1m") — note yfinance only provides 1m data
    for the last 7 days; use "5m" or "1h" for older dates.

    event_time is set to the historical bar timestamp, making event_id deterministic
    and idempotent across multiple backfill runs for the same date.

    Returns the same (events, fetch_metrics) tuple as fetch_snapshot_with_retry so
    that the main backfill loop can use identical downstream storage logic.
    """
    fetch_metrics = {
        "total_tickers":      len(tickers),
        "missing_tickers":    [],
        "empty_tickers":      [],
        "invalid_tickers":    [],
        "successful_tickers": [],
        "fetch_attempts":     0,
        "fetch_success":      False,
    }

    # yfinance end date is exclusive — add one day
    start_str = target_date.strftime("%Y-%m-%d")
    end_str   = (target_date + timedelta(days=1)).strftime("%Y-%m-%d")
    source_fetch_time = pendulum.now("America/New_York").to_iso8601_string()

    df = None
    for attempt in range(1, CONFIG["fetch_max_retries"] + 1):
        fetch_metrics["fetch_attempts"] = attempt
        try:
            log("INFO", "Backfill: fetching yfinance data",
                {"date": start_str, "batch_id": batch_id, "attempt": attempt,
                 "interval": CONFIG["backfill_interval"]})

            df = yf.download(
                tickers  = " ".join(tickers),
                start    = start_str,
                end      = end_str,
                interval = CONFIG["backfill_interval"],
                group_by = "ticker",
                threads  = True,
                progress = False,
            )

            if df is not None and not df.empty:
                fetch_metrics["fetch_success"] = True
                log("INFO", "Backfill: yfinance fetch successful",
                    {"date": start_str, "batch_id": batch_id, "attempt": attempt})
                break
            else:
                raise ValueError("yfinance returned empty DataFrame")

        except Exception as exc:
            log("WARNING", "Backfill: yfinance download failed — retrying",
                {"date": start_str, "batch_id": batch_id, "attempt": attempt,
                 "error": str(exc)})
            if attempt < CONFIG["fetch_max_retries"]:
                backoff = CONFIG["fetch_retry_backoff_base_s"] * (2 ** (attempt - 1))
                time.sleep(backoff)
            else:
                log("ERROR", "Backfill: yfinance fetch failed after all retries",
                    {"date": start_str, "batch_id": batch_id, "error": str(exc)})
                send_alert(
                    f"[PRODUCER BACKFILL] yfinance FAILED | date={start_str} | "
                    f"batch={batch_id} | error={str(exc)}",
                    level="CRITICAL",
                )
                for ticker in tickers:
                    dlq_buffer.append({
                        "event": {"data": {"ticker": ticker}},
                        "error": (
                            f"Backfill yfinance fetch failed after "
                            f"{CONFIG['fetch_max_retries']} attempts: {str(exc)}"
                        ),
                        "stage": "backfill_yfinance_fetch",
                    })
                return [], fetch_metrics

    if df is None or df.empty:
        log("ERROR", "Backfill: no data from yfinance",
            {"date": start_str, "batch_id": batch_id})
        return [], fetch_metrics

    # For backfill we include all bars (no in-progress bar exclusion)
    events = _extract_events_from_df(
        df, tickers, producer_run_id, batch_id, source_fetch_time,
        dlq_buffer, fetch_metrics, exclude_in_progress_bar=False,
    )
    return events, fetch_metrics


# =============================================================
# SHARED EVENT EXTRACTION  (live + backfill share this path)
# =============================================================

def _extract_events_from_df(
    df:                       pd.DataFrame,
    tickers:                  list[str],
    producer_run_id:          str,
    batch_id:                 str,
    source_fetch_time:        str,
    dlq_buffer:               list[dict],
    fetch_metrics:            dict,
    exclude_in_progress_bar:  bool = True,
) -> List[dict]:
    """
    Walk the multi-ticker yfinance DataFrame and build event dicts.

    Shared by both live (fetch_snapshot_with_retry) and backfill
    (fetch_backfill_for_date).  The only behavioural difference is
    exclude_in_progress_bar: live mode drops the last bar (still forming);
    backfill keeps all bars (the date range is already closed).

    Each ticker's last valid bar becomes one event.  For backfill, every bar
    in the date becomes a separate event (one batch_id per date per call).
    """
    events: List[dict] = []

    for ticker in tickers:
        try:
            if ticker not in df.columns.get_level_values(0):
                fetch_metrics["missing_tickers"].append(ticker)
                log("DEBUG", "Ticker missing from yfinance response",
                    {"ticker": ticker, "batch_id": batch_id})
                dlq_buffer.append({
                    "event": {"data": {"ticker": ticker}},
                    "error": f"Ticker '{ticker}' not found in yfinance response",
                    "stage": "yfinance_missing_ticker",
                })
                continue

            sub = df[ticker].dropna()

            if sub.empty:
                fetch_metrics["empty_tickers"].append(ticker)
                log("DEBUG", "Ticker returned empty data",
                    {"ticker": ticker, "batch_id": batch_id})
                dlq_buffer.append({
                    "event": {"data": {"ticker": ticker}},
                    "error": f"Ticker '{ticker}' returned empty data from yfinance",
                    "stage": "yfinance_empty_data",
                })
                continue

            if exclude_in_progress_bar:
                sub = sub.iloc[:-1]
                if sub.empty:
                    fetch_metrics["empty_tickers"].append(ticker)
                    continue
                # Live mode: only the most recent completed bar
                bars = [sub.iloc[-1]]
            else:
                # Backfill mode: all bars in the fetched date range
                bars = [sub.iloc[i] for i in range(len(sub))]

            for row in bars:
                ts = row.name

                if pd.isna(row["Close"]) or row["Volume"] <= 0:
                    fetch_metrics["invalid_tickers"].append(ticker)
                    log("DEBUG", "Ticker skipped — invalid close/volume",
                        {"ticker": ticker, "close": row["Close"],
                         "volume": row["Volume"]})
                    dlq_buffer.append({
                        "event": {"data": {"ticker": ticker}},
                        "error": (
                            f"Invalid market data: close={row['Close']}, "
                            f"volume={row['Volume']}"
                        ),
                        "stage": "yfinance_invalid_data",
                    })
                    continue

                record = {
                    "ticker":    ticker,
                    "open":      float(row["Open"]),
                    "high":      float(row["High"]),
                    "low":       float(row["Low"]),
                    "close":     float(row["Close"]),
                    "volume":    float(row["Volume"]),
                    "timestamp": ts.isoformat(),
                    "date":      ts.date().isoformat(),
                }

                event = build_event(record, producer_run_id, batch_id, source_fetch_time, is_backfill=(not exclude_in_progress_bar))
                events.append(event)

            if ticker not in fetch_metrics["missing_tickers"] \
                    and ticker not in fetch_metrics["empty_tickers"] \
                    and ticker not in fetch_metrics["invalid_tickers"]:
                fetch_metrics["successful_tickers"].append(ticker)

        except Exception as exc:
            fetch_metrics["invalid_tickers"].append(ticker)
            log("WARNING", "Ticker processing failed",
                {"ticker": ticker, "batch_id": batch_id, "error": str(exc)})
            dlq_buffer.append({
                "event": {"data": {"ticker": ticker}},
                "error": f"Ticker processing failed: {str(exc)}",
                "stage": "yfinance_processing_error",
            })

    # Log fetch health
    if fetch_metrics["missing_tickers"]:
        log("WARNING", "Tickers missing from yfinance response",
            {"batch_id": batch_id,
             "count": len(fetch_metrics["missing_tickers"]),
             "sample": fetch_metrics["missing_tickers"][:5]})

    if fetch_metrics["empty_tickers"]:
        log("WARNING", "Tickers returned empty data",
            {"batch_id": batch_id,
             "count": len(fetch_metrics["empty_tickers"]),
             "sample": fetch_metrics["empty_tickers"][:5]})

    if fetch_metrics["invalid_tickers"]:
        log("WARNING", "Tickers with invalid data",
            {"batch_id": batch_id,
             "count": len(fetch_metrics["invalid_tickers"]),
             "sample": fetch_metrics["invalid_tickers"][:5]})

    if len(events) == 0 and len(tickers) > 0:
        log("WARNING", "No events produced — possible upstream issue",
            {"batch_id": batch_id,
             "total_tickers": len(tickers),
             "missing": len(fetch_metrics["missing_tickers"]),
             "empty":   len(fetch_metrics["empty_tickers"]),
             "invalid": len(fetch_metrics["invalid_tickers"])})

        if len(fetch_metrics["missing_tickers"]) > len(tickers) * 0.5:
            send_alert(
                f"[PRODUCER] High failure rate in yfinance fetch | "
                f"batch={batch_id} | "
                f"missing={len(fetch_metrics['missing_tickers'])}/{len(tickers)}",
                level="CRITICAL",
            )


    return events

# =============================================================
# SEND WITH EXPONENTIAL BACKOFF RETRY
# =============================================================

def send_with_retry(
    producer:   KafkaProducer,
    event:      dict,
    dlq_buffer: list[dict],
) -> bool:
    """
    Attempt to send a Kafka payload (single event or aggregated batch payload) with exponential backoff retry.

    Uses future.get() to block and surface delivery failures at the
    application level (on top of the broker-level retry configured in the
    KafkaProducer). If all application retries are exhausted, the event is
    added to dlq_buffer for batch S3 flush.

    Returns True on success, False on final failure.
    """


    for attempt in range(1, CONFIG["max_send_retries"] + 1):
        try:
            if "tickers" in event:
                # Combined payload - send without key
                future = producer.send(
                    CONFIG["topic_name"],
                    value=event,
                )
            else:
                # Individual event - send with key
                key_field = event["data"].get("ticker") or event["data"].get("currency_pair")
                future = producer.send(
                    CONFIG["topic_name"],
                    key=key_field,
                    value=event,
                )
            future.get(timeout=CONFIG["send_timeout_s"])
            return True

        except KafkaError as exc:
            backoff = CONFIG["retry_backoff_base_s"] * (2 ** (attempt - 1))
            log("WARNING", "Kafka send failed — retrying",
                {"tickers": len(event.get("tickers", [])),
                 "attempt":    attempt,
                 "max":        CONFIG["max_send_retries"],
                 "backoff_s":  backoff,
                 "error":      str(exc)})
            if attempt < CONFIG["max_send_retries"]:
                time.sleep(backoff)
            else:
                send_alert(
                    f"[PRODUCER] Kafka send FAILED | "
                    f"tickers={len(event.get('tickers', []))}",
                    level="CRITICAL",
                )
                dlq_buffer.append({
                    "event": event,
                    "error": str(exc),
                    "stage": "producer_send",
                })
                return False

        except Exception as exc:
            log(
                "ERROR",
                "Unexpected send error — routing to DLQ",
                {
                    "tickers_count": len(event.get("tickers", [])),
                    "error": repr(exc),
                    "error_type": type(exc).__name__,
                },
            )

            dlq_buffer.append({
                "event": event,
                "error": str(exc),
                "stage": "producer_send_unexpected",
            })

            return False

    return False  # unreachable but explicit

# =============================================================
# BATCH METRICS
# =============================================================

def make_batch_metrics() -> dict:
    return {
        "events_fetched":  0,
        "events_sent":     0,
        "events_failed":   0,
        "missing_tickers": 0,
        "empty_tickers":   0,
        "invalid_tickers": 0,
        "batch_latency_s": 0.0,
    }


def log_backfill_summary(batch_id: str, metrics: dict) -> None:
    log("INFO", "Backfill Summary", {"batch_id": batch_id, **metrics})

# =============================================================
# DATE RANGE HELPERS  (backfill)
# =============================================================

def _parse_date(date_str: str) -> date:
    """Parse YYYY-MM-DD string to date. Raises ValueError on bad input."""
    return datetime.strptime(date_str, "%Y-%m-%d").date()


def _date_range(start: date, end: date) -> List[date]:
    """Return list of dates from start to end inclusive."""
    days = []
    cur  = start
    while cur <= end:
        days.append(cur)
        cur += timedelta(days=1)
    return days

# =============================================================
# LIVE PRODUCER LOOP
# =============================================================

def run_live(producer_run_id: str, tickers: list[str], producer: KafkaProducer) -> None:
    """
    Main live-mode loop. Runs until SIGTERM/SIGINT.

    Each cycle:
      1. Fetch snapshot from yfinance (with retry + observability)
      2. Store raw events to S3 as parquet (for replay / audit)
      3. Send combined batch payload to Kafka
      4. Flush DLQ buffer to S3 in a single batch write
      5. Flush Kafka producer
      6. Push Prometheus metrics
      7. Log batch metrics
      8. Sleep until next cycle
    """
    while running:

        now = pendulum.now('America/New_York')

        if (
            now.hour == CONFIG["market_shutdown_hour"]
            and now.minute >= CONFIG["market_shutdown_minute"]
        ):
            log(
                "INFO",
                "Market shutdown time reached - stopping producer cleanly",
                {
                    "shutdown_time":
                        f"{CONFIG['market_shutdown_hour']}:"
                        f"{CONFIG['market_shutdown_minute']:02d}"
                },
            )

            break

        batch_id = str(uuid.uuid4())
        source_fetch_time = pendulum.now("America/New_York").to_iso8601_string()
        metrics           = make_batch_metrics()
        dlq_buffer:  list[dict] = []
        cycle_start       = time.monotonic()

        try:
            PROM_RUNS_TOTAL.inc()

            # ── Step 1: Fetch ──────────────────────────────────────────
            events, fetch_metrics = fetch_snapshot_with_retry(
                tickers            = tickers,
                producer_run_id    = producer_run_id,
                batch_id           = batch_id,
                source_fetch_time  = source_fetch_time,
                dlq_buffer         = dlq_buffer,
            )

            metrics["events_fetched"]  = len(events)
            metrics["missing_tickers"] = len(fetch_metrics.get("missing_tickers", []))
            metrics["empty_tickers"]   = len(fetch_metrics.get("empty_tickers", []))
            metrics["invalid_tickers"] = len(fetch_metrics.get("invalid_tickers", []))

            PROM_MISSING_TICKERS.set(metrics["missing_tickers"])
            PROM_EMPTY_TICKERS.set(metrics["empty_tickers"])
            PROM_INVALID_TICKERS.set(metrics["invalid_tickers"])

            if not events:
                log("INFO", "No valid events this cycle — sleeping",
                    {"batch_id": batch_id,
                     "missing": metrics["missing_tickers"],
                     "empty":   metrics["empty_tickers"],
                     "invalid": metrics["invalid_tickers"]})
                PROM_DLQ_TOTAL.inc(len(dlq_buffer))
                flush_dlq_buffer(dlq_buffer, batch_id, producer_run_id, mode="live")
                continue

            # ── Step 2: Store raw events ───────────────────────────────
            store_raw_events_parquet(events, batch_id, producer_run_id, source_type="live")

            # ── Step 3: Send combined message to Kafka ─────────────────
            if events:
                # Build combined payload with all tickers
                combined_payload = {
                    "producer_run_id": producer_run_id,
                    "batch_id": batch_id,
                    "source_fetch_time": source_fetch_time,
                    "producer_pipeline_name": CONFIG["pipeline_name"],
                    "batch_ts": pendulum.now("America/New_York").isoformat(),
                    "tickers": []
                }
                
                for event in events:
                    combined_payload["tickers"].append({
                        "ticker": event["data"]["ticker"],
                        "open": event["data"]["open"],
                        "high": event["data"]["high"],
                        "low": event["data"]["low"],
                        "close": event["data"]["close"],
                        "volume": event["data"]["volume"],
                        "timestamp": event["data"]["timestamp"],
                    })
                
                # Send single message
                success = send_with_retry(producer, combined_payload, dlq_buffer)
                if success:
                    metrics["events_sent"] = len(events)
                    PROM_KAFKA_MESSAGES_TOTAL.inc()
                else:
                    metrics["events_failed"] = len(events)

            # ── Step 4: Flush DLQ buffer ───────────────────────────────
            PROM_DLQ_TOTAL.inc(len(dlq_buffer))
            flush_dlq_buffer(dlq_buffer, batch_id, producer_run_id, mode="live")

            # ── Step 5: Flush Kafka producer ───────────────────────────
            try:
                producer.flush()
            except Exception as exc:
                log("ERROR", "Producer flush failed",
                    {"batch_id": batch_id, "error": str(exc)})
                # Do NOT re-DLQ individually sent events — their delivery
                # result was already resolved (success or failure) by
                # send_with_retry's future.get().  A flush() failure means
                # the internal buffer could not be drained, not that
                # previously-confirmed deliveries were lost.  Log a single
                # sentinel record so operators can investigate.
                dlq_buffer.append({
                    "event": {"data": {"ticker": "BATCH_FLUSH_FAILED"},
                              "batch_id": batch_id},
                    "error": f"producer.flush() failed: {str(exc)}",
                    "stage": "producer_flush",
                })
                send_alert(
                    f"[PRODUCER] Kafka flush FAILED | batch={batch_id} | error={str(exc)}",
                    level="CRITICAL",
                )

            PROM_LAST_SUCCESS_TIMESTAMP.set(time.time())

        except Exception as exc:
            log("ERROR", "Unhandled error in producer cycle",
                {"batch_id": batch_id, "error": str(exc)})
            send_alert(
                f"[PRODUCER] CRITICAL FAILURE | batch={batch_id} | error={str(exc)}",
                level="CRITICAL",
            )
            PROM_FAILURES_TOTAL.inc()

        finally:
            cycle_duration = time.monotonic() - cycle_start
            metrics["batch_latency_s"] = round(cycle_duration, 3)
            PROM_DURATION_SECONDS.observe(cycle_duration)
            log("INFO", "Equity batch processed successfully", {"batch_id": batch_id})

 
            time.sleep(CONFIG["fetch_interval_s"])

# =============================================================
# BACKFILL LOOP
# =============================================================

def run_backfill(producer_run_id: str, tickers: list[str], producer: KafkaProducer) -> None:
    """
    Backfill mode: fetch historical yfinance data for a date range, write raw
    events to S3, AND publish historical data to Kafka in hourly batch payloads.

    Architecture requirement (FINAL):
        Historical YFinance -> Producer -> Kafka -> Consumer -> Processed S3

    Both outputs are mandatory in backfill mode:
      1. Raw S3  (kafka_raw/equity/) — partitioned by historical date for replay
      2. Kafka (equity_stream_backfill) — dedicated backfill topic

    The consumer processes backfill events from Kafka exactly as it processes live
    events, writing output to equity/data/.  Replay (REPLAY_MODE=s3) then reads
    kafka_raw/equity/ for historical reprocessing when needed.

    Backfill events are published to Kafka.
    Consumer behavior determines whether Redis is updated.

    Prometheus backfill metrics:
      producer_runs_total             — one increment per date processed
      producer_failures_total         — one increment per date that fails
      Historical YFinance -> Producer -> Kafka -> Consumer -> Processed S3
      producer_dlq_total              — incremented for fetch/validation/send failures
    """

    target_date_str = CONFIG["backfill_date"]

    if not target_date_str:
        raise ValueError(
            "BACKFILL_MODE=true requires BACKFILL_DATE"
        )

    target_date = _parse_date(target_date_str)

    log(
        "INFO",
        "Backfill starting",
        {
            "producer_run_id": producer_run_id,
            "date": target_date_str,
            "interval": CONFIG["backfill_interval"],
            "topic": CONFIG["topic_name"],
        },
    )

    batch_id = str(uuid.uuid4())
    dlq_buffer: list[dict] = []
    metrics = make_batch_metrics()
    cycle_start = time.monotonic()

    try:
        PROM_RUNS_TOTAL.inc()

        events, fetch_metrics = fetch_backfill_for_date(
            tickers=tickers,
            target_date=target_date,
            producer_run_id=producer_run_id,
            batch_id=batch_id,
            dlq_buffer=dlq_buffer,
        )

        metrics["events_fetched"] = len(events)
        metrics["missing_tickers"] = len(fetch_metrics.get("missing_tickers", []))
        metrics["empty_tickers"] = len(fetch_metrics.get("empty_tickers", []))
        metrics["invalid_tickers"] = len(fetch_metrics.get("invalid_tickers", []))

        PROM_MISSING_TICKERS.set(metrics["missing_tickers"])
        PROM_EMPTY_TICKERS.set(metrics["empty_tickers"])
        PROM_INVALID_TICKERS.set(metrics["invalid_tickers"])

        if events:
            events_by_hour = {}

            for event in events:
                event_time = datetime.fromisoformat(event["event_time"])
                hour = event_time.hour
                
                if hour not in events_by_hour:
                    events_by_hour[hour] = []
                events_by_hour[hour].append(event)

            # Store each hour separately
            for hour, hour_events in sorted(events_by_hour.items()):
                partition_dt = datetime(
                    target_date.year,
                    target_date.month,
                    target_date.day,
                    hour=hour,  # Use actual hour instead of 0
                    tzinfo=pendulum.timezone("America/New_York"),
                )
                
                events_by_minute = {}

                for event in hour_events:
                    event_time = datetime.fromisoformat(event["event_time"])

                    minute_key = event_time.strftime("%Y%m%d%H%M")

                    events_by_minute.setdefault(minute_key, []).append(event)

                for minute_key, minute_events in sorted(events_by_minute.items()):

                    minute_dt = datetime.fromisoformat(
                        minute_events[0]["event_time"]
                    )

                    store_raw_events_parquet(
                        minute_events,   # all tickers for ONE minute
                        f"{batch_id}_{minute_key}",
                        producer_run_id,
                        partition_dt=minute_dt,
                        source_type="backfill",
                    )
                
                combined_payload = {
                    "producer_run_id": producer_run_id,
                    "batch_id": f"{batch_id}_h{hour:02d}",
                    "source_fetch_time": hour_events[0]["source_fetch_time"],
                    "producer_pipeline_name": CONFIG["pipeline_name"],
                    "batch_ts": pendulum.now("America/New_York").isoformat(),
                    "tickers": [],
                }

                for event in hour_events:
                    combined_payload["tickers"].append({
                        "ticker": event["data"]["ticker"],
                        "open": event["data"]["open"],
                        "high": event["data"]["high"],
                        "low": event["data"]["low"],
                        "close": event["data"]["close"],
                        "volume": event["data"]["volume"],
                        "timestamp": event["data"]["timestamp"],
                    })

                log(
                    "INFO",
                    "Sending hourly batch",
                    {
                        "hour": hour,
                        "tickers": len(combined_payload["tickers"])
                    }
                )
                success = send_with_retry(
                    producer,
                    combined_payload,
                    dlq_buffer,
                )

                if success:
                    metrics["events_sent"] += len(hour_events)
                    PROM_KAFKA_MESSAGES_TOTAL.inc()
                else:
                    metrics["events_failed"] += len(hour_events)

            try:
                producer.flush()

            except Exception as exc:
                log(
                    "ERROR",
                    "Backfill: producer flush failed",
                    {
                        "date": target_date.isoformat(),
                        "batch_id": batch_id,
                        "error": str(exc),
                    },
                )

                dlq_buffer.append(
                    {
                        "event": {
                            "data": {"ticker": "BATCH_FLUSH_FAILED"},
                            "batch_id": batch_id,
                        },
                        "error": f"producer.flush() failed: {str(exc)}",
                        "stage": "backfill_producer_flush",
                    }
                )

                send_alert(
                    f"[PRODUCER BACKFILL] Kafka flush FAILED | "
                    f"date={target_date} | "
                    f"batch={batch_id} | "
                    f"error={str(exc)}",
                    level="CRITICAL",
                )

            PROM_LAST_SUCCESS_TIMESTAMP.set(time.time())

        else:
            log(
                "WARNING",
                "Backfill: no events for date",
                {
                    "date": target_date.isoformat(),
                    "batch_id": batch_id,
                },
            )

        PROM_DLQ_TOTAL.inc(len(dlq_buffer))

        flush_dlq_buffer(
            dlq_buffer,
            batch_id,
            producer_run_id,
            mode="backfill"
        )

        log_backfill_summary(
            batch_id,
            {
                **metrics,
                "backfill_date": str(target_date),
            },
        )

    except Exception as exc:
        log(
            "ERROR",
            "Backfill: unhandled error",
            {
                "date": target_date.isoformat(),
                "batch_id": batch_id,
                "error": str(exc),
            },
        )

        send_alert(
            f"[PRODUCER BACKFILL] FAILURE | "
            f"date={target_date} | "
            f"batch={batch_id} | "
            f"error={str(exc)}",
            level="CRITICAL",
        )

        PROM_FAILURES_TOTAL.inc()

    finally:
        cycle_duration = time.monotonic() - cycle_start
        PROM_DURATION_SECONDS.observe(cycle_duration)

        # Push ONE final summary to Pushgateway
        _push_metrics(producer_run_id)

    log(
        "INFO",
        "Backfill complete",
        {
            "producer_run_id": producer_run_id,
            "date": target_date_str,
        },
    )

# =============================================================
# ENTRY POINT
# =============================================================

def main():
    """
    Entry point — dispatches to live or backfill mode based on CONFIG.

    Live mode  (default):
        Runs the live producer loop until SIGTERM/SIGINT.
        Flow: YFinance -> Kafka + raw S3

    Backfill mode (BACKFILL_MODE=true):
        Fetches historical yfinance data for BACKFILL_DATE
        publishes historical data to Kafka in hourly batch payloads, then exits.
        Flow: Historical YFinance -> Kafka + raw S3
        Consumer processes backfill events from Kafka identically to live events.
        Replay (REPLAY_MODE=s3) then reads kafka_raw/equity/ for reprocessing.
    """
    if not CONFIG["kafka_broker"]:
        raise ValueError("KAFKA_BROKER env var not set")

    producer_run_id = str(uuid.uuid4())
    is_backfill     = CONFIG["backfill_mode"]

    log("INFO", "Equity producer starting",
        {"producer_run_id": producer_run_id,
         "mode":            "backfill" if is_backfill else "live",
         "broker":          CONFIG["kafka_broker"],
         "topic":           CONFIG["topic_name"]})

    # Start /metrics HTTP server (background thread — non-fatal if it fails)
    if not is_backfill:
        _start_metrics_server()

    tickers = load_tickers()

    # Both live and backfill require a Kafka producer — backfill MUST publish to Kafka.
    producer = build_kafka_producer()
    try:
        if is_backfill:
            log("INFO", "Running in BACKFILL mode — Kafka + S3 output enabled",
                {"date":    CONFIG["backfill_date"],
                 "interval": CONFIG["backfill_interval"],
                 "topic":    CONFIG["topic_name"]})
            run_backfill(producer_run_id, tickers, producer)
        else:
            run_live(producer_run_id, tickers, producer)


        log(
            "INFO",
            "Producer shutting down — closing Kafka connection.",
            {"producer_run_id": producer_run_id},
        )

        producer.close()

        log(
            "INFO",
            "Producer stopped cleanly.",
            {"producer_run_id": producer_run_id},
        )
        # ===== ADD THIS: 10-minute wait for backfill mode =====
        if is_backfill:
            log(
                "INFO",
                "Backfill mode: waiting 10 minutes before exiting",
                {
                    "producer_run_id": producer_run_id,
                    "wait_seconds": 600,
                    "wait_minutes": 10,
                }
            )
            time.sleep(600)  # 10 minutes
            log(
                "INFO",
                "Backfill mode: 10-minute wait complete, exiting now",
                {"producer_run_id": producer_run_id},
            )
        else:
            try:
                _push_metrics(producer_run_id)
            except Exception:
                pass          
            time.sleep(120)  # 2 minutes
            log(
                "INFO",
                "Live mode: 2-minute wait complete, exiting now",
                {"producer_run_id": producer_run_id},
            )

    except Exception as e:  
        log(
            "ERROR",
            "Fatal error in main execution",
            {"producer_run_id": producer_run_id, "error": str(e)}
        )
        raise
    
    finally:  
        try:
            _push_metrics(producer_run_id)
        except Exception:
            pass

if __name__ == "__main__":
    main()