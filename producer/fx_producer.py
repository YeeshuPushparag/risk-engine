"""
fx_producer.py
=====================
Production-grade Kafka producer for real-time FX OHLC data.
Reads 1-minute OHLC snapshots from yfinance and publishes to Kafka.

Architecture mirrors equity_kafka_producer.py exactly, adapted for FX.

Storage rules
-------------
READ-ONLY  : s3://yeeshu-fx-bucket       (universe / positions)
ALL WRITES : s3://risk-platform-pushparag-analytics  (raw events, DLQ)

Storage layout (writes)
-----------------------
Raw events (parquet, partitioned):
    s3://risk-platform-pushparag-analytics/kafka_raw/fx/
        year=Y/month=MM/day=DD/hour=HH/batch_<batch_id>.parquet

DLQ (parquet, partitioned by day):
    s3://risk-platform-pushparag-analytics/kafka_dlq/fx/
        year=Y/month=MM/day=DD/dlq_<batch_id>.parquet

Lineage on every event
-----------------------
    producer_run_id   — UUID generated once at process start
    batch_id          — UUID generated per fetch cycle
    source_fetch_time — ET timestamp when yfinance data was fetched
    event_id          — SHA-256(currency_pair + timestamp) — deterministic

Kafka guarantees
----------------
    enable_idempotence = True
    acks               = all
    retries            = 10   (broker-level)
    Application-level exponential backoff on top of broker retries.

yfinance resilience
-------------------
    - Retry on fetch failures (3 attempts with exponential backoff)
    - Track missing/empty/invalid currency pairs with metrics
    - Send fetch failures to DLQ for auditability
    - Detect empty batch anomalies (possible upstream issues)

Graceful shutdown
-----------------
    SIGTERM / SIGINT -> sets running=False -> main loop exits cleanly ->
    remaining DLQ buffer flushed -> producer.close() called.

Backfill mode
-------------
    FX_BACKFILL_MODE=true
    FX_BACKFILL_START_DATE=YYYY-MM-DD
    FX_BACKFILL_END_DATE=YYYY-MM-DD

    Backfill fetches historical OHLC for the full date range using yfinance
    start/end params instead of the live "period" param. Events are written
    to kafka_raw/fx/ exactly as in normal mode AND published to Kafka so
    the consumer processes them through the standard live pipeline
    (fx/data/ output). event_time is set to the historical bar timestamp
    so event_id (SHA-256) is fully deterministic and idempotent across
    multiple backfill runs for the same date range.

Prometheus metrics
------------------
    Exposed on /metrics (port 8000 by default, configurable via METRICS_PORT).
    Prometheus scrapes every 60 s.
    Run-level counters only — no pair-level labels.

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
from datetime import datetime, timedelta, date
from io import BytesIO, StringIO
from typing import List, Dict, Tuple, Optional
import requests
import yfinance as yf
import pandas as pd
import boto3
import pendulum
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
    "topic_name":                 "fx_ohlc_1m",
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

    # Backfill mode
    "backfill_mode":              os.getenv("FX_BACKFILL_MODE", "false").lower() == "true",
    "backfill_start_date":        os.getenv("FX_BACKFILL_START_DATE", ""),
    "backfill_end_date":          os.getenv("FX_BACKFILL_END_DATE", ""),

    # yfinance interval to use for backfill fetches.
    # 1m data is only available for last 7 days in yfinance; use 5m or 1h for older dates.
    "backfill_interval":          os.getenv("FX_BACKFILL_INTERVAL", "1m"),

    # FX universe — all pairs this producer covers
    "currency_pairs": [
        "USDEUR", "USDJPY", "USDCAD",
        "USDCHF", "USDGBP", "USDAUD", "USDCNY",
    ],

    # S3
    "read_bucket":                "yeeshu-fx-bucket",
    "write_bucket":               "risk-platform-pushparag-analytics",
    "raw_event_prefix":           "kafka_raw/fx/",
    "dlq_prefix":                 "kafka_dlq/fx/",

    # Lineage
    "pipeline_name":              "fx_kafka_producer",
    "data_source":                "yfinance",
    "schema_version":             "v1",
    "transformation":             "fx_tick_v1",
    "event_type":                 "fx_tick",

    # Market Hours — FX shuts down at 17:03
    "market_shutdown_hour":       17,
    "market_shutdown_minute":     3,

    # Observability
    "log_level":                  "INFO",

    # Prometheus
    "pushgateway_url":            os.getenv("PUSHGATEWAY_URL"),
    "metrics_port":               int(os.getenv("METRICS_PORT", "8000")),
}

# =============================================================
# PROMETHEUS  — run-level counters, no pair-level labels
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


def _push_metrics(producer_run_id: str) -> None:
    """
    Push accumulated Prometheus metrics to Pushgateway.
    Mirrors the Airflow pattern: push_to_gateway(url, job=<pipeline>_<run_id>, registry=registry).
    Non-fatal — a Pushgateway failure must never stop the producer.
    """
    try:
        push_to_gateway(
            CONFIG["pushgateway_url"],
            job=f"{CONFIG['pipeline_name']}_{producer_run_id}",
            registry=_prom_registry,
        )
    except Exception as exc:
        log("WARNING", "Prometheus push_to_gateway failed",
            {"error": str(exc)})


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

def send_alert(message: str, level: str = "CRITICAL", context: dict = None) -> None:
    """Send alert to Slack for critical issues only."""
    # Always log
    log(level, f"[ALERT] {message}", context or {})

    # Only send CRITICAL to Slack (prevent noise)
    if level != "CRITICAL":
        return

    webhook = os.getenv("SLACK_WEBHOOK_URL")
    if not webhook:
        return

    try:
        text = f"*[{level}] fx_kafka_producer *\n{message}"
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
    Final key is either complete or absent — never partially written.
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

# =============================================================
# KAFKA PRODUCER
# =============================================================

def build_kafka_producer() -> KafkaProducer:
    """
    Build a KafkaProducer with idempotence, strong delivery guarantees,
    and serializers matching the FX event schema.
    """
    return KafkaProducer(
        bootstrap_servers                     = CONFIG["kafka_broker"],
        key_serializer                        = lambda k: k.encode("utf-8"),
        value_serializer                      = lambda v: json.dumps(v, default=str).encode("utf-8"),
        acks                                  = CONFIG["kafka_acks"],
        retries                               = CONFIG["kafka_retries"],
        enable_idempotence                    = True,   # exactly-once at broker level
        max_in_flight_requests_per_connection = 1,     
        linger_ms                             = CONFIG["kafka_linger_ms"],
        request_timeout_ms                    = CONFIG["kafka_request_timeout_ms"],
        delivery_timeout_ms                   = CONFIG["kafka_delivery_timeout_ms"],
    )

# =============================================================
# EVENT HELPERS
# =============================================================

def generate_event_id(pair: str, timestamp: str) -> str:
    """
    Deterministic SHA-256 event ID — idempotent across retries.
    Same currency_pair + timestamp always produces the same event_id.
    """
    return hashlib.sha256(f"{pair}-{timestamp}".encode()).hexdigest()


def build_event(
    record:            dict,
    producer_run_id:   str,
    batch_id:          str,
    source_fetch_time: str,
    is_backfill: bool = False,
) -> dict:
    """
    Wrap a raw FX OHLC record in the standard event envelope.
    Adds full lineage so every downstream consumer can trace provenance.
    """
    event_id = generate_event_id(record["currency_pair"], record["timestamp"])

    return {
        # Identity
        "event_id":          event_id,
        "event_type":        CONFIG["event_type"],
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

        # FX payload (no volume — FX OTC market has no centralised volume)
        "data": record,
    }

# =============================================================
# RAW EVENT STORAGE  (parquet, atomic, partitioned by date + hour)
# =============================================================

def store_raw_events_parquet(
    events:           list,
    batch_id:         str,
    producer_run_id:  str,
    partition_dt:     Optional[datetime] = None,
    source_type:      str = "live",
) -> None:
    """
    Persist raw FX events to S3 as parquet for replay and audit.

    Path: s3://risk-platform-pushparag-analytics/kafka_raw/fx/source_type
              year=Y/month=MM/day=DD/hour=HH/batch_<batch_id>.parquet

    Partitions by hour (not just date) because FX markets operate 24/5 and
    hour-level partitioning makes replay and Athena queries much more efficient.
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
            # Flatten FX data payload
            "currency_pair":     ev["data"]["currency_pair"],
            "open":              ev["data"]["open"],
            "high":              ev["data"]["high"],
            "low":               ev["data"]["low"],
            "close":             ev["data"]["close"],
            "timestamp":         ev["data"]["timestamp"],
            "date":              ev["data"]["date"],
        }
        rows.append(row)

    df  = pd.DataFrame(rows)
    # Use the override partition time for backfill; 
    now = (
        partition_dt
        if partition_dt is not None
        else pendulum.now("America/New_York")
    )
    

    key = (
        f"{CONFIG['raw_event_prefix']}"
        f"{source_type}/"
        f"year={now.year}/month={now.month:02d}/day={now.day:02d}/"
        f"hour={now.hour:02d}/"
        f"batch_{batch_id}.parquet"
    )

    try:
        atomic_write_parquet_to_s3(df, CONFIG["write_bucket"], key)
    except Exception as exc:
        log("ERROR", "Raw FX batch S3 write failed — events not persisted for replay",
            {"batch_id": batch_id, "error": str(exc)})
        send_alert(
            f"Raw S3 write FAILED | batch={batch_id} | error={str(exc)}",
            level="CRITICAL",
            context={"batch_id": batch_id},
        )

# =============================================================
# DLQ  (batch write to S3, not per-event)
# =============================================================

def flush_dlq_buffer(
    dlq_buffer:      list,
    batch_id:        str,
    producer_run_id: str,
) -> None:
    """
    Write all failed events from the current batch to S3 DLQ in a single
    atomic write. Parquet format for queryability.

    Path: s3://risk-platform-pushparag-analytics/kafka_dlq/fx/
              year=Y/month=MM/day=DD/dlq_<batch_id>.parquet
    """
    if not dlq_buffer:
        return

    rows = []
    now = pendulum.now("America/New_York")

    for entry in dlq_buffer:
        ev = entry.get("event", {})
        rows.append({
            "batch_id":        batch_id,
            "producer_run_id": producer_run_id,
            "failed_at":       now.isoformat(),
            "stage":           entry.get("stage", "unknown"),
            "error":           entry.get("error", ""),
            "event_id":        ev.get("event_id", ""),
            "currency_pair":   ev.get("data", {}).get("currency_pair", ""),
            "batch_size":      len(dlq_buffer),
            "event_time":      ev.get("event_time", ""),
            "event_json":      json.dumps(ev, default=str),
        })

    df  = pd.DataFrame(rows)
    key = (
        f"{CONFIG['dlq_prefix']}"
        f"year={now.year}/month={now.month:02d}/day={now.day:02d}/"
        f"dlq_{batch_id}.parquet"
    )

    try:
        atomic_write_parquet_to_s3(df, CONFIG["write_bucket"], key)
        log("WARNING", "FX DLQ batch flushed to S3",
            {"batch_id": batch_id, "failed_events": len(rows), "key": key})
    except Exception as exc:
        log("ERROR", "FX DLQ S3 flush failed — dumping to stdout as fallback",
            {"batch_id": batch_id, "error": str(exc)})
        send_alert(
            f"DLQ S3 write FAILED | batch={batch_id} | error={str(exc)}",
            level="CRITICAL",
            context={"batch_id": batch_id},
        )
        for entry in dlq_buffer:
            print(json.dumps({"FX_DLQ_FALLBACK": entry}, default=str))

# =============================================================
# SHARED EVENT EXTRACTION  (live + backfill share this path)
# =============================================================

def _extract_events_from_df(
    df:                      pd.DataFrame,
    pairs:                   list,
    producer_run_id:         str,
    batch_id:                str,
    source_fetch_time:       str,
    dlq_buffer:              list,
    fetch_metrics:           dict,
    exclude_in_progress_bar: bool = True,
) -> List[dict]:
    """
    Walk the multi-pair yfinance DataFrame and build event dicts.

    Shared by both live (fetch_fx_snapshot_with_retry) and backfill
    (fetch_backfill_for_date). The only behavioural difference is
    exclude_in_progress_bar: live mode drops the last bar (still forming);
    backfill keeps all bars (the date range is already closed).

    Each pair's last valid bar becomes one event in live mode. For backfill,
    every bar in the date becomes a separate event.
    """
    symbol_map = {pair + "=X": pair for pair in pairs}
    events: List[dict] = []

    for symbol, pair in symbol_map.items():
        try:
            # Check if pair exists in response
            if symbol not in df.columns.get_level_values(0):
                fetch_metrics["missing_pairs"].append(pair)
                log("DEBUG", "FX pair missing from yfinance response",
                    {"pair": pair, "symbol": symbol, "batch_id": batch_id})
                dlq_buffer.append({
                    "event": {"data": {"currency_pair": pair}},
                    "error": f"FX pair '{pair}' not found in yfinance response",
                    "stage": "yfinance_missing_pair",
                })
                continue

            sub = df[symbol].dropna()

            # Check if we have data
            if sub.empty:
                fetch_metrics["empty_pairs"].append(pair)
                log("DEBUG", "FX pair returned empty data",
                    {"pair": pair, "batch_id": batch_id})
                dlq_buffer.append({
                    "event": {"data": {"currency_pair": pair}},
                    "error": f"FX pair '{pair}' returned empty data from yfinance",
                    "stage": "yfinance_empty_data",
                })
                continue

            if exclude_in_progress_bar:
                # Live mode: drop the in-progress bar; use only last completed bar
                sub = sub.iloc[:-1]
                if sub.empty:
                    fetch_metrics["empty_pairs"].append(pair)
                    continue
                bars = [sub.iloc[-1]]
            else:
                # Backfill mode: all bars in the fetched date range
                bars = [sub.iloc[i] for i in range(len(sub))]

            for row in bars:
                ts = row.name

                # FX validation: close must be positive; high >= low
                if pd.isna(row["Close"]) or row["Close"] <= 0:
                    fetch_metrics["invalid_pairs"].append(pair)
                    log("DEBUG", "FX pair skipped — invalid close",
                        {"pair": pair, "close": str(row.get("Close")), "batch_id": batch_id})
                    dlq_buffer.append({
                        "event": {"data": {"currency_pair": pair}},
                        "error": f"Invalid FX data: close={row['Close']}",
                        "stage": "yfinance_invalid_data",
                    })
                    continue

                if row["High"] < row["Low"]:
                    fetch_metrics["invalid_pairs"].append(pair)
                    log("DEBUG", "FX pair skipped — high < low",
                        {"pair": pair, "high": row["High"], "low": row["Low"],
                         "batch_id": batch_id})
                    dlq_buffer.append({
                        "event": {"data": {"currency_pair": pair}},
                        "error": f"Invalid FX data: high={row['High']} < low={row['Low']}",
                        "stage": "yfinance_invalid_data",
                    })
                    continue

                record = {
                    "currency_pair": pair,
                    "open":          float(row["Open"]),
                    "high":          float(row["High"]),
                    "low":           float(row["Low"]),
                    "close":         float(row["Close"]),
                    "timestamp":     ts.isoformat(),
                    "date":          ts.date().isoformat(),
                }

                event = build_event(record, producer_run_id, batch_id, source_fetch_time, is_backfill=(not exclude_in_progress_bar))
                events.append(event)
                print(f"[PRODUCER] {pair} -> {record['close']}")

            if (pair not in fetch_metrics["missing_pairs"]
                    and pair not in fetch_metrics["empty_pairs"]
                    and pair not in fetch_metrics["invalid_pairs"]):
                fetch_metrics["successful_pairs"].append(pair)

        except Exception as exc:
            fetch_metrics["invalid_pairs"].append(pair)
            log("WARNING", "FX pair processing failed",
                {"pair": pair, "batch_id": batch_id, "error": str(exc)})
            dlq_buffer.append({
                "event": {"data": {"currency_pair": pair}},
                "error": f"FX pair processing failed: {str(exc)}",
                "stage": "yfinance_processing_error",
            })

    # Log fetch health
    if fetch_metrics["missing_pairs"]:
        log("WARNING", "FX pairs missing from yfinance response",
            {"batch_id": batch_id,
             "count": len(fetch_metrics["missing_pairs"]),
             "sample": fetch_metrics["missing_pairs"][:5]})

    if fetch_metrics["empty_pairs"]:
        log("WARNING", "FX pairs returned empty data",
            {"batch_id": batch_id,
             "count": len(fetch_metrics["empty_pairs"]),
             "sample": fetch_metrics["empty_pairs"][:5]})

    if fetch_metrics["invalid_pairs"]:
        log("WARNING", "FX pairs with invalid data",
            {"batch_id": batch_id,
             "count": len(fetch_metrics["invalid_pairs"]),
             "sample": fetch_metrics["invalid_pairs"][:5]})

    # Detect empty batch anomaly
    if len(events) == 0 and len(pairs) > 0:
        log("WARNING", "No FX events produced — possible upstream issue",
            {"batch_id": batch_id,
             "total_pairs":  len(pairs),
             "missing":      len(fetch_metrics["missing_pairs"]),
             "empty":        len(fetch_metrics["empty_pairs"]),
             "invalid":      len(fetch_metrics["invalid_pairs"])})

        if len(fetch_metrics["missing_pairs"]) > len(pairs) * 0.5:
            send_alert(
                f"High failure rate in FX yfinance fetch | "
                f"batch={batch_id} | "
                f"missing={len(fetch_metrics['missing_pairs'])}/{len(pairs)}",
                level="CRITICAL",
                context={"batch_id": batch_id},
            )

    return events

# =============================================================
# FETCH FX DATA — LIVE MODE  (with retry + observability)
# =============================================================

def fetch_fx_snapshot_with_retry(
    pairs:             list,
    producer_run_id:   str,
    batch_id:          str,
    source_fetch_time: str,
    dlq_buffer:        list,
) -> Tuple[List[dict], Dict]:
    """
    Fetch 1-minute OHLC snapshot for each FX pair from yfinance with retry.
    Yahoo Finance uses the suffix =X for FX pairs (e.g. USDEUR=X).

    Returns:
        events:        List of valid event dicts ready for Kafka
        fetch_metrics: Dict with tracking for missing/empty/invalid pairs
    """
    symbols = [pair + "=X" for pair in pairs]

    fetch_metrics = {
        "total_pairs":      len(pairs),
        "missing_pairs":    [],
        "empty_pairs":      [],
        "invalid_pairs":    [],
        "successful_pairs": [],
        "fetch_attempts":   0,
        "fetch_success":    False,
    }

    # Retry loop for yfinance download
    df = None
    for attempt in range(1, CONFIG["fetch_max_retries"] + 1):
        fetch_metrics["fetch_attempts"] = attempt
        try:
            df = yf.download(
                tickers  = " ".join(symbols),
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
            log("WARNING", "FX yfinance download failed — retrying",
                {"batch_id": batch_id, "attempt": attempt, "error": str(exc)})

            if attempt < CONFIG["fetch_max_retries"]:
                backoff = CONFIG["fetch_retry_backoff_base_s"] * (2 ** (attempt - 1))
                time.sleep(backoff)
            else:
                log("ERROR", "FX yfinance fetch failed after all retries",
                    {"batch_id": batch_id, "attempts": attempt, "error": str(exc)})
                send_alert(
                    f"FX yfinance fetch FAILED after {CONFIG['fetch_max_retries']} attempts | "
                    f"batch={batch_id} | error={str(exc)}",
                    level="CRITICAL",
                    context={"batch_id": batch_id},
                )
                log("WARNING", "Batch fully failed — all pairs sent to DLQ",
                    {"batch_id": batch_id, "total_pairs": len(pairs),
                     "attempts": CONFIG["fetch_max_retries"], "error": str(exc)})

                for pair in pairs:
                    dlq_buffer.append({
                        "event": {"data": {"currency_pair": pair}},
                        "error": (
                            f"yfinance fetch failed after "
                            f"{CONFIG['fetch_max_retries']} attempts: {str(exc)}"
                        ),
                        "stage": "yfinance_fetch",
                    })

                return [], fetch_metrics

    if df is None or df.empty:
        log("ERROR", "No data returned from FX yfinance after retries",
            {"batch_id": batch_id})
        return [], fetch_metrics

    events = _extract_events_from_df(
        df, pairs, producer_run_id, batch_id, source_fetch_time,
        dlq_buffer, fetch_metrics, exclude_in_progress_bar=True,
    )
    print(
        f"[PRODUCER] batch={batch_id} "
        f"events={len(events)} "
        f"pairs={[e['data']['currency_pair'] for e in events]}"
    )
    return events, fetch_metrics

# =============================================================
# FETCH FX DATA — BACKFILL MODE
# =============================================================

def fetch_backfill_for_date(
    pairs:           list,
    target_date:     date,
    producer_run_id: str,
    batch_id:        str,
    dlq_buffer:      list,
) -> Tuple[List[dict], Dict]:
    """
    Fetch historical OHLC for a single calendar date using yfinance start/end params.

    Uses FX_BACKFILL_INTERVAL (default "1m") — note yfinance only provides 1m data
    for the last 7 days; use "5m" or "1h" for older dates.

    event_time is set to the historical bar timestamp, making event_id deterministic
    and idempotent across multiple backfill runs for the same date.

    Returns the same (events, fetch_metrics) tuple as fetch_fx_snapshot_with_retry so
    that the main backfill loop can use identical downstream storage logic.
    """
    fetch_metrics = {
        "total_pairs":      len(pairs),
        "missing_pairs":    [],
        "empty_pairs":      [],
        "invalid_pairs":    [],
        "successful_pairs": [],
        "fetch_attempts":   0,
        "fetch_success":    False,
    }

    # yfinance end date is exclusive — add one day
    start_str         = target_date.strftime("%Y-%m-%d")
    end_str           = (target_date + timedelta(days=1)).strftime("%Y-%m-%d")
    source_fetch_time = pendulum.now("America/New_York").to_iso8601_string()
    symbols           = [pair + "=X" for pair in pairs]

    df = None
    for attempt in range(1, CONFIG["fetch_max_retries"] + 1):
        fetch_metrics["fetch_attempts"] = attempt
        try:
            log("INFO", "Backfill: fetching FX yfinance data",
                {"date": start_str, "batch_id": batch_id, "attempt": attempt,
                 "interval": CONFIG["backfill_interval"]})

            df = yf.download(
                tickers  = " ".join(symbols),
                start    = start_str,
                end      = end_str,
                interval = CONFIG["backfill_interval"],
                group_by = "ticker",
                threads  = True,
                progress = False,
            )

            if df is not None and not df.empty:
                fetch_metrics["fetch_success"] = True
                log("INFO", "Backfill: FX yfinance fetch successful",
                    {"date": start_str, "batch_id": batch_id, "attempt": attempt})
                break
            else:
                raise ValueError("yfinance returned empty DataFrame")

        except Exception as exc:
            log("WARNING", "Backfill: FX yfinance download failed — retrying",
                {"date": start_str, "batch_id": batch_id, "attempt": attempt,
                 "error": str(exc)})
            if attempt < CONFIG["fetch_max_retries"]:
                backoff = CONFIG["fetch_retry_backoff_base_s"] * (2 ** (attempt - 1))
                time.sleep(backoff)
            else:
                log("ERROR", "Backfill: FX yfinance fetch failed after all retries",
                    {"date": start_str, "batch_id": batch_id, "error": str(exc)})
                send_alert(
                    f"[PRODUCER BACKFILL] FX yfinance FAILED | date={start_str} | "
                    f"batch={batch_id} | error={str(exc)}",
                    level="CRITICAL",
                    context={"batch_id": batch_id},
                )
                for pair in pairs:
                    dlq_buffer.append({
                        "event": {"data": {"currency_pair": pair}},
                        "error": (
                            f"Backfill FX yfinance fetch failed after "
                            f"{CONFIG['fetch_max_retries']} attempts: {str(exc)}"
                        ),
                        "stage": "backfill_yfinance_fetch",
                    })
                return [], fetch_metrics

    if df is None or df.empty:
        log("ERROR", "Backfill: no data from FX yfinance",
            {"date": start_str, "batch_id": batch_id})
        return [], fetch_metrics

    # For backfill we include all bars (no in-progress bar exclusion)
    events = _extract_events_from_df(
        df, pairs, producer_run_id, batch_id, source_fetch_time,
        dlq_buffer, fetch_metrics, exclude_in_progress_bar=False,
    )
    return events, fetch_metrics

# =============================================================
# SEND WITH EXPONENTIAL BACKOFF RETRY
# =============================================================

def send_with_retry(
    producer:    KafkaProducer,
    event:       dict,
    dlq_buffer:  list,
) -> bool:
    """
    Attempt to send a single FX event to Kafka with exponential backoff.

    Uses future.get() to block and surface delivery failures at the
    application level (after broker-level retries have already exhausted).
    On final failure, routes to dlq_buffer for batch S3 flush.

    Returns True on success, False on final failure.
    """
    topic = CONFIG["topic_name"]

    for attempt in range(1, CONFIG["max_send_retries"] + 1):
        try:
            future = producer.send(
                topic,
                key   = event["data"]["currency_pair"],
                value = event,
            )
            future.get(timeout=CONFIG["send_timeout_s"])
            return True

        except KafkaError as exc:
            backoff = CONFIG["retry_backoff_base_s"] * (2 ** (attempt - 1))
            log("WARNING", "Kafka FX send failed — retrying",
                {"pair":      event["data"]["currency_pair"],
                 "attempt":   attempt,
                 "max":       CONFIG["max_send_retries"],
                 "backoff_s": backoff,
                 "error":     str(exc)})
            if attempt < CONFIG["max_send_retries"]:
                time.sleep(backoff)
            else:
                send_alert(
                    f"Kafka send FAILED after retries | "
                    f"pair={event['data']['currency_pair']} | "
                    f"event_id={event['event_id']}",
                    level="CRITICAL",
                    context={"pair": event['data']['currency_pair']},
                )
                dlq_buffer.append({
                    "event": event,
                    "error": str(exc),
                    "stage": "producer_send",
                })
                return False

        except Exception as exc:
            log("ERROR", "Unexpected FX send error — routing to DLQ",
                {"pair": event["data"]["currency_pair"], "error": str(exc)})
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
        "missing_pairs":   0,
        "empty_pairs":     0,
        "invalid_pairs":   0,
        "batch_latency_s": 0.0,
    }


def log_backfill_summary(batch_id: str, metrics: dict) -> None:
    log("INFO", "FX Backfill Summary", {"batch_id": batch_id, **metrics})

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

def run_live(producer_run_id: str, producer: KafkaProducer) -> None:
    """
    Main live-mode loop. Runs until SIGTERM/SIGINT or market shutdown time.

    Each cycle:
      1. Check market shutdown (17:03 FX) — exit cleanly if reached
      2. Fetch snapshot from yfinance (with retry + observability)
      3. Store raw events to S3 as parquet (for replay / audit)
      4. Send each event to Kafka with retry + DLQ buffering
      5. Flush DLQ buffer to S3 in a single batch write
      6. Flush Kafka producer
      7. Push Prometheus metrics
      8. Log batch metrics
      9. Sleep until next cycle
    """
    while running:

        # ── Market shutdown check ──────────────────────────────────────
        now = pendulum.now('America/New_York')
        if (
            now.weekday() == 4
            and now.hour == CONFIG["market_shutdown_hour"]
            and now.minute >= CONFIG["market_shutdown_minute"]
        ):
            log(
                "INFO",
                "Market shutdown time reached - stopping FX producer cleanly",
                {
                    "shutdown_time":
                        f"{CONFIG['market_shutdown_hour']}:"
                        f"{CONFIG['market_shutdown_minute']:02d}"
                },
            )
            break

        batch_id          = str(uuid.uuid4())
        source_fetch_time = pendulum.now("America/New_York").to_iso8601_string()
        metrics           = make_batch_metrics()
        dlq_buffer:  list = []
        cycle_start       = time.monotonic()

        try:
            PROM_RUNS_TOTAL.inc()

            # ── Step 1: Fetch FX snapshot (with retry + observability) ──
            events, fetch_metrics = fetch_fx_snapshot_with_retry(
                pairs              = CONFIG["currency_pairs"],
                producer_run_id    = producer_run_id,
                batch_id           = batch_id,
                source_fetch_time  = source_fetch_time,
                dlq_buffer         = dlq_buffer,
            )

            metrics["events_fetched"] = len(events)
            metrics["missing_pairs"]  = len(fetch_metrics.get("missing_pairs", []))
            metrics["empty_pairs"]    = len(fetch_metrics.get("empty_pairs", []))
            metrics["invalid_pairs"]  = len(fetch_metrics.get("invalid_pairs", []))

            if not events:
                log("INFO", "No valid FX events this cycle — sleeping",
                    {"batch_id": batch_id,
                     "missing":  metrics["missing_pairs"],
                     "empty":    metrics["empty_pairs"],
                     "invalid":  metrics["invalid_pairs"]})
                PROM_DLQ_TOTAL.inc(len(dlq_buffer))
                flush_dlq_buffer(dlq_buffer, batch_id, producer_run_id)
                time.sleep(CONFIG["fetch_interval_s"])
                continue

            # ── Step 2: Store raw events ───────────────────────────────
            store_raw_events_parquet(events, batch_id, producer_run_id, source_type="live")

            # ── Step 3: Send to Kafka with retry ───────────────────────
            for event in events:
                success = send_with_retry(producer, event, dlq_buffer)
                if success:
                    metrics["events_sent"] += 1
                    PROM_KAFKA_MESSAGES_TOTAL.inc()
                else:
                    metrics["events_failed"] += 1

            # ── Step 4: Flush DLQ buffer ───────────────────────────────
            PROM_DLQ_TOTAL.inc(len(dlq_buffer))
            flush_dlq_buffer(dlq_buffer, batch_id, producer_run_id)

            # ── Step 5: Flush Kafka producer ───────────────────────────
            try:
                producer.flush()
            except Exception as exc:
                log("ERROR", "Producer flush failed",
                    {"batch_id": batch_id, "error": str(exc)})
                # Do NOT re-DLQ individually sent events — their delivery
                # result was already resolved by send_with_retry's future.get().
                # Log a single sentinel record so operators can investigate.
                dlq_buffer.append({
                    "event": {"data": {"currency_pair": "BATCH_FLUSH_FAILED"},
                              "batch_id": batch_id},
                    "error": f"producer.flush() failed: {str(exc)}",
                    "stage": "producer_flush",
                })
                send_alert(
                    f"[PRODUCER] Kafka flush FAILED | batch={batch_id} | error={str(exc)}",
                    level="CRITICAL",
                    context={"batch_id": batch_id},
                )

            PROM_LAST_SUCCESS_TIMESTAMP.set(time.time())

        except Exception as exc:
            log("ERROR", "Unhandled error in FX producer cycle",
                {"batch_id": batch_id, "error": str(exc)})
            send_alert(
                f"CRITICAL FAILURE | batch={batch_id} | error={str(exc)}",
                level="CRITICAL",
                context={"batch_id": batch_id},
            )
            PROM_FAILURES_TOTAL.inc()

        finally:
            cycle_duration = time.monotonic() - cycle_start
            metrics["batch_latency_s"] = round(cycle_duration, 3)
            PROM_DURATION_SECONDS.observe(cycle_duration)
            log("INFO", "FX batch processed successfully", {"batch_id": batch_id})

            # Push metrics to Pushgateway at end of every cycle
            _push_metrics(producer_run_id)

        time.sleep(CONFIG["fetch_interval_s"])

# =============================================================
# BACKFILL LOOP
# =============================================================

def run_backfill(producer_run_id: str, producer: KafkaProducer) -> None:
    """
    Backfill mode: fetch historical yfinance data for a date range, write raw
    events to S3, AND publish every event to Kafka.

    Architecture requirement:
        Historical YFinance -> FX Producer -> Kafka -> FX Consumer -> Processed S3

    Both outputs are mandatory in backfill mode:
      1. Raw S3  (kafka_raw/fx/backfill/) — partitioned by historical date/hour for replay
      2. Kafka   (fx_ohlc_1m)   — same topic as live, same schema, same guarantees

    The consumer processes backfill events from Kafka exactly as it processes live
    events, writing output to fx/data/. Replay (REPLAY_MODE=s3) then reads
    kafka_raw/fx/backfill/ for historical reprocessing when needed.

    Key improvements over original:
      - Groups events by actual hour before storing to S3
      - Uses backfill/ partition for clean separation from live data
      - Preserves original batch_id for Kafka lineage (all events share same batch_id)
      - Uses hour-specific batch_id for S3 files to avoid collisions
      - Each hour's events are stored in correct hour=HH partition

    FX backfill spans a date range (FX_BACKFILL_START_DATE to FX_BACKFILL_END_DATE)
    because FX markets operate across multiple calendar days continuously.

    Prometheus backfill metrics:
      producer_runs_total           — one increment per date processed
      producer_failures_total       — one increment per date that fails
      producer_kafka_messages_total — incremented for every event sent to Kafka
      producer_dlq_total            — incremented for fetch/validation/send failures
    """
    start_date_str = CONFIG["backfill_start_date"]
    end_date_str   = CONFIG["backfill_end_date"]

    if not start_date_str or not end_date_str:
        raise ValueError(
            "FX_BACKFILL_MODE=true requires both FX_BACKFILL_START_DATE "
            "and FX_BACKFILL_END_DATE"
        )

    start_date = _parse_date(start_date_str)
    end_date   = _parse_date(end_date_str)
    dates      = _date_range(start_date, end_date)

    log("INFO", "FX backfill starting",
        {"producer_run_id": producer_run_id,
         "start_date":      start_date_str,
         "end_date":        end_date_str,
         "total_dates":     len(dates),
         "interval":        CONFIG["backfill_interval"],
         "topic":           CONFIG["topic_name"]})

    for target_date in dates:
        # Single batch_id for ALL events on this date (used for Kafka lineage)
        batch_id    = str(uuid.uuid4())
        dlq_buffer: list = []
        metrics     = make_batch_metrics()
        cycle_start = time.monotonic()

        try:
            PROM_RUNS_TOTAL.inc()

            # Fetch all events for this date (all hours)
            events, fetch_metrics = fetch_backfill_for_date(
                pairs           = CONFIG["currency_pairs"],
                target_date     = target_date,
                producer_run_id = producer_run_id,
                batch_id        = batch_id,
                dlq_buffer      = dlq_buffer,
            )

            metrics["events_fetched"] = len(events)
            metrics["missing_pairs"]  = len(fetch_metrics.get("missing_pairs", []))
            metrics["empty_pairs"]    = len(fetch_metrics.get("empty_pairs", []))
            metrics["invalid_pairs"]  = len(fetch_metrics.get("invalid_pairs", []))

            if events:
                # =============================================================
                # STEP 1: Group events by their actual hour
                # =============================================================
                # This ensures each hour's data goes to the correct S3 partition
                # (hour=09, hour=10, etc.) instead of all going to hour=00.
                # Without this, replay with REPLAY_HOUR=10 would not work correctly.
                # =============================================================
                events_by_hour: dict[int, list[dict]] = {}
                
                for event in events:
                    # Parse the actual event timestamp to get the hour
                    event_time = datetime.fromisoformat(event["event_time"])
                    hour = event_time.hour
                    
                    if hour not in events_by_hour:
                        events_by_hour[hour] = []
                    events_by_hour[hour].append(event)
                
                log("INFO", "FX backfill: grouped events by hour",
                    {"date": str(target_date),
                     "batch_id": batch_id,
                     "hours": list(events_by_hour.keys()),
                     "events_per_hour": {h: len(evts) for h, evts in events_by_hour.items()}})
                
                # =============================================================
                # STEP 2: Store each hour's events to S3 with correct partition
                # =============================================================
                # Uses backfill/ prefix for clean separation from live data.
                # Uses hour-specific batch_id (batch_id_h{hour}) for S3 file naming
                # to avoid collisions when storing multiple hours.
                # =============================================================
                for hour, hour_events in events_by_hour.items():
                    # Create partition datetime with the actual hour (not midnight)
                    partition_dt = datetime(
                        target_date.year,
                        target_date.month,
                        target_date.day,
                        hour=hour,  # Use actual hour from event timestamp!
                        tzinfo=pendulum.timezone("America/New_York")
                    )
                    
                    # Store raw events with source_type="backfill" and hour-specific batch_id
                    store_raw_events_parquet(
                        hour_events,
                        f"{batch_id}_h{hour:02d}",  # Unique batch_id per hour for S3
                        producer_run_id,
                        partition_dt=partition_dt,
                        source_type="backfill",  # ← Writes to source=backfill/ path
                    )
                    
                    log("INFO", "FX backfill: stored hour partition",
                        {"date": str(target_date),
                         "hour": hour,
                         "events": len(hour_events),
                         "batch_id": f"{batch_id}_h{hour:02d}"})
                
                # =============================================================
                # STEP 3: Send ALL events to Kafka (using original batch_id)
                # =============================================================
                # IMPORTANT: Kafka uses the original batch_id (not hour-specific)
                # so all events from this backfill share the same lineage.
                # This allows the consumer to trace all events from one backfill run.
                # =============================================================
                for event in events:
                    success = send_with_retry(producer, event, dlq_buffer)
                    if success:
                        metrics["events_sent"] += 1
                        PROM_KAFKA_MESSAGES_TOTAL.inc()
                    else:
                        metrics["events_failed"] += 1

                # Flush Kafka producer to ensure all messages are sent
                try:
                    producer.flush()
                except Exception as exc:
                    log("ERROR", "Backfill: FX producer flush failed",
                        {"date": target_date.isoformat(),
                         "batch_id": batch_id, "error": str(exc)})
                    dlq_buffer.append({
                        "event": {"data": {"currency_pair": "BATCH_FLUSH_FAILED"},
                                  "batch_id": batch_id},
                        "error": f"producer.flush() failed: {str(exc)}",
                        "stage": "backfill_producer_flush",
                    })
                    send_alert(
                        f"[PRODUCER BACKFILL] Kafka flush FAILED | "
                        f"date={target_date} | batch={batch_id} | error={str(exc)}",
                        level="CRITICAL",
                        context={"batch_id": batch_id},
                    )

                PROM_LAST_SUCCESS_TIMESTAMP.set(time.time())

            else:
                log("WARNING", "Backfill: no FX events for date",
                    {"date": target_date.isoformat(), "batch_id": batch_id})

            # Flush any failed events to DLQ
            PROM_DLQ_TOTAL.inc(len(dlq_buffer))
            flush_dlq_buffer(dlq_buffer, batch_id, producer_run_id)

            log_backfill_summary(batch_id, {
                **metrics,
                "backfill_date": str(target_date),
            })

        except Exception as exc:
            log("ERROR", "Backfill: unhandled error",
                {"date": target_date.isoformat(),
                 "batch_id": batch_id, "error": str(exc)})
            send_alert(
                f"[PRODUCER BACKFILL] FAILURE | date={target_date} | "
                f"batch={batch_id} | error={str(exc)}",
                level="CRITICAL",
                context={"batch_id": batch_id},
            )
            PROM_FAILURES_TOTAL.inc()

        finally:
            cycle_duration = time.monotonic() - cycle_start
            PROM_DURATION_SECONDS.observe(cycle_duration)

    # Push ONE final summary to Pushgateway when the entire backfill finishes
    _push_metrics(producer_run_id)

    log("INFO", "FX backfill complete",
        {"producer_run_id": producer_run_id,
         "start_date":      start_date_str,
         "end_date":        end_date_str})

# =============================================================
# ENTRY POINT
# =============================================================

def main():
    """
    Entry point — dispatches to live or backfill mode based on CONFIG.

    Live mode  (default):
        Runs the live FX producer loop until SIGTERM/SIGINT or 17:03 market close.
        Flow: YFinance -> Kafka + raw S3

    Backfill mode (FX_BACKFILL_MODE=true):
        Fetches historical yfinance data for FX_BACKFILL_START_DATE..FX_BACKFILL_END_DATE,
        publishes every event to Kafka AND writes raw S3 events, then exits.
        Flow: Historical YFinance -> Kafka + raw S3
        Consumer processes backfill events from Kafka identically to live events.
        Replay (REPLAY_MODE=s3) then reads kafka_raw/fx/ for reprocessing.
    """
    if not CONFIG["kafka_broker"]:
        raise ValueError("KAFKA_BROKER env var not set")

    producer_run_id = str(uuid.uuid4())
    is_backfill     = CONFIG["backfill_mode"]

    log("INFO", "FX producer starting",
        {"producer_run_id": producer_run_id,
         "mode":            "backfill" if is_backfill else "live",
         "broker":          CONFIG["kafka_broker"],
         "topic":           CONFIG["topic_name"],
         "pairs":           CONFIG["currency_pairs"]})

    # Start /metrics HTTP server only in live mode (background thread — non-fatal)
    if not is_backfill:
        _start_metrics_server()

    # Both live and backfill require a Kafka producer — backfill MUST publish to Kafka.
    producer = build_kafka_producer()
    try:
        if is_backfill:
            log("INFO", "Running in BACKFILL mode — Kafka + S3 output enabled",
                {"start_date": CONFIG["backfill_start_date"],
                 "end_date":   CONFIG["backfill_end_date"],
                 "interval":   CONFIG["backfill_interval"],
                 "topic":      CONFIG["topic_name"]})
            run_backfill(producer_run_id, producer)
        else:
            run_live(producer_run_id, producer)
    finally:
        try:
            _push_metrics(producer_run_id)
        except Exception:
            pass

        log("INFO", "FX producer shutting down — closing Kafka connection.",
            {"producer_run_id": producer_run_id})
        producer.close()
        log("INFO", "FX producer stopped cleanly.",
            {"producer_run_id": producer_run_id})


if __name__ == "__main__":
    main()