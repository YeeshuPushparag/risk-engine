"""
equity_kafka_producer.py
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
        year=Y/month=MM/day=DD/batch_<batch_id>.parquet

DLQ (parquet, partitioned by day):
    s3://risk-platform-pushparag-analytics/kafka_dlq/equity/
        year=Y/month=MM/day=DD/dlq_<batch_id>.parquet

Lineage on every event
-----------------------
    producer_run_id   — UUID generated once at process start
    batch_id          — UUID generated per fetch cycle
    source_fetch_time — UTC timestamp when yfinance data was fetched
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
"""

import json
import time
import signal
import uuid
import hashlib
import threading
import os
from datetime import datetime, timezone
from io import BytesIO, StringIO
from typing import List, Dict, Tuple
import requests
import yfinance as yf
import pandas as pd
import boto3
from botocore.exceptions import ClientError
from kafka import KafkaProducer
from kafka.errors import KafkaError

# =============================================================
# CONFIG  —  single source of truth.  No magic numbers in code.
# =============================================================

CONFIG: dict = {
    # Kafka
    "kafka_broker":               os.getenv("KAFKA_BROKER", "kafka:9092"),
    "topic_name":                 "equity_stream",
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

    # Fetch
    "fetch_interval_s":           60,
    "fetch_period":               "1d",
    "fetch_interval":             "1m",

    # S3
    "read_bucket":                "yeeshu-equity-bucket",
    "write_bucket":               "risk-platform-pushparag-analytics",
    "ticker_key":                 "historical-equity/tickers50.csv",
    "raw_event_prefix":           "kafka_raw/equity/",
    "dlq_prefix":                 "kafka_dlq/equity/",

    # Lineage
    "pipeline_name":              "equity_kafka_producer",
    "data_source":                "yfinance",
    "schema_version":             "v1",
    "transformation":             "equity_tick_v1",

    # Observability
    "log_level":                  "INFO",      # DEBUG | INFO | WARNING | ERROR
}


def send_alert(message: str, level: str = "CRITICAL"):
    """Send alert to Slack for critical issues only."""
    # Always log
    print(f"[{level}] {message}")

    # Only send CRITICAL to Slack (prevent noise)
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
        "ts":       datetime.now(timezone.utc).isoformat(),
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
        log("INFO", "S3 atomic write complete",
            {"bucket": bucket, "key": key, "rows": len(df)})
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
        bootstrap_servers                 = CONFIG["kafka_broker"],
        key_serializer                    = lambda k: k.encode("utf-8"),
        value_serializer                  = lambda v: json.dumps(v, default=str).encode("utf-8"),
        acks                              = CONFIG["kafka_acks"],
        retries                           = CONFIG["kafka_retries"],
        enable_idempotence                = True,   # exactly-once delivery guarantee
        max_in_flight_requests_per_connection = 5,  # safe with idempotence (Kafka ≥ 2.1)
        linger_ms                         = CONFIG["kafka_linger_ms"],
        request_timeout_ms                = CONFIG["kafka_request_timeout_ms"],
        delivery_timeout_ms               = CONFIG["kafka_delivery_timeout_ms"],
        compression_type                  = "snappy",
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
    record:           dict,
    producer_run_id:  str,
    batch_id:         str,
    source_fetch_time: str,
) -> dict:
    """
    Wrap a raw market data record in the standard event envelope.
    Adds full lineage so every downstream consumer can trace provenance.
    """
    event_id = generate_event_id(record["ticker"], record["timestamp"])

    return {
        # Identity
        "event_id":         event_id,
        "event_type":       "equity_tick",
        "schema_version":   CONFIG["schema_version"],

        # Lineage (new fields — critical for traceability)
        "producer_run_id":  producer_run_id,
        "batch_id":         batch_id,
        "source_fetch_time": source_fetch_time,

        # Pipeline metadata
        "pipeline_name":    CONFIG["pipeline_name"],
        "data_source":      CONFIG["data_source"],
        "transformation":   CONFIG["transformation"],

        # Timing
        "event_time":       record["timestamp"],
        "ingested_at":      datetime.now(timezone.utc).isoformat(),

        # Payload
        "data":             record,
    }

# =============================================================
# RAW EVENT STORAGE  (parquet, atomic, partitioned by date)
# =============================================================

def store_raw_events_parquet(
    events:          list[dict],
    batch_id:        str,
    producer_run_id: str,
) -> None:
    """
    Persist raw events to S3 as parquet for replay and audit.

    Path: s3://risk-platform-pushparag-analytics/kafka_raw/equity/
              year=Y/month=MM/day=DD/batch_<batch_id>.parquet

    Uses atomic write (temp -> final) to prevent partial files.
    Upgraded from JSON to parquet for columnar efficiency and schema consistency.
    """
    if not events:
        return

    # Flatten nested event structure for columnar storage
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
    now = datetime.now(timezone.utc)
    key = (
        f"{CONFIG['raw_event_prefix']}"
        f"year={now.year}/month={now.month:02d}/day={now.day:02d}/"
        f"batch_{batch_id}.parquet"
    )

    try:
        atomic_write_parquet_to_s3(df, CONFIG["write_bucket"], key)
        log("INFO", "Raw batch stored",
            {"batch_id": batch_id, "events": len(events), "key": key})
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
) -> None:
    """
    Write all failed events from the current batch to S3 DLQ in a single
    atomic write. Structured as a parquet file for queryability.

    Path: s3://risk-platform-pushparag-analytics/kafka_dlq/equity/
              year=Y/month=MM/day=DD/dlq_<batch_id>.parquet

    DLQ record structure per row:
        batch_id, failed_at, event_id, ticker, error, stage,
        producer_run_id, event (JSON string for full fidelity)
    """
    if not dlq_buffer:
        return

    rows = []
    now  = datetime.now(timezone.utc)

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
            "event_json":      json.dumps(ev, default=str),  # full fidelity preserved
        })

    df  = pd.DataFrame(rows)
    key = (
        f"{CONFIG['dlq_prefix']}"
        f"year={now.year}/month={now.month:02d}/day={now.day:02d}/"
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
# FETCH MARKET DATA (WITH RETRY + OBSERVABILITY)
# =============================================================

def fetch_snapshot_with_retry(
    tickers:          list[str],
    producer_run_id:  str,
    batch_id:         str,
    source_fetch_time: str,
    dlq_buffer:       list[dict],
) -> Tuple[List[dict], Dict]:
    """
    Fetch 1-minute OHLCV snapshot from yfinance with retry logic.
    
    Returns:
        events: List of valid event dicts ready for Kafka
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
        "total_tickers": len(tickers),
        "missing_tickers": [],
        "empty_tickers": [],
        "invalid_tickers": [],
        "successful_tickers": [],
        "fetch_attempts": 0,
        "fetch_success": False,
    }
    
    # Retry loop for yfinance download
    df = None
    for attempt in range(1, CONFIG["fetch_max_retries"] + 1):
        fetch_metrics["fetch_attempts"] = attempt
        try:
            log("INFO", "Fetching yfinance data",
                {"batch_id": batch_id, "attempt": attempt, "max_retries": CONFIG["fetch_max_retries"]})
            
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
                log("INFO", "yfinance fetch successful",
                    {"batch_id": batch_id, "attempt": attempt})
                break
            else:
                raise ValueError("yfinance returned empty DataFrame")
                
        except Exception as exc:
            log("WARNING", "yfinance download failed — retrying",
                {"batch_id": batch_id, "attempt": attempt, "error": str(exc)})
            
            if attempt < CONFIG["fetch_max_retries"]:
                backoff = CONFIG["fetch_retry_backoff_base_s"] * (2 ** (attempt - 1))
                log("INFO", "Retrying yfinance fetch",
                    {"batch_id": batch_id, "backoff_s": backoff})
                time.sleep(backoff)
            else:
                # Final failure after all retries
                log("ERROR", "yfinance fetch failed after all retries",
                    {"batch_id": batch_id, "attempts": attempt, "error": str(exc)})
                
                send_alert(
                    f"[PRODUCER] yfinance fetch FAILED after {CONFIG['fetch_max_retries']} attempts | "
                    f"batch={batch_id} | error={str(exc)}",
                    level="CRITICAL"
                )
                
                # Send entire batch to DLQ
                for ticker in tickers:
                    dlq_buffer.append({
                        "event": {"data": {"ticker": ticker}},
                        "error": f"yfinance fetch failed after {CONFIG['fetch_max_retries']} attempts: {str(exc)}",
                        "stage": "yfinance_fetch",
                    })
                
                return [], fetch_metrics
    
    if df is None or df.empty:
        log("ERROR", "No data returned from yfinance after retries",
            {"batch_id": batch_id})
        return [], fetch_metrics
    
    events = []
    
    for ticker in tickers:
        try:
            # Check if ticker exists in response
            if ticker not in df.columns.get_level_values(0):
                fetch_metrics["missing_tickers"].append(ticker)
                log("DEBUG", "Ticker missing from yfinance response",
                    {"ticker": ticker, "batch_id": batch_id})
                
                # Send to DLQ for auditability
                dlq_buffer.append({
                    "event": {"data": {"ticker": ticker}},
                    "error": f"Ticker '{ticker}' not found in yfinance response",
                    "stage": "yfinance_missing_ticker",
                })
                continue
            
            sub = df[ticker].dropna()
            
            # Check if we have data
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
            
            # Exclude in-progress bar (original logic)
            sub = sub.iloc[:-1]
            if sub.empty:
                fetch_metrics["empty_tickers"].append(ticker)
                continue
            
            row = sub.iloc[-1]
            ts  = row.name
            
            # Basic data validation
            if pd.isna(row["Close"]) or row["Volume"] <= 0:
                fetch_metrics["invalid_tickers"].append(ticker)
                log("DEBUG", "Ticker skipped — invalid close/volume",
                    {"ticker": ticker, "close": row["Close"], "volume": row["Volume"]})
                
                dlq_buffer.append({
                    "event": {"data": {"ticker": ticker}},
                    "error": f"Invalid market data: close={row['Close']}, volume={row['Volume']}",
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
            
            event = build_event(record, producer_run_id, batch_id, source_fetch_time)
            events.append(event)
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
    
    # Log fetch metrics
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
    
    # Detect empty batch anomaly
    if len(events) == 0 and len(tickers) > 0:
        log("WARNING", "No events fetched — possible upstream issue",
            {"batch_id": batch_id, 
             "total_tickers": len(tickers),
             "missing": len(fetch_metrics["missing_tickers"]),
             "empty": len(fetch_metrics["empty_tickers"]),
             "invalid": len(fetch_metrics["invalid_tickers"])})
        
        # Send alert for empty batch (potential yfinance degradation)
        if len(fetch_metrics["missing_tickers"]) > len(tickers) * 0.5:
            send_alert(
                f"[PRODUCER] High failure rate in yfinance fetch | "
                f"batch={batch_id} | "
                f"missing={len(fetch_metrics['missing_tickers'])}/{len(tickers)}",
                level="CRITICAL"
            )
    
    log("INFO", "Snapshot fetched",
        {"batch_id": batch_id, 
         "valid_events": len(events),
         "total_tickers": len(tickers),
         "missing": len(fetch_metrics["missing_tickers"]),
         "empty": len(fetch_metrics["empty_tickers"]),
         "invalid": len(fetch_metrics["invalid_tickers"])})
    
    return events, fetch_metrics

# =============================================================
# SEND WITH EXPONENTIAL BACKOFF RETRY
# =============================================================

def send_with_retry(
    producer:    KafkaProducer,
    event:       dict,
    dlq_buffer:  list[dict],
) -> bool:
    """
    Attempt to send a single event to Kafka with exponential backoff retry.

    Uses future.get() to block and surface delivery failures at the
    application level (on top of the broker-level retry configured in the
    KafkaProducer). If all application retries are exhausted, the event is
    added to dlq_buffer for batch S3 flush.

    Returns True on success, False on final failure.
    """
    topic = CONFIG["topic_name"]

    for attempt in range(1, CONFIG["max_send_retries"] + 1):
        try:
            future = producer.send(
                topic,
                key   = event["data"]["ticker"],
                value = event,
            )
            # Block to surface delivery errors at the application level.
            # KafkaProducer's broker-level retries have already run by the
            # time this raises — so this is the final application check.
            future.get(timeout=CONFIG["send_timeout_s"])
            return True

        except KafkaError as exc:
            backoff = CONFIG["retry_backoff_base_s"] * (2 ** (attempt - 1))
            log("WARNING", "Kafka send failed — retrying",
                {"ticker": event["data"]["ticker"],
                 "attempt": attempt,
                 "max": CONFIG["max_send_retries"],
                 "backoff_s": backoff,
                 "error": str(exc)})
            if attempt < CONFIG["max_send_retries"]:
                time.sleep(backoff)
            else:
                send_alert(
                    f"[PRODUCER] Kafka send FAILED | "
                    f"ticker={event['data']['ticker']} | "
                    f"event_id={event['event_id']}",
                    level="CRITICAL"
                )
                dlq_buffer.append({
                    "event": event,
                    "error": str(exc),
                    "stage": "producer_send",
                })
                return False

        except Exception as exc:
            # Non-Kafka exception (serialization, network timeout, etc.)
            log("ERROR", "Unexpected send error — routing to DLQ",
                {"ticker": event["data"]["ticker"],
                 "error": str(exc)})
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
        "events_fetched":    0,
        "events_sent":       0,
        "events_failed":     0,
        "missing_tickers":   0,
        "empty_tickers":     0,
        "invalid_tickers":   0,
        "batch_latency_s":   0.0,
    }


def log_batch_metrics(batch_id: str, metrics: dict) -> None:
    log("INFO", "Batch metrics",
        {"batch_id": batch_id, **metrics})

# =============================================================
# MAIN LOOP
# =============================================================

def main():
    """
    Entry point. Runs the producer loop until SIGTERM/SIGINT.

    Each cycle:
      1. Fetch snapshot from yfinance (with retry + observability)
      2. Store raw events to S3 as parquet (for replay / audit)
      3. Send each event to Kafka with retry + DLQ buffering
      4. Flush DLQ buffer to S3 in a single batch write
      5. Flush Kafka producer
      6. Log batch metrics (including fetch health)
      7. Sleep until next cycle
    """
    if not CONFIG["kafka_broker"]:
        raise ValueError("KAFKA_BROKER not set")

    producer_run_id = str(uuid.uuid4())
    log("INFO", "Equity Kafka producer started",
        {"producer_run_id": producer_run_id,
         "broker": CONFIG["kafka_broker"],
         "topic": CONFIG["topic_name"]})

    tickers  = load_tickers()
    producer = build_kafka_producer()

    while running:
        batch_id         = str(uuid.uuid4())
        source_fetch_time = datetime.now(timezone.utc).isoformat()
        metrics          = make_batch_metrics()
        dlq_buffer:  list[dict] = []
        cycle_start      = time.monotonic()

        try:
            # ── Step 1: Fetch (with retry + observability) ─────────────
            events, fetch_metrics = fetch_snapshot_with_retry(
                tickers           = tickers,
                producer_run_id   = producer_run_id,
                batch_id          = batch_id,
                source_fetch_time = source_fetch_time,
                dlq_buffer        = dlq_buffer,
            )
            
            metrics["events_fetched"] = len(events)
            metrics["missing_tickers"] = len(fetch_metrics.get("missing_tickers", []))
            metrics["empty_tickers"] = len(fetch_metrics.get("empty_tickers", []))
            metrics["invalid_tickers"] = len(fetch_metrics.get("invalid_tickers", []))

            if not events:
                log("INFO", "No valid events this cycle — sleeping",
                    {"batch_id": batch_id,
                     "missing": metrics["missing_tickers"],
                     "empty": metrics["empty_tickers"],
                     "invalid": metrics["invalid_tickers"]})
                
                # Still flush any DLQ entries from fetch failures
                flush_dlq_buffer(dlq_buffer, batch_id, producer_run_id)
                time.sleep(CONFIG["fetch_interval_s"])
                continue

            # ── Step 2: Store raw events (parquet, atomic) ─────────────
            store_raw_events_parquet(events, batch_id, producer_run_id)

            # ── Step 3: Send to Kafka with retry ───────────────────────
            for event in events:
                success = send_with_retry(producer, event, dlq_buffer)
                if success:
                    metrics["events_sent"] += 1
                else:
                    metrics["events_failed"] += 1

            # ── Step 4: Flush DLQ buffer to S3 (single batch write) ────
            flush_dlq_buffer(dlq_buffer, batch_id, producer_run_id)

            # ── Step 5: Flush Kafka producer ───────────────────────────
            try:
                producer.flush()
            except Exception as exc:
                log("ERROR", "Producer flush failed — routing all remaining to DLQ",
                    {"batch_id": batch_id, "error": str(exc)})
                # Events already sent individually; flush failure is unusual.
                # Route all events to DLQ as a precaution.
                flush_dlq_buffer(
                    [{"event": ev, "error": str(exc), "stage": "producer_flush"}
                     for ev in events],
                    f"{batch_id}_flush_fail",
                    producer_run_id,
                )

        except Exception as exc:
            log("ERROR", "Unhandled error in producer cycle",
                {"batch_id": batch_id, "error": str(exc)})

            send_alert(f"[PRODUCER] CRITICAL FAILURE | batch={batch_id} | error={str(exc)}", level="CRITICAL")

        finally:
            # ── Step 6: Log metrics regardless of success/failure ──────
            metrics["batch_latency_s"] = round(time.monotonic() - cycle_start, 3)
            log_batch_metrics(batch_id, metrics)

            # ── Step 7: Sleep until next cycle ─────────────────────────
            log("INFO", "Producer heartbeat", {
                "producer_run_id": producer_run_id,
                "batch_id": batch_id,
                "status": "alive"
            })

            time.sleep(CONFIG["fetch_interval_s"])

    # ── Graceful shutdown ─────────────────────────────────────────────
    log("INFO", "Producer shutting down — closing Kafka connection.",
        {"producer_run_id": producer_run_id})

    producer.close()

    log("INFO", "Producer stopped cleanly.", {
        "producer_run_id": producer_run_id
    })

if __name__ == "__main__":
    main()