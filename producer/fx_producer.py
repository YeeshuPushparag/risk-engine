"""
fx_kafka_producer.py
=====================
Production-grade Kafka producer for real-time FX OHLC data.
Reads 1-minute OHLC snapshots from yfinance and publishes to Kafka.

Architecture mirrors equity_kafka_producer.py exactly.

Storage rules
-------------
READ-ONLY  : s3://pushparag-fx-bucket       (universe / positions)
ALL WRITES : s3://pushparag-risk-analytics  (raw events, DLQ)

Storage layout (writes)
-----------------------
Raw events (parquet, partitioned):
    s3://pushparag-risk-analytics/kafka_raw/fx/
        year=Y/month=MM/day=DD/hour=HH/batch_<batch_id>.parquet

DLQ (parquet, partitioned by day):
    s3://pushparag-risk-analytics/kafka_dlq/fx/
        year=Y/month=MM/day=DD/dlq_<batch_id>.parquet

Lineage on every event
-----------------------
    producer_run_id   — UUID generated once at process start
    batch_id          — UUID generated per fetch cycle
    source_fetch_time — UTC timestamp when yfinance data was fetched
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
"""

import json
import time
import signal
import uuid
import hashlib
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
    "kafka_broker":                   os.getenv("KAFKA_BROKER", "kafka:9092"),
    "topic_name":                     "fx_ohlc_1m",
    "kafka_retries":                  10,         # broker-level retries
    "kafka_linger_ms":                10,
    "kafka_acks":                     "all",
    "kafka_request_timeout_ms":       30000,
    "kafka_delivery_timeout_ms":      120000,

    # Application-level send retry (on top of broker retries)
    "max_send_retries":               3,
    "send_timeout_s":                 10,         # per future.get()
    "retry_backoff_base_s":           1,          # sleep = base * 2^(attempt-1)

    # yfinance fetch retry
    "fetch_max_retries":              3,
    "fetch_retry_backoff_base_s":     2,          # sleep = base * 2^(attempt-1)

    # Fetch
    "fetch_interval_s":               60,
    "fetch_period":                   "1d",
    "fetch_interval":                 "1m",

    # FX universe — all pairs this producer covers
    "currency_pairs": [
        "USDEUR", "USDJPY", "USDCAD",
        "USDCHF", "USDGBP", "USDAUD", "USDCNY",
    ],

    # S3
    "read_bucket":                    "pushparag-fx-bucket",
    "write_bucket":                   "pushparag-risk-analytics",
    "raw_event_prefix":               "kafka_raw/fx/",
    "dlq_prefix":                     "kafka_dlq/fx/",

    # Lineage
    "pipeline_name":                  "fx_kafka_producer",
    "data_source":                    "yfinance",
    "schema_version":                 "v1",
    "transformation":                 "fx_tick_v1",
    "event_type":                     "fx_tick",

    # Observability
    "log_level":                      "INFO",
}

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
        "ts":       datetime.now(timezone.utc).isoformat(),
        "level":    level,
        "pipeline": CONFIG["pipeline_name"],
        "msg":      message,
        **context,
    }
    print(json.dumps(record, default=str))

# =============================================================
# ALERTING
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
        max_in_flight_requests_per_connection = 5,      # safe with idempotence (Kafka ≥ 2.1)
        linger_ms                             = CONFIG["kafka_linger_ms"],
        request_timeout_ms                    = CONFIG["kafka_request_timeout_ms"],
        delivery_timeout_ms                   = CONFIG["kafka_delivery_timeout_ms"],
        compression_type                      = "snappy",
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

        # Pipeline metadata
        "pipeline_name":     CONFIG["pipeline_name"],
        "data_source":       CONFIG["data_source"],
        "transformation":    CONFIG["transformation"],

        # Timing
        "event_time":        record["timestamp"],
        "ingested_at":       datetime.now(timezone.utc).isoformat(),

        # FX payload (no volume — FX OTC market has no centralised volume)
        "data": record,
    }

# =============================================================
# RAW EVENT STORAGE  (parquet, atomic, partitioned by date + hour)
# =============================================================

def store_raw_events_parquet(
    events:           list[dict],
    batch_id:         str,
    producer_run_id:  str,
) -> None:
    """
    Persist raw FX events to S3 as parquet for replay and audit.

    Path: s3://pushparag-risk-analytics/kafka_raw/fx/
              year=Y/month=MM/day=DD/hour=HH/batch_<batch_id>.parquet

    Partitions by hour (not just date) because FX markets operate 24/5 and
    hour-level partitioning makes replay and Athena queries much more efficient.
    Uses atomic write (temp -> final) to prevent partial files.
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
    now = datetime.now(timezone.utc)
    key = (
        f"{CONFIG['raw_event_prefix']}"
        f"year={now.year}/month={now.month:02d}/day={now.day:02d}/"
        f"hour={now.hour:02d}/"
        f"batch_{batch_id}.parquet"
    )

    try:
        atomic_write_parquet_to_s3(df, CONFIG["write_bucket"], key)
        log("INFO", "Raw FX batch stored",
            {"batch_id": batch_id, "events": len(events), "key": key})
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
    dlq_buffer:      list[dict],
    batch_id:        str,
    producer_run_id: str,
) -> None:
    """
    Write all failed events from the current batch to S3 DLQ in a single
    atomic write. Parquet format for queryability.

    Path: s3://pushparag-risk-analytics/kafka_dlq/fx/
              year=Y/month=MM/day=DD/dlq_<batch_id>.parquet
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
# FETCH FX DATA (WITH RETRY + OBSERVABILITY)
# =============================================================

def fetch_fx_snapshot_with_retry(
    pairs:             list[str],
    producer_run_id:   str,
    batch_id:          str,
    source_fetch_time: str,
    dlq_buffer:        list[dict],
) -> Tuple[List[dict], Dict]:
    """
    Fetch 1-minute OHLC snapshot for each FX pair from yfinance with retry.
    Yahoo Finance uses the suffix =X for FX pairs (e.g. USDEUR=X).

    Returns:
        events: List of valid event dicts ready for Kafka
        fetch_metrics: Dict with tracking for missing/empty/invalid pairs

    Handles:
        - Retry on fetch failure (3 attempts with exponential backoff)
        - Tracks missing pairs (not in response)
        - Tracks empty data (no rows returned)
        - Tracks invalid data (NaN close or high < low)
        - Sends fetch failures to DLQ for auditability
        - Detects empty batch anomalies
    """
    
    symbols    = [pair + "=X" for pair in pairs]
    symbol_map = {pair + "=X": pair for pair in pairs}
    
    fetch_metrics = {
        "total_pairs": len(pairs),
        "missing_pairs": [],
        "empty_pairs": [],
        "invalid_pairs": [],
        "successful_pairs": [],
        "fetch_attempts": 0,
        "fetch_success": False,
    }
    
    # Retry loop for yfinance download
    df = None
    for attempt in range(1, CONFIG["fetch_max_retries"] + 1):
        fetch_metrics["fetch_attempts"] = attempt
        try:
            log("INFO", "Fetching FX yfinance data",
                {"batch_id": batch_id, "attempt": attempt, "max_retries": CONFIG["fetch_max_retries"]})
            
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
                log("INFO", "FX yfinance fetch successful",
                    {"batch_id": batch_id, "attempt": attempt})
                break
            else:
                raise ValueError("yfinance returned empty DataFrame")
                
        except Exception as exc:
            log("WARNING", "FX yfinance download failed — retrying",
                {"batch_id": batch_id, "attempt": attempt, "error": str(exc)})
            
            if attempt < CONFIG["fetch_max_retries"]:
                backoff = CONFIG["fetch_retry_backoff_base_s"] * (2 ** (attempt - 1))
                log("INFO", "Retrying FX yfinance fetch",
                    {"batch_id": batch_id, "backoff_s": backoff})
                time.sleep(backoff)
            else:
                # Final failure after all retries
                log("ERROR", "FX yfinance fetch failed after all retries",
                    {"batch_id": batch_id, "attempts": attempt, "error": str(exc)})
                
                send_alert(
                    f"FX yfinance fetch FAILED after {CONFIG['fetch_max_retries']} attempts | "
                    f"batch={batch_id} | error={str(exc)}",
                    level="CRITICAL",
                    context={"batch_id": batch_id}
                )
                
                # Send entire batch to DLQ
                for pair in pairs:
                    dlq_buffer.append({
                        "event": {"data": {"currency_pair": pair}},
                        "error": f"yfinance fetch failed after {CONFIG['fetch_max_retries']} attempts: {str(exc)}",
                        "stage": "yfinance_fetch",
                    })
                
                return [], fetch_metrics
    
    if df is None or df.empty:
        log("ERROR", "No data returned from FX yfinance after retries",
            {"batch_id": batch_id})
        return [], fetch_metrics
    
    events = []
    
    for symbol, pair in symbol_map.items():
        try:
            # Check if pair exists in response
            if symbol not in df.columns.get_level_values(0):
                fetch_metrics["missing_pairs"].append(pair)
                log("DEBUG", "FX pair missing from yfinance response",
                    {"pair": pair, "symbol": symbol, "batch_id": batch_id})
                
                # Send to DLQ for auditability
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
            
            # Exclude in-progress bar
            sub = sub.iloc[:-1]
            if sub.empty:
                fetch_metrics["empty_pairs"].append(pair)
                continue
            
            row = sub.iloc[-1]
            ts  = row.name
            
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
                    {"pair": pair, "high": row["High"], "low": row["Low"], "batch_id": batch_id})
                
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
            
            event = build_event(record, producer_run_id, batch_id, source_fetch_time)
            events.append(event)
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
    
    # Log fetch metrics
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
        log("WARNING", "No FX events fetched — possible upstream issue",
            {"batch_id": batch_id, 
             "total_pairs": len(pairs),
             "missing": len(fetch_metrics["missing_pairs"]),
             "empty": len(fetch_metrics["empty_pairs"]),
             "invalid": len(fetch_metrics["invalid_pairs"])})
        
        # Send alert for empty batch (potential yfinance degradation)
        if len(fetch_metrics["missing_pairs"]) > len(pairs) * 0.5:
            send_alert(
                f"High failure rate in FX yfinance fetch | "
                f"batch={batch_id} | "
                f"missing={len(fetch_metrics['missing_pairs'])}/{len(pairs)}",
                level="CRITICAL",
                context={"batch_id": batch_id}
            )
    
    log("INFO", "FX snapshot fetched",
        {"batch_id": batch_id,
         "valid_events": len(events),
         "total_pairs": len(pairs),
         "missing": len(fetch_metrics["missing_pairs"]),
         "empty": len(fetch_metrics["empty_pairs"]),
         "invalid": len(fetch_metrics["invalid_pairs"])})
    
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
                {"pair":     event["data"]["currency_pair"],
                 "attempt":  attempt,
                 "max":      CONFIG["max_send_retries"],
                 "backoff_s": backoff,
                 "error":    str(exc)})
            if attempt < CONFIG["max_send_retries"]:
                time.sleep(backoff)
            else:
                send_alert(
                    f"Kafka send FAILED after retries | "
                    f"pair={event['data']['currency_pair']} | "
                    f"event_id={event['event_id']}",
                    level="CRITICAL",
                    context={"pair": event['data']['currency_pair']}
                )
                dlq_buffer.append({
                    "event": event,
                    "error": str(exc),
                    "stage": "producer_send",
                })
                return False

        except Exception as exc:
            log("ERROR", "Unexpected FX send error — routing to DLQ",
                {"pair":  event["data"]["currency_pair"], "error": str(exc)})
            dlq_buffer.append({
                "event": event,
                "error": str(exc),
                "stage": "producer_send_unexpected",
            })
            return False

    return False

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


def log_batch_metrics(batch_id: str, metrics: dict) -> None:
    log("INFO", "FX batch metrics", {"batch_id": batch_id, **metrics})

# =============================================================
# MAIN LOOP
# =============================================================

def main():
    """
    Entry point. Runs the FX producer loop until SIGTERM/SIGINT.

    Each cycle:
      1. Fetch snapshot from yfinance for all FX pairs (with retry + observability)
      2. Store raw events to S3 as parquet (partitioned by date + hour)
      3. Send each event to Kafka with exponential backoff retry + DLQ buffering
      4. Flush DLQ buffer to S3 in a single batch write
      5. Flush Kafka producer (ensure all buffered messages are delivered)
      6. Log batch metrics (including fetch health)
      7. Sleep until next cycle
    """
    if not CONFIG["kafka_broker"]:
        raise ValueError("KAFKA_BROKER environment variable not set.")

    producer_run_id = str(uuid.uuid4())
    log("INFO", "FX Kafka producer started",
        {"producer_run_id": producer_run_id,
         "broker":          CONFIG["kafka_broker"],
         "topic":           CONFIG["topic_name"],
         "pairs":           CONFIG["currency_pairs"]})

    producer = build_kafka_producer()

    while running:
        batch_id          = str(uuid.uuid4())
        source_fetch_time = datetime.now(timezone.utc).isoformat()
        metrics           = make_batch_metrics()
        dlq_buffer:  list[dict] = []
        cycle_start       = time.monotonic()

        try:
            # ── Step 1: Fetch FX snapshot (with retry + observability) ──
            events, fetch_metrics = fetch_fx_snapshot_with_retry(
                pairs              = CONFIG["currency_pairs"],
                producer_run_id    = producer_run_id,
                batch_id           = batch_id,
                source_fetch_time  = source_fetch_time,
                dlq_buffer         = dlq_buffer,
            )
            
            metrics["events_fetched"] = len(events)
            metrics["missing_pairs"] = len(fetch_metrics.get("missing_pairs", []))
            metrics["empty_pairs"] = len(fetch_metrics.get("empty_pairs", []))
            metrics["invalid_pairs"] = len(fetch_metrics.get("invalid_pairs", []))

            if not events:
                log("INFO", "No valid FX events this cycle — sleeping",
                    {"batch_id": batch_id,
                     "missing": metrics["missing_pairs"],
                     "empty": metrics["empty_pairs"],
                     "invalid": metrics["invalid_pairs"]})
                
                # Still flush any DLQ entries from fetch failures
                flush_dlq_buffer(dlq_buffer, batch_id, producer_run_id)
                time.sleep(CONFIG["fetch_interval_s"])
                continue

            # ── Step 2: Store raw events (parquet, atomic, date+hour) ──
            store_raw_events_parquet(events, batch_id, producer_run_id)

            # ── Step 3: Send to Kafka with retry ───────────────────────
            for event in events:
                success = send_with_retry(producer, event, dlq_buffer)
                if success:
                    metrics["events_sent"] += 1
                else:
                    metrics["events_failed"] += 1

            # ── Step 4: Flush DLQ buffer (single S3 batch write) ───────
            flush_dlq_buffer(dlq_buffer, batch_id, producer_run_id)

            # ── Step 5: Flush Kafka producer ───────────────────────────
            try:
                producer.flush()
            except Exception as exc:
                log("ERROR", "Producer flush failed — routing all events to DLQ",
                    {"batch_id": batch_id, "error": str(exc)})
                flush_dlq_buffer(
                    [{"event": ev, "error": str(exc), "stage": "producer_flush"}
                     for ev in events],
                    f"{batch_id}_flush_fail",
                    producer_run_id,
                )

        except Exception as exc:
            log("ERROR", "Unhandled error in FX producer cycle",
                {"batch_id": batch_id, "error": str(exc)})
            send_alert(
                f"CRITICAL FAILURE | batch={batch_id} | error={str(exc)}",
                level="CRITICAL",
                context={"batch_id": batch_id},
            )

        finally:
            # ── Step 6: Log metrics regardless of outcome ──────────────
            metrics["batch_latency_s"] = round(time.monotonic() - cycle_start, 3)
            log_batch_metrics(batch_id, metrics)

            log("INFO", "FX producer heartbeat",
                {"producer_run_id": producer_run_id,
                 "batch_id":        batch_id,
                 "status":          "alive"})

        # ── Step 7: Sleep until next cycle ─────────────────────────────
        time.sleep(CONFIG["fetch_interval_s"])

    # ── Graceful shutdown ─────────────────────────────────────────────
    log("INFO", "FX producer shutting down — closing Kafka connection.",
        {"producer_run_id": producer_run_id})
    producer.close()
    log("INFO", "FX producer stopped cleanly.",
        {"producer_run_id": producer_run_id})


if __name__ == "__main__":
    main()