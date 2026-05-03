"""
equity_kafka_consumer.py
=========================
Production-grade Spark Structured Streaming consumer for real-time equity ticks.

Storage rules
-------------
READ-ONLY  : s3://yeeshu-equity-bucket   (static positions)
ALL WRITES : s3://risk-platform-pushparag-analytics  (parquet output, DLQ, snapshots)

Storage layout (writes)
-----------------------
Processed output (parquet, partitioned):
    s3://risk-platform-pushparag-analytics/equity/data/
        date=YYYY-MM-DD/hour=HH/batch_<batch_id>.parquet

Consumer DLQ (S3, parquet, partitioned by day):
    s3://risk-platform-pushparag-analytics/kafka_dlq/consumer/
        year=Y/month=MM/day=DD/dlq_<batch_id>.parquet

Consumer DLQ (Kafka topic, for stream-level routing):
    topic: equity_stream_dlq

Raw Kafka storage (for replay + state rebuild):
    s3://risk-platform-pushparag-analytics/kafka_raw/equity/
        year=YYYY/month=MM/day=DD/hour=HH/batch_<batch_id>.parquet

Replay modes
------------
REPLAY_MODE = "kafka"  -> normal Spark Structured Streaming from Kafka
REPLAY_MODE = "s3"     -> reads raw parquet from kafka_raw/equity/ directly,
                         processes through the same pipeline, writes output.
                         Useful for reprocessing without re-consuming from Kafka.

State Management
----------------
On startup: rebuilds ticker_buffers from last 15 minutes of raw S3 data
During runtime: maintains rolling buffers in memory per ticker
On restart: state is rehydrated from S3 (not lost)

Late Event Handling
-------------------
Events older than 5 minutes from current time are skipped.
Prevents stale data from corrupting rolling metrics.

Lineage on every output row
----------------------------
    event_id               — from producer (SHA-256, idempotent)
    kafka_offset           — exact offset in source partition
    kafka_partition        — source partition
    producer_pipeline_name — value from event envelope
    consumer_pipeline_name — "equity_stream_consumer"
    pipeline_run_id        — UUID per batch (constant within one batch)
    processing_timestamp   — UTC timestamp when batch was processed

Idempotency
-----------
    event_id deduplication using pandas drop_duplicates (pure data logic)
    No Redis dependency for dedup — simpler, faster, deterministic
    Spark checkpoints handle offset tracking

Buffer safety
-------------
    MAX_BUFFER_PER_TICKER   — rolling window cap per symbol
    MAX_TOTAL_TICKER_SYMBOLS — evict oldest symbols when total count exceeds cap
"""

import os
import json
import uuid
import time
import boto3
import pandas as pd
import numpy as np
from io import BytesIO
from datetime import datetime, timezone, timedelta
from typing import List, Dict
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType, StructField, StringType, FloatType
)
import redis
from kafka import KafkaProducer

# =============================================================
# CONFIG  —  single source of truth.  No magic numbers in code.
# =============================================================

CONFIG: dict = {
    # Kafka
    "kafka_broker":         os.getenv("KAFKA_BROKER", "kafka:9092"),
    "topic":                "equity_stream",
    "dlq_topic":            "equity_stream_dlq",

    # Replay
    "replay_mode":          os.getenv("REPLAY_MODE", "kafka"),   # "kafka" | "s3"
    "replay_date":          os.getenv("REPLAY_DATE", ""),         # "YYYY-MM-DD" for s3 replay
    "replay_hour":          os.getenv("REPLAY_HOUR", ""),         # "HH" optional filter

    # S3
    "read_bucket":          "yeeshu-equity-bucket",
    "write_bucket":         "risk-platform-pushparag-analytics",
    "positions_key":        "historical-equity/final_merged.parquet",
    "output_prefix":        "equity/data",
    "consumer_dlq_prefix":  "kafka_dlq/consumer",
    "raw_replay_prefix":    "kafka_raw/equity",
    "checkpoint_dir":       os.getenv("CHECKPOINT_DIR","s3a://risk-platform-pushparag-analytics")+ "/equity/checkpoints",

    # State rebuild (S3 fallback)
    "state_rebuild_minutes":    15,     # how many minutes of history to reload on startup
    "state_max_rows_load":      50000,  # safety cap for state rebuild memory
    
    # Late event handling
    "late_event_max_minutes":   5,      # skip events older than this

    # Buffer safety (single definition - no duplicates)
    "max_buffer_per_ticker":    16,     # max rows retained per ticker symbol
    "max_total_ticker_symbols": 500,    # evict oldest symbols beyond this count

    # Redis (snapshots only — dedup removed)
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

    # Positions columns to drop (unchanged from original)
    "positions_drop_cols": [
        "option_type", "Discretion", "other_manager",
        "Sole", "Shared", "None", "beta",
        "dividendYield", "gdp", "unrate", "cpi", "fedfunds", "date"
    ],
}


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
        "ts":       datetime.now(timezone.utc).isoformat(),
        "level":    level,
        "pipeline": CONFIG["consumer_pipeline_name"],
        "msg":      message,
        **context,
    }
    print(json.dumps(record, default=str))

# =============================================================
# REDIS  —  snapshots ONLY (dedup removed)
# =============================================================

redis_client = redis.Redis(
    host            = CONFIG["redis_host"],
    port            = CONFIG["redis_port"],
    db              = CONFIG["redis_db_stream"],
    decode_responses = True,
)

# Note: No dedup functions — using pandas drop_duplicates instead

# =============================================================
# S3 HELPERS  —  atomic writes + state rebuild
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


def list_s3_files_with_prefix(bucket: str, prefix: str) -> List[str]:
    """
    List all files with given prefix from S3.
    Returns list of keys.
    """
    s3 = get_s3()
    try:
        paginator = s3.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
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
    """
    Read a single parquet file from S3 into pandas DataFrame.
    Returns empty DataFrame on error.
    """
    try:
        obj = get_s3().get_object(Bucket=bucket, Key=key)
        df = pd.read_parquet(BytesIO(obj["Body"].read()))
        return df
    except Exception as exc:
        log("ERROR", "Failed to read parquet from S3",
            {"bucket": bucket, "key": key, "error": str(exc)})
        return pd.DataFrame()

# =============================================================
# KAFKA DLQ PRODUCER
# =============================================================

_dlq_producer = KafkaProducer(
    bootstrap_servers = CONFIG["kafka_broker"],
    value_serializer  = lambda v: json.dumps(v, default=str).encode("utf-8"),
)


def send_to_kafka_dlq(payload: dict) -> None:
    """Send a single DLQ record to the Kafka DLQ topic (non-blocking)."""
    try:
        _dlq_producer.send(CONFIG["dlq_topic"], payload)
    except Exception as exc:
        log("ERROR", "Kafka DLQ send failed",
            {"error": str(exc), "event_id": payload.get("event_id", "unknown")})


def flush_consumer_dlq_to_s3(
    dlq_buffer: list[dict],
    batch_id:   str,
) -> None:
    """
    Batch-flush consumer DLQ records to S3 as parquet (atomic write).
    Called once per batch — not per failed event.

    Path: s3://risk-platform-pushparag-analytics/kafka_dlq/consumer/
              year=Y/month=MM/day=DD/dlq_<batch_id>.parquet
    """
    if not dlq_buffer:
        return

    df  = pd.DataFrame(dlq_buffer)
    now = datetime.now(timezone.utc)
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
        log("INFO", "Positions loaded", {"rows": len(df), "tickers": df["ticker"].nunique()
            if "ticker" in df.columns else "N/A"})
        return df
    except Exception as exc:
        log("ERROR", "Failed to load positions", {"error": str(exc)})
        raise

# =============================================================
# STATE MANAGEMENT  (rebuild from S3 on startup)
# =============================================================

# Global ticker buffer — shared across batches within the same executor
ticker_buffers: dict[str, list] = {}


def rebuild_state_from_s3() -> None:
    """
    Rebuild ticker_buffers from raw S3 data on startup.
    
    Reads last N minutes (config: state_rebuild_minutes) of raw Kafka storage
    and reconstructs the rolling buffers per ticker.
    
    Optimizations:
    - Filters by event_time early to reduce memory
    - Caps total rows loaded (state_max_rows_load)
    - Sorts by timestamp for correct ordering
    
    This ensures:
    - State survives restarts
    - Rolling metrics are accurate from the beginning
    - No cold-start problems
    
    Called ONCE at startup before processing any new data.
    """
    global ticker_buffers
    
    rebuild_window_minutes = CONFIG["state_rebuild_minutes"]
    max_buffer_per_ticker = CONFIG["max_buffer_per_ticker"]
    max_rows_load = CONFIG["state_max_rows_load"]
    
    log("INFO", "Starting state rebuild from S3",
        {"window_minutes": rebuild_window_minutes, "max_rows_cap": max_rows_load})
    
    # Calculate time window
    now_utc = datetime.now(timezone.utc)
    cutoff_time = now_utc - timedelta(minutes=rebuild_window_minutes)
    
    # Build S3 prefixes for last N minutes
    # We need to scan multiple hour partitions
    prefixes = []
    current = cutoff_time.replace(minute=0, second=0, microsecond=0)
    end = now_utc.replace(minute=0, second=0, microsecond=0)
    
    while current <= end:
        prefix = (
            f"{CONFIG['raw_replay_prefix']}/"
            f"year={current.year}/month={current.month:02d}/"
            f"day={current.day:02d}/hour={current.hour:02d}/"
        )
        prefixes.append(prefix)
        current += timedelta(hours=1)
    
    log("INFO", "Scanning S3 partitions for state rebuild",
        {"prefixes": prefixes, "partition_count": len(prefixes)})
    
    all_rows: List[dict] = []
    
    # Collect all relevant rows from S3
    for prefix in prefixes:
        keys = list_s3_files_with_prefix(CONFIG["write_bucket"], prefix)
        
        for key in keys:
            df = read_parquet_from_s3(CONFIG["write_bucket"], key)
            if df.empty:
                continue
            
            # Early filter by event_time to reduce memory
            if "event_time" in df.columns:
                df["event_time_dt"] = pd.to_datetime(df["event_time"])
                df = df[df["event_time_dt"] >= cutoff_time]
                if df.empty:
                    continue
                # Drop the helper column
                df = df.drop(columns=["event_time_dt"])
            
            # Convert to dict records
            rows = df.to_dict("records")
            all_rows.extend(rows)
            
            # Safety cap: prevent memory explosion
            if len(all_rows) > max_rows_load:
                log("WARNING", "Reached max rows limit during state rebuild, truncating",
                    {"limit": max_rows_load, "current_rows": len(all_rows)})
                all_rows = all_rows[-max_rows_load:]
                break
        
        if len(all_rows) >= max_rows_load:
            break
    
    if not all_rows:
        log("WARNING", "No historical data found for state rebuild",
            {"window_minutes": rebuild_window_minutes})
        return
    
    # Sort by event_time to maintain correct order (critical for rolling metrics)
    all_rows.sort(key=lambda x: x.get("event_time", ""))
    
    log("INFO", "Rebuilding state from historical data",
        {"total_rows": len(all_rows), 
         "unique_tickers": len(set(r.get("ticker", "") for r in all_rows)),
         "time_range_start": all_rows[0].get("event_time", "N/A"),
         "time_range_end": all_rows[-1].get("event_time", "N/A")})
    
    # Rebuild buffers per ticker
    rebuilt_count = 0
    skipped_late = 0
    
    for row in all_rows:
        ticker = row.get("ticker")
        if not ticker:
            continue
        
        # Optional: filter very old events during rebuild (defensive)
        event_time_str = row.get("event_time")
        if event_time_str:
            try:
                event_time = pd.to_datetime(event_time_str)
                if event_time < cutoff_time - timedelta(minutes=CONFIG["late_event_max_minutes"]):
                    skipped_late += 1
                    continue
            except:
                pass
        
        if ticker not in ticker_buffers:
            ticker_buffers[ticker] = []
        
        ticker_buffers[ticker].append(row)
        
        # Trim to max buffer size
        if len(ticker_buffers[ticker]) > max_buffer_per_ticker:
            ticker_buffers[ticker] = ticker_buffers[ticker][-max_buffer_per_ticker:]
        
        rebuilt_count += 1
    
    # Final trim for all tickers
    for ticker in list(ticker_buffers.keys()):
        if len(ticker_buffers[ticker]) > max_buffer_per_ticker:
            ticker_buffers[ticker] = ticker_buffers[ticker][-max_buffer_per_ticker:]
    
    log("INFO", "State rebuild complete",
        {"tickers_rebuilt": len(ticker_buffers), 
         "total_rows_loaded": rebuilt_count,
         "rows_skipped_too_old": skipped_late})


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
        log("DEBUG", "Ticker buffer trimmed",
            {"ticker": ticker, "trimmed": overflow, "cap": cap})

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
        metrics["close_diff"]    = closes[-1] - closes[-2]
        metrics["return_1m"]     = (closes[-1] - closes[-2]) / closes[-2]
        metrics["range_pct_1m"]  = (highs[-1] - lows[-1]) / closes[-2]

    if len(closes) >= 6:
        metrics["return_5m"]     = (closes[-1] - closes[-6]) / closes[-6]
        metrics["trend_slope_5m"] = (closes[-1] - closes[-6]) / 5

    if len(closes) >= 16:
        r   = (closes[1:] - closes[:-1]) / closes[:-1]
        log_r = np.log1p(r)
        metrics["vol_15m"] = float(np.std(log_r[-15:]))

    if len(closes) >= 5:
        metrics["rolling_vwap_5m"] = float(
            np.sum(closes[-5:] * volumes[-5:]) / np.sum(volumes[-5:])
        )
        metrics["rolling_high_5m"] = float(np.max(highs[-5:]))
        metrics["rolling_low_5m"]  = float(np.min(lows[-5:]))

        high5 = np.max(highs[-5:])
        low5  = np.min(lows[-5:])
        rng   = high5 - low5
        metrics["breakout_strength"] = (
            float((closes[-1] - low5) / rng) if rng != 0 else None
        )

    if len(volumes) >= 6:
        avg5 = np.mean(volumes[-6:-1])
        metrics["volume_burst"] = float(volumes[-1] / avg5) if avg5 != 0 else None

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

def save_to_parquet(df: pd.DataFrame, batch_id: str) -> None:
    """
    Write processed batch to S3 as parquet using atomic write.
    Partitioned by date and hour — never overwrites existing batches.

    Path: s3://risk-platform-pushparag-analytics/equity/data/
              date=YYYY-MM-DD/hour=HH/batch_<batch_id>.parquet
    """
    if df.empty:
        return

    try:
        ts             = pd.to_datetime(df["event_time"].iloc[0])
        partition_date = ts.strftime("%Y-%m-%d")
        partition_hour = ts.strftime("%H")

        key = (
            f"{CONFIG['output_prefix']}/"
            f"date={partition_date}/hour={partition_hour}/"
            f"batch_{batch_id}.parquet"
        )
        atomic_write_parquet_to_s3(df, CONFIG["write_bucket"], key)

    except Exception as exc:
        log("ERROR", "save_to_parquet failed",
            {"batch_id": batch_id, "error": str(exc)})
        send_alert(f"S3 write FAILED | batch_id={batch_id} | error={str(exc)}")

# =============================================================
# SNAPSHOT TO REDIS  (unchanged core logic)
# =============================================================

def save_latest_snapshot_all_tickers(
    ticker_buffers: dict,
    positions_df:   pd.DataFrame,
) -> None:
    """
    Build and publish a per-ticker snapshot to Redis.
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
                "prev_open":   np.nan, "prev_high":   np.nan,
                "prev_low":    np.nan, "prev_close":  np.nan,
                "prev_volume": np.nan,
            })

        latest_rows.append(latest_row)

    if not latest_rows:
        return

    try:
        df_latest    = pd.DataFrame(latest_rows)
        df_latest    = enrich_intraday_with_positions(df_latest, positions_df)
        df_latest    = df_latest.where(pd.notnull(df_latest), None)
        snapshot_json = df_latest.to_json(orient="records")

        redis_client.set("equity_latest_snapshot", snapshot_json)
        redis_client.publish("equity_stream", snapshot_json)

        log("INFO", "Redis snapshot published", {"tickers": len(latest_rows)})

    except Exception as exc:
        log("ERROR", "Redis snapshot publish failed", {"error": str(exc)})
        send_alert(f"Redis snapshot FAILED | error={str(exc)}")
# =============================================================
# ADD LINEAGE TO OUTPUT ROWS
# =============================================================

def add_lineage(
    row_dict:              dict,
    batch_id:              str,
    pipeline_run_id:       str,
    processing_timestamp:  str,
) -> dict:
    """
    Stamp lineage fields onto an output row dict.
    All 7 required lineage fields are set here; values are constant
    within a batch (pipeline_run_id, processing_timestamp) or per-row
    (event_id, kafka_offset, kafka_partition, producer_pipeline_name).
    """
    row_dict["consumer_pipeline_name"]  = CONFIG["consumer_pipeline_name"]
    row_dict["pipeline_run_id"]         = pipeline_run_id       # per-batch constant
    row_dict["processing_timestamp"]    = processing_timestamp   # per-batch constant
    row_dict["data_source"]             = CONFIG["data_source"]
    row_dict["transformation"]          = CONFIG["transformation"]
    row_dict["record_created_at"]       = processing_timestamp
    # event_id, kafka_offset, kafka_partition, producer_pipeline_name
    # are already present from the Kafka event envelope — not re-stamped here
    return row_dict

# =============================================================
# PROCESS BATCH  (core consumer logic)
# =============================================================

def process_batch(
    batch_df,
    batch_id:     int,
    positions_df: pd.DataFrame,
) -> None:
    """
    Spark foreachBatch handler. Called once per micro-batch.

    Per-batch lifecycle:
      1. Convert Spark -> pandas
      2. Deduplicate by event_id (pure pandas - no Redis)
      3. Filter late events (older than late_event_max_minutes)
      4. For each row: validation -> buffer update -> metrics computation -> lineage
      5. Enrich with positions
      6. Atomic S3 write
      7. Redis snapshot
      8. Flush consumer DLQ (S3 + Kafka) as a single batch write

    Idempotency:
      - event_id deduplication via drop_duplicates (deterministic, no external deps)
      - Spark checkpoints handle offset tracking
      - Duplicate batches are idempotent (parquet append is safe)
      - State persists across restarts via S3 rebuild

    Late Event Handling:
      - Events older than late_event_max_minutes are skipped
      - Prevents stale data from corrupting rolling metrics
      - Logged and sent to DLQ for auditability

    Partial failure handling:
      Exceptions within a row's processing are caught individually.
      Failed rows are collected in dlq_buffer and written once at end.
      Successful rows are never rolled back by a subsequent row's failure.
    """
    # Batch-level constants
    pipeline_run_id      = str(uuid.uuid4())
    processing_timestamp = datetime.now(timezone.utc).isoformat()
    metrics              = make_batch_metrics()
    dlq_buffer:   list[dict]  = []
    batch_start          = time.monotonic()
    current_time         = datetime.now(timezone.utc)
    late_cutoff          = current_time - timedelta(minutes=CONFIG["late_event_max_minutes"])

    # Batch-level guard — if Spark gives us an empty batch, exit cleanly
    try:
        if batch_df.count() == 0:
            log("INFO", "Empty batch received", {"batch_id": batch_id})
            return
        pdf = batch_df.toPandas()
    except Exception as exc:
        log("ERROR", "Failed to convert batch to pandas",
            {"batch_id": batch_id, "error": str(exc)})
        send_alert(f"Batch conversion FAILED | batch_id={batch_id} | error={str(exc)}")
        return

    # ── Step 1: Deduplicate by event_id (pure data logic, no Redis) ──
    original_count = len(pdf)
    pdf = pdf.drop_duplicates(subset=["event_id"])
    deduped_count = len(pdf)
    metrics["events_deduped"] = original_count - deduped_count

    if deduped_count == 0:
        log("INFO", "All events in batch were duplicates",
            {"batch_id": batch_id, "original_count": original_count})
        metrics["batch_latency_s"] = round(time.monotonic() - batch_start, 3)
        log_batch_metrics(str(batch_id), metrics)
        return

    # ── Step 2: Filter late events (prevent stale data corruption) ──
    if "event_time" in pdf.columns:
        pdf["event_time_dt"] = pd.to_datetime(pdf["event_time"])

        # STEP 1: Capture late events BEFORE filtering
        late_events = pdf[pdf["event_time_dt"] < late_cutoff]

        before_late_filter = len(pdf)

        # STEP 2: Keep only valid events
        pdf = pdf[pdf["event_time_dt"] >= late_cutoff]

        after_late_filter = len(pdf)

        metrics["events_late"] = len(late_events)

        # STEP 3: Send late events to DLQ
        if not late_events.empty:
            for _, late_row in late_events.iterrows():
                error_payload = {
                    "error": f"Late event skipped (older than {CONFIG['late_event_max_minutes']} minutes)",
                    "event_id": late_row.get("event_id", ""),
                    "ticker": late_row.get("ticker", ""),
                    "event_time": late_row.get("event_time", ""),
                    "current_time": current_time.isoformat(),
                    "kafka_offset": int(late_row.get("offset", -1)),
                    "kafka_partition": int(late_row.get("partition", -1)),
                    "consumer_pipeline": CONFIG["consumer_pipeline_name"],
                    "batch_id": str(batch_id),
                    "pipeline_run_id": pipeline_run_id,
                }
                send_to_kafka_dlq(error_payload)
                dlq_buffer.append(error_payload)

        # Drop helper column
        pdf = pdf.drop(columns=["event_time_dt"])
    
    if len(pdf) == 0:
        log("INFO", "All events filtered out as late",
            {"batch_id": batch_id, "late_count": metrics["events_late"]})
        flush_consumer_dlq_to_s3(dlq_buffer, str(batch_id))
        _dlq_producer.flush()
        metrics["batch_latency_s"] = round(time.monotonic() - batch_start, 3)
        log_batch_metrics(str(batch_id), metrics)
        return

    metrics["events_received"] = len(pdf)
    log("INFO", "Batch received and deduplicated",
        {"batch_id": batch_id,
         "pipeline_run_id": pipeline_run_id,
         "original_rows": original_count,
         "deduped_rows": deduped_count,
         "late_filtered": metrics["events_late"],
         "final_rows": len(pdf),
         "unique_tickers": pdf["ticker"].nunique() if "ticker" in pdf.columns else "N/A",
         "max_offset": int(pdf["offset"].max()) if "offset" in pdf.columns else "N/A",
         "min_offset": int(pdf["offset"].min()) if "offset" in pdf.columns else "N/A"})

    snapshot_rows: list[dict] = []

    for _, row in pdf.iterrows():
        try:
            ticker   = row["ticker"]
            event_id = row["event_id"]

            # Note: No manual offset dedup — Spark checkpoints handle this
            # Note: No Redis event_id dedup — drop_duplicates already handled

            # ── Guard: Data validation ──────────────────────────────
            if row["close"] <= 0 or row["high"] < row["low"]:
                raise ValueError(
                    f"Invalid market data: close={row['close']}, "
                    f"high={row['high']}, low={row['low']}"
                )

            # ── Buffer update (with safety guards) ────────────────────
            row_dict = row.to_dict()
            update_ticker_buffer(ticker, row_dict)

            # ── Feature computation ───────────────────────────────────
            computed = compute_metrics(ticker_buffers[ticker])
            row_dict.update(computed)

            # ── Lineage stamping ──────────────────────────────────────
            row_dict = add_lineage(
                row_dict             = row_dict,
                batch_id             = str(batch_id),
                pipeline_run_id      = pipeline_run_id,
                processing_timestamp = processing_timestamp,
            )
            # Kafka provenance fields — already in the row from parsed schema
            row_dict["kafka_offset"]             = int(row["offset"])
            row_dict["kafka_partition"]          = int(row["partition"])
            row_dict["producer_pipeline_name"]   = row.get(
                "source_pipeline", "equity_kafka_producer"
            )

            snapshot_rows.append(row_dict)
            metrics["events_processed"] += 1

        except Exception as exc:
            metrics["events_failed"] += 1
            error_payload = {
                "error":            str(exc),
                "event_id":         row.get("event_id", ""),
                "ticker":           row.get("ticker", ""),
                "kafka_offset":     int(row.get("offset", -1)),
                "kafka_partition":  int(row.get("partition", -1)),
                "original_event":   row.to_dict(),
                "failed_at":        datetime.now(timezone.utc).isoformat(),
                "consumer_pipeline": CONFIG["consumer_pipeline_name"],
                "batch_id":         str(batch_id),
                "pipeline_run_id":  pipeline_run_id,
            }
            # Route to Kafka DLQ immediately for stream-level handling
            send_to_kafka_dlq(error_payload)
            # Also buffer for batch S3 DLQ flush
            dlq_buffer.append(error_payload)

    if not snapshot_rows:
        # Flush DLQ even when no valid rows
        flush_consumer_dlq_to_s3(dlq_buffer, str(batch_id))
        _dlq_producer.flush()
        metrics["batch_latency_s"] = round(time.monotonic() - batch_start, 3)
        log_batch_metrics(str(batch_id), metrics)
        return

    snapshot_df = pd.DataFrame(snapshot_rows)

    # ── Enrichment ────────────────────────────────────────────────────
    snapshot_df = enrich_intraday_with_positions(snapshot_df, positions_df)
    snapshot_df = snapshot_df.where(pd.notnull(snapshot_df), None)

    # ── Atomic S3 write ───────────────────────────────────────────────
    save_to_parquet(snapshot_df, str(batch_id))

    # ── Redis snapshot ────────────────────────────────────────────────
    save_latest_snapshot_all_tickers(ticker_buffers, positions_df)

    # Note: No manual offset commits — Spark checkpoints handle this automatically
    # Spark's checkpointing provides exactly-once semantics for offset tracking

    # ── Flush DLQ (single batch write to S3 + Kafka) ─────────────────
    flush_consumer_dlq_to_s3(dlq_buffer, str(batch_id))
    _dlq_producer.flush()

    metrics["batch_latency_s"] = round(time.monotonic() - batch_start, 3)
    log_batch_metrics(str(batch_id), metrics)

# =============================================================
# S3 REPLAY  (REPLAY_MODE = "s3")
# =============================================================

def load_s3_replay_partitions(
    date_str:    str,
    hour_str:    str = "",
    positions_df: pd.DataFrame = None,
) -> None:
    """
    Read raw event parquet files from S3 and process them through the same
    pipeline as the Kafka stream. Used when REPLAY_MODE = "s3".

    Path pattern:
        s3://risk-platform-pushparag-analytics/kafka_raw/equity/
            year=Y/month=MM/day=DD/<optional hour filter>/batch_*.parquet

    Each parquet file is treated as one "batch" for consistency.
    Event_id dedup still applies — replay is idempotent.
    Late event filtering still applies (respects late_event_max_minutes).
    """
    if not date_str:
        log("ERROR", "REPLAY_DATE must be set for s3 replay mode.")
        return

    try:
        dt     = datetime.strptime(date_str, "%Y-%m-%d")
        prefix = (
            f"{CONFIG['raw_replay_prefix']}/"
            f"year={dt.year}/month={dt.month:02d}/day={dt.day:02d}/"
        )
        if hour_str:
            prefix = f"{prefix}hour={int(hour_str):02d}/"
    except ValueError as exc:
        log("ERROR", "Invalid REPLAY_DATE format — expected YYYY-MM-DD",
            {"replay_date": date_str, "error": str(exc)})
        return

    log("INFO", "Starting S3 replay",
        {"date": date_str, "hour": hour_str, "prefix": prefix})

    s3 = get_s3()
    try:
        paginator = s3.get_paginator("list_objects_v2")
        pages     = paginator.paginate(
            Bucket=CONFIG["write_bucket"], Prefix=prefix
        )
        keys = [
            obj["Key"]
            for page in pages
            for obj in page.get("Contents", [])
            if obj["Key"].endswith(".parquet")
        ]
    except Exception as exc:
        log("ERROR", "S3 replay listing failed",
            {"prefix": prefix, "error": str(exc)})
        return

    if not keys:
        log("WARNING", "No raw parquet files found for replay",
            {"prefix": prefix})
        return

    log("INFO", "Replay partitions found", {"count": len(keys)})

    # Process each partition file as a synthetic batch
    for batch_num, key in enumerate(keys, start=1):
        try:
            obj = s3.get_object(Bucket=CONFIG["write_bucket"], Key=key)
            pdf = pd.read_parquet(BytesIO(obj["Body"].read()))

            if pdf.empty:
                continue

            # Replay needs Kafka metadata columns — synthesize if absent
            if "topic" not in pdf.columns:
                pdf["topic"]     = CONFIG["topic"]
            if "partition" not in pdf.columns:
                pdf["partition"] = 0
            if "offset" not in pdf.columns:
                pdf["offset"]    = batch_num * 10000  # synthetic — non-colliding

            log("INFO", "Replaying S3 partition",
                {"key": key, "rows": len(pdf), "batch_num": batch_num})

            # Build a mock Spark-like batchDF wrapper using a simple object
            # process_batch expects .count() and .toPandas() — provide both
            class _MockBatch:
                def __init__(self, df):
                    self._df = df
                def count(self):
                    return len(self._df)
                def toPandas(self):
                    return self._df.copy()

            process_batch(_MockBatch(pdf), batch_num, positions_df)

        except Exception as exc:
            log("ERROR", "S3 replay partition failed",
                {"key": key, "error": str(exc)})

    log("INFO", "S3 replay complete", {"date": date_str, "files_processed": len(keys)})

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
    StructField("event_id",       StringType(), True),
    StructField("event_type",     StringType(), True),
    StructField("schema_version", StringType(), True),
    StructField("pipeline_name",  StringType(), True),   # producer pipeline
    StructField("data_source",    StringType(), True),
    StructField("producer_run_id", StringType(), True),  # new: producer lineage
    StructField("batch_id",        StringType(), True),  # new: producer batch
    StructField("source_fetch_time", StringType(), True),# new: fetch time
    StructField("event_time",     StringType(), True),
    StructField("ingested_at",    StringType(), True),
    StructField("data", StructType([
        StructField("ticker",    StringType(), True),
        StructField("open",      FloatType(),  True),
        StructField("high",      FloatType(),  True),
        StructField("low",       FloatType(),  True),
        StructField("close",     FloatType(),  True),
        StructField("volume",    FloatType(),  True),
        StructField("timestamp", StringType(), True),
        StructField("date",      StringType(), True),
    ])),
])

# =============================================================
# ENTRY POINT
# =============================================================

if __name__ == "__main__":

    # Load static positions once at startup (read-only)
    positions_df = load_positions()

    # ── CRITICAL: Rebuild state from S3 BEFORE processing ──
    # This ensures rolling metrics survive restarts
    log("INFO", "Rebuilding state from S3 before starting pipeline")
    rebuild_state_from_s3()
    log("INFO", "State rebuild complete, starting pipeline",
        {"tickers_in_state": len(ticker_buffers)})

    replay_mode = CONFIG["replay_mode"].lower()
    log("INFO", "Consumer starting", {"replay_mode": replay_mode})

    if replay_mode == "s3":
        # ── S3 Replay mode — process raw parquet directly, no Spark stream ──
        load_s3_replay_partitions(
            date_str     = CONFIG["replay_date"],
            hour_str     = CONFIG["replay_hour"],
            positions_df = positions_df,
        )

    else:
        # ── Kafka stream mode (default) ──────────────────────────────────
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
                "event_time",
                "ingested_at",
                col("data.*"),
                "topic", "partition", "offset", "kafka_timestamp",
            )
            .filter(col("volume") > 0)
        )

        query = (
            parsed_df.writeStream
            .foreachBatch(lambda batch_df, batch_id:
                          process_batch(batch_df, batch_id, positions_df))
            .outputMode("append")
            .option("checkpointLocation", CONFIG["checkpoint_dir"])
            .start()
        )

        query.awaitTermination()