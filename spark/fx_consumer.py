"""
fx_kafka_consumer.py
=====================
Production-grade Spark Structured Streaming consumer for real-time FX ticks.
Architecture mirrors equity_kafka_consumer.py exactly, adapted for FX.

Storage rules
-------------
READ-ONLY  : s3://pushparag-fx-bucket       (FX universe / positions)
ALL WRITES : s3://pushparag-risk-analytics  (parquet output, DLQ)

Storage layout (writes)
-----------------------
Processed output (parquet, partitioned):
    s3://pushparag-risk-analytics/fx/data/
        date=YYYY-MM-DD/hour=HH/batch_<batch_id>.parquet

Consumer DLQ (S3, parquet, partitioned by day):
    s3://pushparag-risk-analytics/kafka_dlq/fx/consumer/
        year=Y/month=MM/day=DD/dlq_<batch_id>.parquet

Consumer DLQ (Kafka topic, for stream-level routing):
    topic: fx_stream_dlq

Replay modes
------------
REPLAY_MODE = "kafka"  -> Spark Structured Streaming from Kafka (default)
REPLAY_MODE = "s3"     -> reads kafka_raw/fx/ parquet directly,
                         processes through the same pipeline, writes output.

State management
----------------
On startup  : rebuilds pair_buffers from last 15 minutes of raw S3 data.
At runtime  : maintains rolling OHLC buffers in memory per currency pair.
On restart  : state is rehydrated from S3 — no cold-start metric accuracy loss.

Idempotency
-----------
    event_id deduplication via pandas drop_duplicates (deterministic, no Redis).
    Spark checkpoints handle offset tracking.
    Duplicate batches are idempotent (parquet writes use unique batch_id keys).

Late event handling
-------------------
    Events older than late_event_max_minutes are skipped and routed to DLQ.

Exposure logic
--------------
    Loads FX universe from S3: ticker -> sector -> currency_pair mapping.
    For each FX bar, fans out to all tickers exposed to that currency pair.
    Computes position_size, fx_pnl, VaR_95_15m per ticker row.

Lineage on every output row
----------------------------
    event_id               — from producer (SHA-256, idempotent)
    kafka_offset           — exact Kafka partition offset
    kafka_partition        — source partition
    producer_pipeline_name — from event envelope
    consumer_pipeline_name — "fx_stream_consumer"
    pipeline_run_id        — UUID per batch (constant within one batch)
    processing_timestamp   — UTC timestamp when batch was processed
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
from collections import deque
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
    "kafka_broker":          os.getenv("KAFKA_BROKER", "kafka:9092"),
    "topic":                 "fx_ohlc_1m",
    "dlq_topic":             "fx_stream_dlq",

    # Replay
    "replay_mode":           os.getenv("REPLAY_MODE", "kafka"),   # "kafka" | "s3"
    "replay_date":           os.getenv("REPLAY_DATE", ""),         # "YYYY-MM-DD"
    "replay_hour":           os.getenv("REPLAY_HOUR", ""),         # "HH" optional

    # S3
    "read_bucket":           "pushparag-fx-bucket",
    "write_bucket":          "pushparag-risk-analytics",
    "universe_key":          "historical-fx/final_merged.parquet",
    "output_prefix":         "fx/data",
    "consumer_dlq_prefix":   "kafka_dlq/fx/consumer",
    "raw_replay_prefix":     "kafka_raw/fx",
    "checkpoint_dir":        (
        os.getenv("CHECKPOINT_DIR", "s3a://pushparag-risk-analytics")
        + "/fx/checkpoints"
    ),

    # State rebuild (S3 fallback on startup)
    "state_rebuild_minutes":    15,
    "state_max_rows_load":      50000,

    # Late event handling
    "late_event_max_minutes":   5,

    # Buffer safety
    "max_buffer_per_pair":      60,    # FX 24/5 — larger buffer than equity
    "max_total_pairs":          50,    # FX universe is small — conservative cap

    # Redis (snapshot publish ONLY — not used for dedup or offsets)
    "redis_host":            os.getenv("REDIS_HOST", "localhost"),
    "redis_port":            int(os.getenv("REDIS_PORT", 6379)),
    "redis_db_stream":       int(os.getenv("REDIS_DB_STREAM", 1)),

    # Exposure constants (mirroring original fx consumer)
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
# REDIS  —  snapshot publish ONLY
# =============================================================

redis_client = redis.Redis(
    host             = CONFIG["redis_host"],
    port             = CONFIG["redis_port"],
    db               = CONFIG["redis_db_stream"],
    decode_responses = True,
)

# Note: Redis is NOT used for dedup or offset management.
# Dedup  -> pandas drop_duplicates(event_id)
# Offsets -> Spark checkpoint

# =============================================================
# S3 HELPERS  —  atomic writes + state rebuild utilities
# =============================================================

def get_s3():
    return boto3.client("s3")


def atomic_write_parquet_to_s3(df: pd.DataFrame, bucket: str, key: str) -> None:
    """
    Atomically write DataFrame as parquet to S3.
    PUT -> _temp/<key> -> server-side COPY -> DELETE temp.
    Final key is either complete or absent — never partial.
    """
    temp_key = f"_temp/{key}"
    s3       = get_s3()

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
        log("ERROR", "S3 list failed",
            {"bucket": bucket, "prefix": prefix, "error": str(exc)})
        return []


def read_parquet_from_s3(bucket: str, key: str) -> pd.DataFrame:
    """Read a single parquet file from S3. Returns empty DataFrame on error."""
    try:
        obj = get_s3().get_object(Bucket=bucket, Key=key)
        return pd.read_parquet(BytesIO(obj["Body"].read()))
    except Exception as exc:
        log("ERROR", "S3 parquet read failed",
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
    """Send a DLQ record to the Kafka DLQ topic (non-blocking)."""
    try:
        _dlq_producer.send(CONFIG["dlq_topic"], payload)
    except Exception as exc:
        log("ERROR", "Kafka FX DLQ send failed",
            {"error": str(exc), "event_id": payload.get("event_id", "unknown")})


def flush_consumer_dlq_to_s3(dlq_buffer: list[dict], batch_id: str) -> None:
    """
    Batch-flush consumer DLQ records to S3 as parquet (atomic write).
    Called once per batch — not per failed event.

    Path: s3://pushparag-risk-analytics/kafka_dlq/fx/consumer/
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

def load_fx_universe() -> tuple[pd.DataFrame, dict]:
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
            {"tickers":      len(df),
             "pairs":        len(pair_groups),
             "pair_list":    list(pair_groups.keys())})
        return df, pair_groups

    except Exception as exc:
        log("ERROR", "Failed to load FX universe", {"error": str(exc)})
        raise

# =============================================================
# PAIR BUFFER MANAGEMENT
# =============================================================

# Global pair buffer — maintained across batches within the same executor.
# Each key is a currency_pair; value is a list of OHLC dicts.
pair_buffers: dict[str, list] = {}


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
# STATE REBUILD  (from S3 on startup)
# =============================================================

def rebuild_state_from_s3() -> None:
    """
    Rebuild pair_buffers from raw FX S3 data on startup.

    Reads the last N minutes (state_rebuild_minutes) of raw Kafka storage
    and reconstructs the rolling OHLC buffers per currency pair.

    CRITICAL: Rebuild uses EXACT SAME buffer structure as runtime:
        {
            "currency_pair": str,
            "open": float,
            "high": float,
            "low": float,
            "close": float,
            "event_time": str,
            "ingested_at": str
        }

    This ensures:
    - State survives restarts — no cold-start metric accuracy loss.
    - fx_return_5m and fx_vol_15m are accurate from the first new batch.
    - No dependency on in-memory state that is lost between runs.
    - Runtime and rebuild buffers are identical — no silent bugs.

    Called ONCE at startup before processing any new data.
    """
    global pair_buffers

    rebuild_mins = CONFIG["state_rebuild_minutes"]
    max_rows     = CONFIG["state_max_rows_load"]
    cap          = CONFIG["max_buffer_per_pair"]

    log("INFO", "Starting FX state rebuild from S3",
        {"window_minutes": rebuild_mins, "max_rows_cap": max_rows})

    now_utc     = datetime.now(timezone.utc)
    cutoff_time = now_utc - timedelta(minutes=rebuild_mins)

    # Identify which hour partitions to scan
    prefixes = []
    current  = cutoff_time.replace(minute=0, second=0, microsecond=0)
    end      = now_utc.replace(minute=0, second=0, microsecond=0)
    while current <= end:
        prefix = (
            f"{CONFIG['raw_replay_prefix']}/"
            f"year={current.year}/month={current.month:02d}/"
            f"day={current.day:02d}/hour={current.hour:02d}/"
        )
        prefixes.append(prefix)
        current += timedelta(hours=1)

    log("INFO", "Scanning S3 partitions for FX state rebuild",
        {"partitions": len(prefixes)})

    all_rows: List[dict] = []

    for prefix in prefixes:
        keys = list_s3_files_with_prefix(CONFIG["write_bucket"], prefix)

        for key in keys:
            df = read_parquet_from_s3(CONFIG["write_bucket"], key)
            if df.empty:
                continue

            # Early time filter to reduce memory pressure
            if "event_time" in df.columns:
                df["_evt_dt"] = pd.to_datetime(df["event_time"])
                df = df[df["_evt_dt"] >= cutoff_time]
                if df.empty:
                    continue
                df = df.drop(columns=["_evt_dt"])

            all_rows.extend(df.to_dict("records"))

            if len(all_rows) >= max_rows:
                log("WARNING", "FX state rebuild hit max rows cap — truncating",
                    {"limit": max_rows})
                all_rows = all_rows[-max_rows:]
                break

        if len(all_rows) >= max_rows:
            break

    if not all_rows:
        log("WARNING", "No FX history found for state rebuild — starting cold",
            {"window_minutes": rebuild_mins})
        return

    # Sort by event_time — critical for correct rolling metric ordering
    all_rows.sort(key=lambda x: x.get("event_time", ""))

    log("INFO", "Rebuilding FX state from historical data",
        {"total_rows":       len(all_rows),
         "unique_pairs":     len(set(r.get("currency_pair", "") for r in all_rows)),
         "time_range_start": all_rows[0].get("event_time", "N/A"),
         "time_range_end":   all_rows[-1].get("event_time", "N/A")})

    rebuilt  = 0
    skipped  = 0
    late_cut = cutoff_time - timedelta(minutes=CONFIG["late_event_max_minutes"])

    for row in all_rows:
        pair = row.get("currency_pair")
        if not pair:
            continue

        # Skip rows that are too old relative to the rebuild window
        evt_str = row.get("event_time")
        if evt_str:
            try:
                evt_dt = pd.to_datetime(evt_str)
                if evt_dt < late_cut:
                    skipped += 1
                    continue
            except Exception:
                pass

        # ─── FIX 1: Use EXACT SAME buffer structure as runtime ───
        # CRITICAL: Do NOT store full raw row in buffer
        # Buffer must contain ONLY OHLC + minimal metadata
        clean_bar = {
            "currency_pair": pair,
            "open":          float(row.get("open", 0)),
            "high":          float(row.get("high", 0)),
            "low":           float(row.get("low", 0)),
            "close":         float(row.get("close", 0)),
            "event_time":    row.get("event_time", ""),
            "ingested_at":   row.get("ingested_at", "")
        }

        if pair not in pair_buffers:
            pair_buffers[pair] = []

        pair_buffers[pair].append(clean_bar)

        if len(pair_buffers[pair]) > cap:
            pair_buffers[pair] = pair_buffers[pair][-cap:]

        rebuilt += 1

    log("INFO", "FX state rebuild complete",
        {"pairs_rebuilt":  len(pair_buffers),
         "rows_loaded":    rebuilt,
         "rows_skipped":   skipped})

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
        fx_vol_15m     — 15-bar log-return standard deviation (annualisation
                         is left to the consumer — raw vol for intraday use)
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
        r   = np.diff(closes) / closes[:-1]
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
) -> list[dict]:
    """
    Fan out a single FX OHLC bar into one output row per exposed ticker.

    For each ticker in pair_groups[pair]:
        position_size = daily_exposure (revenue-weighted, quarter normalised)
        fx_pnl        = position_size × fx_return_1m
        VaR_95_15m    = position_size × fx_vol_15m × Z_95

    This mirrors the original fx_kafka_consumer compute logic exactly.
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

def save_to_parquet(df: pd.DataFrame, batch_id: str) -> None:
    """
    Write processed FX batch to S3 as parquet using atomic write.
    Partitioned by date and hour — each batch gets a unique key.

    Path: s3://pushparag-risk-analytics/fx/data/
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
# REDIS SNAPSHOT  (publish per-pair latest state)
# =============================================================

def publish_fx_snapshot_to_redis(snapshot_rows: list[dict]) -> None:
    """
    Publish the latest FX snapshot to Redis for real-time consumers.
    Mirrors the equity pattern: SET + PUBLISH.
    """
    if not snapshot_rows:
        return

    try:
        safe_rows     = [{k: json_safe(v) for k, v in r.items()} for r in snapshot_rows]
        snapshot_json = json.dumps(safe_rows, default=str)

        redis_client.set("fx_latest_snapshot", snapshot_json)
        redis_client.publish("fx_stream", snapshot_json)

        log("INFO", "Redis FX snapshot published",
            {"pairs": len(set(r.get("currency_pair") for r in snapshot_rows)),
             "rows":  len(snapshot_rows)})
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
) -> dict:
    """
    Stamp all 7 required lineage fields onto an output row.
    Constant fields (pipeline_run_id, processing_timestamp) are set here.
    Per-row fields (kafka_offset, kafka_partition) are passed in from the source row.
    """
    row_dict["consumer_pipeline_name"]  = CONFIG["consumer_pipeline_name"]
    row_dict["pipeline_run_id"]         = pipeline_run_id
    row_dict["processing_timestamp"]    = processing_timestamp
    row_dict["data_source"]             = CONFIG["data_source"]
    row_dict["transformation"]          = CONFIG["transformation"]
    row_dict["record_created_at"]       = processing_timestamp
    # Per-row Kafka provenance
    row_dict["kafka_offset"]            = kafka_offset
    row_dict["kafka_partition"]         = kafka_partition
    row_dict["producer_pipeline_name"]  = producer_pipeline
    return row_dict

# =============================================================
# PROCESS BATCH  (core consumer logic)
# =============================================================

def process_batch(
    batch_df,
    batch_id:    int,
    pair_groups: dict,
) -> None:
    """
    Spark foreachBatch handler. Called once per micro-batch.

    Per-batch lifecycle:
      1. Convert Spark -> pandas; guard empty batch.
      2. Deduplicate by event_id (pandas drop_duplicates — no Redis).
      3. Filter late events (> late_event_max_minutes old); route to DLQ.
      4. Per-bar: validate -> buffer update -> compute FX metrics ->
         fan-out to ticker rows -> add lineage.
      5. Enriched DataFrame -> atomic S3 write.
      6. Redis snapshot publish.
      7. Flush consumer DLQ (Kafka + S3) as single batch write.
      8. Log batch metrics.

    Partial failure handling:
        Per-bar exceptions are isolated — failed bars go to DLQ without
        affecting successfully processed bars.
    """
    pipeline_run_id      = str(uuid.uuid4())
    processing_timestamp = datetime.now(timezone.utc).isoformat()
    metrics              = make_batch_metrics()
    dlq_buffer: list[dict] = []
    batch_start          = time.monotonic()
    current_time         = datetime.now(timezone.utc)
    late_cutoff          = current_time - timedelta(minutes=CONFIG["late_event_max_minutes"])

    # ── Guard: empty batch ─────────────────────────────────────────────
    try:
        if batch_df.count() == 0:
            log("INFO", "Empty FX batch received", {"batch_id": batch_id})
            return
        pdf = batch_df.toPandas()
    except Exception as exc:
        log("ERROR", "Failed to convert FX batch to pandas",
            {"batch_id": batch_id, "error": str(exc)})
        send_alert(f"Batch conversion FAILED | batch_id={batch_id} | error={str(exc)}")
        return

    # ── Step 1: Deduplicate by event_id ──────────────────────────────
    original_count         = len(pdf)
    pdf                    = pdf.drop_duplicates(subset=["event_id"])
    metrics["events_deduped"] = original_count - len(pdf)

    if len(pdf) == 0:
        log("INFO", "All FX events in batch were duplicates",
            {"batch_id": batch_id, "original_count": original_count})
        metrics["batch_latency_s"] = round(time.monotonic() - batch_start, 3)
        log_batch_metrics(str(batch_id), metrics)
        return

    # ── Step 2: Filter late events ────────────────────────────────────
    if "event_time" in pdf.columns:
        pdf["_evt_dt"] = pd.to_datetime(pdf["event_time"])
        late_events    = pdf[pdf["_evt_dt"] < late_cutoff]
        pdf            = pdf[pdf["_evt_dt"] >= late_cutoff].copy()
        metrics["events_late"] = len(late_events)

        # Route late events to DLQ immediately
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
            send_to_kafka_dlq(dlq_entry)
            dlq_buffer.append(dlq_entry)

        pdf = pdf.drop(columns=["_evt_dt"])

    if len(pdf) == 0:
        log("INFO", "All FX events filtered as late",
            {"batch_id": batch_id, "late_count": metrics["events_late"]})
        flush_consumer_dlq_to_s3(dlq_buffer, str(batch_id))
        _dlq_producer.flush()
        metrics["batch_latency_s"] = round(time.monotonic() - batch_start, 3)
        log_batch_metrics(str(batch_id), metrics)
        return

    metrics["events_received"] = len(pdf)
    log("INFO", "FX batch received and deduplicated",
        {"batch_id":         batch_id,
         "pipeline_run_id":  pipeline_run_id,
         "original_rows":    original_count,
         "final_rows":       len(pdf),
         "late_filtered":    metrics["events_late"],
         "unique_pairs":     pdf["currency_pair"].nunique()
                             if "currency_pair" in pdf.columns else "N/A",
         "max_offset":       int(pdf["offset"].max())
                             if "offset" in pdf.columns else "N/A"})

    output_rows: list[dict] = []

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

            # ─── Buffer update: clean OHLC structure (same as rebuild) ───
            # CRITICAL: Do NOT store full row in buffer
            # Buffer must contain ONLY OHLC + minimal metadata
            clean_bar = {
                "currency_pair": pair,
                "open":          float(bar["open"]),
                "high":          float(bar["high"]),
                "low":           float(bar["low"]),
                "close":         float(bar["close"]),
                "event_time":    bar.get("event_time", ""),
                "ingested_at":   bar.get("ingested_at", "")
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
            kafka_offset     = int(bar.get("offset", -1))
            kafka_partition  = int(bar.get("partition", -1))
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
                )
                output_rows.append(out_row)

            metrics["events_processed"] += 1

        except Exception as exc:
            metrics["events_failed"] += 1
            dlq_entry = {
                "error":            str(exc),
                "event_id":         bar.get("event_id", ""),
                "currency_pair":    bar.get("currency_pair", ""),
                "kafka_offset":     int(bar.get("offset", -1)),
                "kafka_partition":  int(bar.get("partition", -1)),
                "original_event":   bar.to_dict(),
                "failed_at":        datetime.now(timezone.utc).isoformat(),
                "consumer_pipeline": CONFIG["consumer_pipeline_name"],
                "batch_id":         str(batch_id),
                "pipeline_run_id":  pipeline_run_id,
            }
            send_to_kafka_dlq(dlq_entry)
            dlq_buffer.append(dlq_entry)

    metrics["output_rows"] = len(output_rows)

    if not output_rows:
        flush_consumer_dlq_to_s3(dlq_buffer, str(batch_id))
        _dlq_producer.flush()
        metrics["batch_latency_s"] = round(time.monotonic() - batch_start, 3)
        log_batch_metrics(str(batch_id), metrics)
        return

    # ── Step 4: Build output DataFrame, clean NaN ─────────────────────
    df_out = pd.DataFrame(output_rows)
    df_out = df_out.where(pd.notnull(df_out), None)

    # ── Step 5: Atomic S3 write ───────────────────────────────────────
    save_to_parquet(df_out, str(batch_id))

    # ── Step 6: Redis snapshot ────────────────────────────────────────
    publish_fx_snapshot_to_redis(output_rows)

    # Note: No manual offset commits — Spark checkpoints handle this.

    # ── Step 7: Flush DLQ ────────────────────────────────────────────
    flush_consumer_dlq_to_s3(dlq_buffer, str(batch_id))
    _dlq_producer.flush()

    metrics["batch_latency_s"] = round(time.monotonic() - batch_start, 3)
    log_batch_metrics(str(batch_id), metrics)

# =============================================================
# S3 REPLAY  (REPLAY_MODE = "s3")
# =============================================================

def load_s3_replay_partitions(
    date_str:    str,
    hour_str:    str      = "",
    pair_groups: dict     = None,
) -> None:
    """
    Read raw FX event parquet files from S3 and reprocess them through the
    same pipeline. Used when REPLAY_MODE = "s3".

    Path pattern:
        s3://pushparag-risk-analytics/kafka_raw/fx/
            year=Y/month=MM/day=DD/<optional hour>/batch_*.parquet

    Each parquet file is treated as one synthetic batch.
    event_id dedup and late event filtering both apply — replay is idempotent.
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

    log("INFO", "Starting FX S3 replay",
        {"date": date_str, "hour": hour_str, "prefix": prefix})

    keys = list_s3_files_with_prefix(CONFIG["write_bucket"], prefix)

    if not keys:
        log("WARNING", "No raw FX parquet files found for replay",
            {"prefix": prefix})
        return

    log("INFO", "Replay partitions found", {"count": len(keys)})

    for batch_num, key in enumerate(keys, start=1):
        try:
            pdf = read_parquet_from_s3(CONFIG["write_bucket"], key)
            if pdf.empty:
                continue

            # Synthesise Kafka metadata columns absent from raw files
            if "topic"     not in pdf.columns:
                pdf["topic"]     = CONFIG["topic"]
            if "partition" not in pdf.columns:
                pdf["partition"] = 0
            if "offset"    not in pdf.columns:
                pdf["offset"]    = batch_num * 10000   # synthetic, non-colliding

            log("INFO", "Replaying FX S3 partition",
                {"key": key, "rows": len(pdf), "batch_num": batch_num})

            class _MockBatch:
                """Adapter giving process_batch the same interface as a Spark DataFrame."""
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

    log("INFO", "FX S3 replay complete",
        {"date": date_str, "files_processed": len(keys)})

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
# NOTE: Removed data.timestamp field — using ONLY event_time for market time
# This eliminates duplicate timestamp confusion.

EVENT_SCHEMA = StructType([
    StructField("event_id",          StringType(), True),
    StructField("event_type",        StringType(), True),
    StructField("schema_version",    StringType(), True),
    StructField("pipeline_name",     StringType(), True),   # producer pipeline
    StructField("data_source",       StringType(), True),
    StructField("producer_run_id",   StringType(), True),   # producer lineage
    StructField("batch_id",          StringType(), True),   # producer batch
    StructField("source_fetch_time", StringType(), True),   # fetch timestamp
    StructField("event_time",        StringType(), True),   # PRIMARY — market timestamp
    StructField("ingested_at",       StringType(), True),   # producer ingestion
    StructField("data", StructType([
        StructField("currency_pair", StringType(), True),
        StructField("open",          FloatType(),  True),
        StructField("high",          FloatType(),  True),
        StructField("low",           FloatType(),  True),
        StructField("close",         FloatType(),  True),
        StructField("date",          StringType(), True),   # partition date
        # NOTE: data.timestamp REMOVED — use event_time instead
    ])),
])

# =============================================================
# ENTRY POINT
# =============================================================

if __name__ == "__main__":

    # Load FX universe once at startup (read-only)
    fx_universe, pair_groups = load_fx_universe()

    # Rebuild rolling state from S3 BEFORE processing any new data
    log("INFO", "Rebuilding FX state from S3 before starting pipeline")
    rebuild_state_from_s3()
    log("INFO", "FX state rebuild complete — starting pipeline",
        {"pairs_in_state": len(pair_buffers)})

    replay_mode = CONFIG["replay_mode"].lower()
    log("INFO", "FX consumer starting", {"replay_mode": replay_mode})

    if replay_mode == "s3":
        # ── S3 Replay mode — process raw parquet directly ───────────────
        load_s3_replay_partitions(
            date_str     = CONFIG["replay_date"],
            hour_str     = CONFIG["replay_hour"],
            pair_groups  = pair_groups,
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

        # FIX 2: Removed data.timestamp field — using ONLY event_time
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
                "event_time",                      # PRIMARY market timestamp
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
                          process_batch(batch_df, batch_id, pair_groups))
            .outputMode("append")
            .option("checkpointLocation", CONFIG["checkpoint_dir"])
            .start()
        )

        query.awaitTermination()