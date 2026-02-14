import os
import json
import redis
import boto3
import pandas as pd
import numpy as np
from io import BytesIO
from collections import deque

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, explode
from pyspark.sql.types import (
    StructType, StructField,
    StringType, FloatType, ArrayType
)

# =====================================================
# CONFIG
# =====================================================
BUCKET = "pushparag-fx-bucket"
INTRADAY_BUCKET= "pushparag-risk-analytics"
FINAL_MERGED_KEY = "historical-fx/final_merged.parquet"

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")

TOPIC = "fx_ohlc_1m"

OUTPUT_DIR = f"s3a://{INTRADAY_BUCKET}/fx/data"
CHECKPOINT_DIR = f"s3a://{INTRADAY_BUCKET}/fx/checkpoints"


MAX_BUFFER = 60
Z_95 = 1.65
TRADING_DAYS_PER_Q = 63

# =====================================================
# CONNECTIONS
# =====================================================
s3 = boto3.client("s3")

redis_client = redis.Redis(
    host=os.getenv("REDIS_HOST"),
    port=int(os.getenv("REDIS_PORT", 6379)), 
    db=int(os.environ.get("REDIS_DB_STREAM", 1)),     
    decode_responses=True
)

# =====================================================
# HELPERS
# =====================================================
def nz(x):
    if x is None:
        return 0.0
    try:
        if isinstance(x, float) and (np.isnan(x) or np.isinf(x)):
            return 0.0
        return float(x)
    except Exception:
        return 0.0

def json_safe(x):
    if x is None:
        return None
    if isinstance(x, float) and (np.isnan(x) or np.isinf(x)):
        return None
    return x

# =====================================================
# STATIC LOOKUPS
# =====================================================
SECTOR_TO_PAIR = {
    "Technology": "USDEUR",
    "Healthcare": "USDCHF",
    "Consumer Cyclical": "USDEUR",
    "Financial Services": "USDJPY",
    "Consumer Defensive": "USDCAD",
    "Utilities": "USDGBP",
    "Basic Materials": "USDAUD",
    "Industrials": "USDCNY",
    "Real Estate": "USDJPY",
    "Energy": "USDCAD",
    "Communication Services": "USDEUR",
}

FOREIGN_RATIO = {
    "USDEUR": 0.45, "USDCHF": 0.30, "USDJPY": 0.25,
    "USDCAD": 0.33, "USDGBP": 0.10,
    "USDAUD": 0.40, "USDCNY": 0.35,
}

# =====================================================
# LOAD UNIVERSE
# =====================================================
obj = s3.get_object(Bucket=BUCKET, Key=FINAL_MERGED_KEY)
universe = pd.read_parquet(BytesIO(obj["Body"].read()))

universe = universe[["ticker", "sector", "revenue"]].drop_duplicates()
universe["currency_pair"] = universe["sector"].map(SECTOR_TO_PAIR)
universe["foreign_revenue_ratio"] = universe["currency_pair"].map(FOREIGN_RATIO)
universe = universe.dropna()

universe["daily_exposure"] = (
    universe["revenue"] / TRADING_DAYS_PER_Q
) * universe["foreign_revenue_ratio"]

PAIR_GROUPS = {p: df for p, df in universe.groupby("currency_pair")}

# =====================================================
# SPARK SESSION
# =====================================================
spark = (
    SparkSession.builder
        .appName("FXKafkaStreaming")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .getOrCreate()
)


spark.sparkContext.setLogLevel("WARN")

# =====================================================
# KAFKA STREAM
# =====================================================
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", TOPIC) \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

# =====================================================
# MESSAGE SCHEMA
# =====================================================
bar_schema = StructType([
    StructField("currency_pair", StringType(), True),
    StructField("open", FloatType(), True),
    StructField("high", FloatType(), True),
    StructField("low", FloatType(), True),
    StructField("close", FloatType(), True),
])

msg_schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("bars", ArrayType(bar_schema), True),
])

json_df = raw_df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), msg_schema).alias("data")) \
    .select(col("data.timestamp").alias("timestamp"), explode("data.bars").alias("bar")) \
    .select(
        col("timestamp"),
        col("bar.currency_pair"),
        col("bar.open"),
        col("bar.high"),
        col("bar.low"),
        col("bar.close")
    )

# =====================================================
# FX BUFFERS
# =====================================================
pair_buffers = {}

# =====================================================
# METRICS
# =====================================================
def compute_fx_metrics(buffer):
    closes = np.array([b["close"] for b in buffer])

    out = {
        "fx_return_1m": 0.0,
        "fx_return_5m": 0.0,
        "fx_vol_15m": 0.0,
    }

    if len(closes) >= 2:
        out["fx_return_1m"] = (closes[-1] - closes[-2]) / closes[-2]

    if len(closes) >= 6:
        out["fx_return_5m"] = (closes[-1] - closes[-6]) / closes[-6]

    if len(closes) >= 16:
        r = np.diff(closes) / closes[:-1]
        out["fx_vol_15m"] = np.std(np.log1p(r[-15:]))

    return out

# =====================================================
# PROCESS EACH BATCH
# =====================================================
def process_batch(batch_df, batch_id):
    if batch_df.count() == 0:
        return

    pdf = batch_df.toPandas()
    minute_rows = []

    ts = pdf["timestamp"].iloc[0]

    # ---------------- FX PROCESSING ----------------
    for _, bar in pdf.iterrows():
        pair = bar["currency_pair"]
        if pair not in PAIR_GROUPS:
            continue

        if pair not in pair_buffers:
            pair_buffers[pair] = deque(maxlen=MAX_BUFFER)

        pair_buffers[pair].append({
            "open": bar["open"],
            "high": bar["high"],
            "low": bar["low"],
            "close": bar["close"],
        })

        metrics = compute_fx_metrics(pair_buffers[pair])

        for _, row in PAIR_GROUPS[pair].iterrows():
            minute_rows.append({
                "timestamp": ts,
                "ticker": row["ticker"],
                "currency_pair": pair,
                "open": bar["open"],
                "high": bar["high"],
                "low": bar["low"],
                "close": bar["close"],
                "fx_return_1m": nz(metrics["fx_return_1m"]),
                "fx_return_5m": nz(metrics["fx_return_5m"]),
                "fx_vol_15m": nz(metrics["fx_vol_15m"]),
                "position_size": nz(row["daily_exposure"]),
                "fx_pnl": nz(row["daily_exposure"] * metrics["fx_return_1m"]),
                "VaR_95_15m": nz(row["daily_exposure"] * metrics["fx_vol_15m"] * Z_95),
            })

    # ---------------- REDIS SNAPSHOT (UNCHANGED) ----------------
    latest_rows = []

    for pair, buffer in pair_buffers.items():
        if pair not in PAIR_GROUPS or not buffer:
            continue

        curr = buffer[-1]
        prev = buffer[-2] if len(buffer) >= 2 else None
        metrics = compute_fx_metrics(buffer)

        for _, row in PAIR_GROUPS[pair].iterrows():
            latest_rows.append({
                "timestamp": ts,
                "ticker": row["ticker"],
                "currency_pair": pair,
                "open": curr["open"],
                "high": curr["high"],
                "low": curr["low"],
                "close": curr["close"],
                "prev_open": prev["open"] if prev else None,
                "prev_high": prev["high"] if prev else None,
                "prev_low": prev["low"] if prev else None,
                "prev_close": prev["close"] if prev else None,
                "fx_return_1m": nz(metrics["fx_return_1m"]),
                "fx_return_5m": nz(metrics["fx_return_5m"]),
                "fx_vol_15m": nz(metrics["fx_vol_15m"]),
                "position_size": nz(row["daily_exposure"]),
                "fx_pnl": nz(row["daily_exposure"] * metrics["fx_return_1m"]),
                "VaR_95_15m": nz(row["daily_exposure"] * metrics["fx_vol_15m"] * Z_95),
            })

    safe_rows = [{k: json_safe(v) for k, v in r.items()} for r in latest_rows]
    redis_client.set("fx_latest_snapshot", json.dumps(safe_rows))
    redis_client.publish("fx_stream", json.dumps(safe_rows))
    print(f"[Redis] published latest snapshot | {len(safe_rows)} tickers")
    # ---------------- AWS S3 SAVE ----------------
    if minute_rows:
        df_out = pd.DataFrame(minute_rows)

        # timestamp format: YYYY-MM-DDTHH:MM:SS+00:00
        date_part = ts[:10]
        hour_part = ts[11:13]

        path = f"{OUTPUT_DIR}/date={date_part}/hour={hour_part}/fx_intraday_batch_{batch_id}.parquet"

        df_out.to_parquet(path, index=False)

        print(f"[S3] Saved FX batch {batch_id} -> {path}")



# =====================================================
# START STREAM
# =====================================================
query = json_df.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("append") \
    .option("checkpointLocation", CHECKPOINT_DIR) \
    .start()

query.awaitTermination()
