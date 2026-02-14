import os
import json
import tempfile
import boto3
import pandas as pd
import numpy as np
from io import BytesIO
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, explode
from pyspark.sql.types import StructType, StructField, StringType, FloatType, ArrayType
import redis

# =================================================
# REDIS
# =================================================
redis_client = redis.Redis(
    host=os.getenv("REDIS_HOST"),  
    port=int(os.getenv("REDIS_PORT", 6379)),
    db=int(os.environ.get("REDIS_DB_STREAM", 1)),      
    decode_responses=True
)

# =====================================================
# S3 LOAD STATIC POSITIONS
# =====================================================
s3 = boto3.client("s3")

BUCKET = "pushparag-equity-bucket"
INTRADAY_BUCKET = "pushparag-risk-analytics"
FINAL_MERGED_KEY = "historical-equity/final_merged.parquet"

obj = s3.get_object(Bucket=BUCKET, Key=FINAL_MERGED_KEY)
positions_df = pd.read_parquet(BytesIO(obj["Body"].read()))

cols_to_drop = [
    "option_type", "Discretion", "other_manager",
    "Sole", "Shared", "None", "beta",
    "dividendYield", "gdp", "unrate", "cpi", "fedfunds", "date"
]

positions_df = positions_df.drop(columns=[c for c in cols_to_drop if c in positions_df.columns])

# =====================================================
# CONFIG
# =====================================================
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")

TOPIC = "equity_stream"

OUTPUT_DIR = f"s3a://{INTRADAY_BUCKET}/equity/data"
CHECKPOINT_DIR = f"s3a://{INTRADAY_BUCKET}/equity/checkpoints"

MAX_BUFFER = 16

# =====================================================
# SPARK SESSION
# =====================================================
spark = (
    SparkSession.builder
        .appName("EquityKafkaStreaming_IB_PerTicker_Fixed")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.sql.shuffle.partitions", "4")
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

ticker_schema = StructType([
    StructField("ticker", StringType(), True),
    StructField("open", FloatType(), True),
    StructField("high", FloatType(), True),
    StructField("low", FloatType(), True),
    StructField("close", FloatType(), True),
    StructField("volume", FloatType(), True),
    StructField("timestamp", StringType(), True),
    StructField("date", StringType(), True)
])

json_df = raw_df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), ArrayType(ticker_schema)).alias("tickers")) \
    .select(explode("tickers").alias("ticker_data")) \
    .select("ticker_data.*") \
    .filter(col("volume") > 0)

# =====================================================
# EXISTING BUFFER + LOGIC (UNCHANGED)
# =====================================================
ticker_buffers = {}

def save_to_parquet(df, batch_id):
    if df.empty:
        return

    ts = pd.to_datetime(df['timestamp'].iloc[0])
    partition_date = ts.strftime("%Y-%m-%d")
    partition_hour = ts.strftime("%H")

    partition_path = f"{OUTPUT_DIR}/date={partition_date}/hour={partition_hour}/batch_{batch_id}.parquet"
    df.to_parquet(partition_path, index=False)
    print(f"[S3] Saved batch {batch_id} -> {partition_path}")

def enrich_intraday_with_positions(data_frame):
    df = data_frame.merge(positions_df, on="ticker", how="inner")
    df["intraday_exposure"] = df["Shares"] * df["close"]

    df["intraday_pnl"] = np.where(
        df["close_diff"].notna(),
        df["Shares"] * df["close_diff"],
        np.nan
    )

    df["portfolio_intraday_pnl"] = (
        df.groupby("asset_manager")["intraday_pnl"].transform("sum")
    )

    df["portfolio_intraday_exposure"] = (
        df.groupby("asset_manager")["intraday_exposure"].transform("sum")
    )

    return df

def save_latest_snapshot_all_tickers(ticker_buffers):
    latest_rows = []

    for ticker, buffer in ticker_buffers.items():
        if len(buffer) == 0:
            continue

        latest_row = buffer[-1].copy()

        if len(buffer) >= 2:
            prev = buffer[-2]
            latest_row.update({
                "prev_open": prev.get("open", np.nan),
                "prev_high": prev.get("high", np.nan),
                "prev_low": prev.get("low", np.nan),
                "prev_close": prev.get("close", np.nan),
                "prev_volume": prev.get("volume", np.nan)
            })
        else:
            latest_row.update({
                "prev_open": np.nan,
                "prev_high": np.nan,
                "prev_low": np.nan,
                "prev_close": np.nan,
                "prev_volume": np.nan
            })

        latest_rows.append(latest_row)

    if not latest_rows:
        return

    df_latest = pd.DataFrame(latest_rows)
    df_latest = enrich_intraday_with_positions(df_latest)
    df_latest = df_latest.where(pd.notnull(df_latest), None)
    snapshot_json = df_latest.to_json(orient="records")

    redis_client.set("equity_latest_snapshot", snapshot_json)
    redis_client.publish("equity_stream", snapshot_json)

    print(f"[Redis] published latest snapshot | {len(latest_rows)} tickers")

def compute_metrics(buffer):
    closes = np.array([row['close'] for row in buffer])
    highs = np.array([row['high'] for row in buffer])
    lows = np.array([row['low'] for row in buffer])
    volumes = np.array([row['volume'] for row in buffer])

    metrics = {
        "return_1m": np.nan,
        "return_5m": np.nan,
        "vol_15m": np.nan,
        "range_pct_1m": np.nan,
        "rolling_vwap_5m": np.nan,
        "close_diff": np.nan,
        "rolling_high_5m": np.nan,
        "rolling_low_5m": np.nan,
        "trend_slope_5m": np.nan,
        "breakout_strength": np.nan,
        "volume_burst": np.nan,
    }

    if len(closes) >= 2:
        metrics["close_diff"] = closes[-1] - closes[-2]
        metrics["return_1m"] = (closes[-1] - closes[-2]) / closes[-2]
        metrics["range_pct_1m"] = (highs[-1] - lows[-1]) / closes[-2]

    if len(closes) >= 6:
        metrics["return_5m"] = (closes[-1] - closes[-6]) / closes[-6]
        metrics["trend_slope_5m"] = (closes[-1] - closes[-6]) / 5

    if len(closes) >= 16:
        r = (closes[1:] - closes[:-1]) / closes[:-1]
        log_r = np.log1p(r)
        metrics["vol_15m"] = np.std(log_r[-15:])

    if len(closes) >= 5:
        metrics["rolling_vwap_5m"] = np.sum(closes[-5:] * volumes[-5:]) / np.sum(volumes[-5:])
        metrics["rolling_high_5m"] = float(np.max(highs[-5:]))
        metrics["rolling_low_5m"] = float(np.min(lows[-5:]))

        high5 = np.max(highs[-5:])
        low5 = np.min(lows[-5:])
        rng = high5 - low5
        metrics["breakout_strength"] = ((closes[-1] - low5) / rng) if rng != 0 else None

    if len(volumes) >= 6:
        avg5 = np.mean(volumes[-6:-1])
        metrics["volume_burst"] = (volumes[-1] / avg5) if avg5 != 0 else None

    return metrics

def process_batch(batch_df, batch_id):
    global ticker_buffers
    if batch_df.count() == 0:
        return

    pdf = batch_df.toPandas()
    snapshot_rows = []

    for _, row in pdf.iterrows():
        ticker = row['ticker']
        row_dict = row.to_dict()

        if ticker not in ticker_buffers:
            ticker_buffers[ticker] = []

        ticker_buffers[ticker].append(row_dict)
        if len(ticker_buffers[ticker]) > MAX_BUFFER:
            ticker_buffers[ticker].pop(0)

        metrics = compute_metrics(ticker_buffers[ticker])
        row_dict.update(metrics)
        snapshot_rows.append(row_dict)

    snapshot_df = pd.DataFrame(snapshot_rows)
    save_to_parquet(snapshot_df, batch_id)
    save_latest_snapshot_all_tickers(ticker_buffers)

# =====================================================
# START STREAM  (CONSISTENT WITH FX)
# =====================================================
query = json_df.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("append") \
    .option("checkpointLocation", CHECKPOINT_DIR) \
    .start()

query.awaitTermination()
