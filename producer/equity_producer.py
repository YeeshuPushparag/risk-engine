import json
import time
import signal
import sys
import yfinance as yf
import pandas as pd
import boto3
from io import StringIO
from kafka import KafkaProducer
import os

TOPIC_NAME = "equity_stream"
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")


running = True

# =================================================
# Graceful Stop (for Kubernetes / Docker)
# =================================================
def stop_signal(signum, frame):
    global running
    print("Stop signal received. Stopping Equity Producer...")
    running = False

signal.signal(signal.SIGTERM, stop_signal)
signal.signal(signal.SIGINT, stop_signal)

# =================================================
# Load tickers from S3
# =================================================
s3 = boto3.client("s3")
bucket_name = "pushparag-equity-bucket"
key = "historical-equity/tickers50.csv"

csv_obj = s3.get_object(Bucket=bucket_name, Key=key)
body = csv_obj["Body"].read().decode("utf-8")
TICKERS = pd.read_csv(StringIO(body))["ticker"].tolist()

print(f"Loaded {len(TICKERS)} tickers from S3")

# ==================================================
# Kafka Producer
# ==================================================
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# ==================================================
# Fetch Live 1-Minute OHLCV Snapshot
# ==================================================
def fetch_latest_market_snapshot():
    market_df = yf.download(
        tickers=" ".join(TICKERS),
        period="1d",
        interval="1m",
        group_by="ticker",
        threads=True
    )

    batch_list = []

    for ticker in TICKERS:
        if ticker not in market_df.columns.get_level_values(0):
            print(f" Missing: {ticker}")
            continue

        sub = market_df[ticker].dropna()
        sub = sub.iloc[:-1]

        if sub.empty:
            print(f" No valid rows for: {ticker}")
            continue

        row = sub.iloc[-1]
        ts = row.name

        batch_list.append({
            "ticker": ticker,
            "open": float(row["Open"]),
            "high": float(row["High"]),
            "low": float(row["Low"]),
            "close": float(row["Close"]),
            "volume": float(row["Volume"]),
            "timestamp": ts.isoformat(),
            "date": ts.date().isoformat()
        })

    return batch_list if batch_list else None

# ==================================================
# Main Loop (1-minute streaming)
# ==================================================
if __name__ == "__main__":
    print("Equity Kafka Producer Started")

    while running:
        snapshot = fetch_latest_market_snapshot()

        if snapshot:
            producer.send(TOPIC_NAME, snapshot)
            producer.flush()
            print(f"Sent snapshot with {len(snapshot)} tickers")
        else:
            print("No data to send")

        time.sleep(60)

    print("Equity Producer stopped cleanly")
    producer.close()
