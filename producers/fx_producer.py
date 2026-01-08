import json
import time
import signal
import yfinance as yf
from kafka import KafkaProducer
from datetime import datetime, timezone
import os

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")

TOPIC = "fx_ohlc_1m"

CURRENCY_PAIRS = [
    "USDEUR", "USDJPY", "USDCAD",
    "USDCHF", "USDGBP", "USDAUD", "USDCNY"
]

running = True

# ==================================================
# Graceful Stop (Docker / Kubernetes Friendly)
# ==================================================
def stop_signal(signum, frame):
    global running
    print("Stop signal received. Stopping FX Producer...")
    running = False

signal.signal(signal.SIGTERM, stop_signal)
signal.signal(signal.SIGINT, stop_signal)

# ==================================================
# Kafka Producer
# ==================================================
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# ==================================================
# Fetch FX OHLC 1-Minute Data
# ==================================================
def fetch_all_fx_1m():
    symbols = [pair + "=X" for pair in CURRENCY_PAIRS]

    data = yf.download(
        tickers=symbols,
        period="1d",
        interval="1m",
        group_by="ticker",
        threads=True,
        progress=False,
    )

    if data.empty:
        return None

    bars = []
    ts = datetime.now(timezone.utc).replace(second=0, microsecond=0)

    for pair, symbol in zip(CURRENCY_PAIRS, symbols):
        if symbol not in data.columns.get_level_values(0):
            continue

        df = data[symbol].dropna()
        if df.empty:
            continue

        row = df.iloc[-1]

        bars.append({
            "currency_pair": pair,
            "open": float(row["Open"]),
            "high": float(row["High"]),
            "low": float(row["Low"]),
            "close": float(row["Close"]),
        })

    if not bars:
        return None

    return {
        "timestamp": ts.isoformat(),
        "bars": bars,
    }

# ==================================================
# Main Loop
# ==================================================
if __name__ == "__main__":
    print("FX Kafka Producer Started")

    while running:
        payload = fetch_all_fx_1m()

        if payload:
            producer.send(TOPIC, payload)
            producer.flush()
            print(f"Sent FX 1m payload with {len(payload['bars'])} pairs")
        else:
            print("No FX data to send")

        time.sleep(60)

    print("FX Producer stopped cleanly")
    producer.close()
