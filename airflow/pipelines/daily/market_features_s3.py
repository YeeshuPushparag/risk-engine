import pandas as pd
import numpy as np
import yfinance as yf
import boto3
from tqdm import tqdm
from datetime import datetime, timedelta
from io import BytesIO


# =============================================================
# S3 HELPERS — REUSABLE IN DAG TASKS
# =============================================================

def read_csv_from_s3(bucket, key):
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(obj["Body"])


def read_parquet_from_s3(bucket, key):
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_parquet(BytesIO(obj["Body"].read()))


def write_parquet_to_s3(df, bucket, key):
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    boto3.client("s3").put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())
    return f"s3://{bucket}/{key}"


# =============================================================
# FEATURE ENGINEERING — NO CHANGE IN LOGIC
# =============================================================

def generate_market_features(df, risk_free_rate=0.05):
    results = []

    for ticker, sub in tqdm(df.groupby("ticker"), desc="Computing features", ncols=100):
        sub = sub.copy()
        sub["daily_return"] = sub["close"].pct_change()
        sub["log_return"] = np.log(sub["close"] / sub["close"].shift(1))
        sub["high_low_spread"] = (sub["high"] - sub["low"]) / sub["open"]
        sub["close_open_diff"] = (sub["close"] - sub["open"]) / sub["open"]
        sub["vol_5d"] = sub["daily_return"].rolling(5, min_periods=2).std() * np.sqrt(252)
        sub["vol_20d"] = sub["daily_return"].rolling(20, min_periods=5).std() * np.sqrt(252)
        sub["volatility_21d"] = sub["daily_return"].rolling(21, min_periods=5).std()
        sub["ma_5"] = sub["close"].rolling(5, min_periods=1).mean()
        sub["ma_20"] = sub["close"].rolling(20, min_periods=1).mean()
        sub["momentum_5d"] = sub["close"].pct_change(5)
        sub["momentum_20d"] = sub["close"].pct_change(20)
        sub["avg_volume_10d"] = sub["volume"].rolling(10, min_periods=1).mean()
        sub["vol_change"] = sub["volume"].pct_change()
        sub["vol_change"] = sub["vol_change"].fillna(0)
        sub["excess_return"] = sub["daily_return"] - (risk_free_rate / 252)
        sub["downside_risk"] = (
            sub["daily_return"].where(sub["daily_return"] < 0)
            .rolling(21, min_periods=5).std()
        )
        sub["sharpe_ratio"] = (
            sub["excess_return"].rolling(21, min_periods=5).mean()
            / sub["volatility_21d"]
        )
        sub["sortino_ratio"] = (
            sub["excess_return"].rolling(21, min_periods=5).mean()
            / sub["downside_risk"]
        )
        sub["downside_risk"] = sub["downside_risk"].fillna(0)
        sub["sortino_ratio"] = sub["sortino_ratio"].fillna(0)
        results.append(sub)

    final = pd.concat(results, ignore_index=True)
    final.replace([np.inf, -np.inf], np.nan, inplace=True)
    final.dropna(subset=["close"], inplace=True)
    return final


# =============================================================
# MAIN LOGIC FOR DAG — PURE FUNCTION
# =============================================================

def update_market_features(input_filename="final_tickers.csv",
                           bucket="pushparag-equity-bucket",
                           prefix="historical-equity/",
                           batch_size=100):
    input_key = prefix + input_filename
    output_key = prefix + "market_features.parquet"

    # Load ticker list
    tickers_df = read_csv_from_s3(bucket, input_key)
    tickers = tickers_df["ticker"].dropna().unique().tolist()

    today = datetime.today().date()


    # Try load existing parquet
    s3 = boto3.client("s3")
    old_df = None
    try:
        s3.head_object(Bucket=bucket, Key=output_key)
        old_df = read_parquet_from_s3(bucket, output_key)
        if not np.issubdtype(old_df["date"].dtype, np.datetime64):
            old_df["date"] = pd.to_datetime(old_df["date"])
        last_date = old_df["date"].max().date()
        if last_date >= today:
            return None, "Already updated"
        start_date = last_date + timedelta(days=1)
    except Exception:
        start_date = today - timedelta(days=365)

    new_records = []
    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i + batch_size]
        data = yf.download(
            tickers=" ".join(batch),
            start=str(start_date),
            end=str(today + timedelta(days=1)),
            interval="1d",
            group_by="ticker",
            auto_adjust=False,
            threads=True
        )
        for t in batch:
            if t not in data.columns.get_level_values(0):
                continue
            sub = data[t].reset_index().rename(columns={
                "Date": "date",
                "Open": "open",
                "High": "high",
                "Low": "low",
                "Close": "close",
                "Volume": "volume"
            })
            if "Adj Close" in sub.columns:
                sub = sub.drop(columns=["Adj Close"])
            sub["ticker"] = t
            new_records.append(sub)

    if not new_records:
        return None, "No new data"

    new_df_raw = pd.concat(new_records, ignore_index=True)
    # Keep column as Timestamp
    new_df_raw["date"] = pd.to_datetime(new_df_raw["date"])
    new_df_raw = new_df_raw[new_df_raw["date"] >= pd.Timestamp(start_date)]
    new_df_raw = new_df_raw[new_df_raw["date"].dt.weekday < 5]
    FEATURE_COLS = ["date", "open", "high", "low", "close", "volume", "ticker"]
    new_df_raw = new_df_raw[FEATURE_COLS]
    # ---------------------------
    # Tail history
    # ---------------------------
    if old_df is not None:
        tail_df = (
            old_df[FEATURE_COLS]
            .sort_values(["ticker", "date"])
            .groupby("ticker", group_keys=False)
            .tail(20)
        )
    else:
        tail_df = pd.DataFrame(columns=FEATURE_COLS)

    feature_input = pd.concat([tail_df, new_df_raw], ignore_index=True)
    feature_input = feature_input.sort_values(["ticker","date"]).reset_index(drop=True)
    # ---------------------------
    # Feature engineering
    # ---------------------------
    print("Computing features...")
    feature_output = generate_market_features(feature_input.copy())


    first_new_date = new_df_raw["date"].min()
    new_features = feature_output[feature_output["date"] >= first_new_date].copy()

    if old_df is not None:
        final_df = pd.concat([old_df, new_features], ignore_index=True)
    else:
        final_df = new_features

    write_parquet_to_s3(final_df, bucket, output_key)
    return final_df, "Success"
