import pandas as pd
import numpy as np
import yfinance as yf
import boto3
from tqdm import tqdm
from datetime import datetime, timedelta
from io import StringIO


def run_commodities_update(
    s3_bucket="pushparag-commodity-bucket",
    s3_key="commodities_daily.csv",
    tickers=None,
    roll_window=60
):
    """
    Incrementally fetch commodity data, compute rolling features, and update file in S3.
    Returns status string for Airflow.
    """
    if tickers is None:
        tickers = ["GC=F", "CL=F", "SI=F", "NG=F", "ZC=F"]

    today = datetime.today().date()

    s3_client = boto3.client('s3')

    # Load existing CSV from S3
    try:
        obj = s3_client.get_object(Bucket=s3_bucket, Key=s3_key)
        existing = pd.read_csv(obj['Body'])
        existing["date"] = pd.to_datetime(existing["date"])
        last_date = existing["date"].max().date()
        if last_date >= today:
            return "NO_UPDATE_NEEDED"
    except Exception:
        existing = pd.DataFrame()
        last_date = None

    start_date = last_date if last_date else datetime(2025, 1, 1).date()
    end_date = today + timedelta(days=1)

    data = yf.download(
        tickers,
        start=start_date.strftime("%Y-%m-%d"),
        end=end_date.strftime("%Y-%m-%d"),
        group_by="ticker",
        progress=False,
    )

    records = []

    for tkr in tqdm(tickers, desc="Processing", ncols=100):
        df_new = data[tkr].reset_index()
        df_new.columns = ["date", "open", "high", "low", "close", "volume"]
        df_new["commodity_symbol"] = tkr
        df_new = df_new.dropna(subset=["close"]).sort_values("date")
        df_new["date"] = pd.to_datetime(df_new["date"])
        # Remove rows equal to last_date if present
        if last_date:
            df_new = df_new[df_new["date"].dt.date > last_date]

        # === If today's data does not exist, fill with 3-day average ===
        if df_new.empty or df_new["date"].dt.date.max() < today:
            print(f"Today's data missing for {tkr}. Filling with last 3-day averages...")

            prev = existing[existing["commodity_symbol"] == tkr].sort_values("date").tail(3)

            if not prev.empty:
                avg_row = {
                    "date": pd.Timestamp(today),
                    "open": round(prev["open"].mean(), 2),
                    "high": round(prev["high"].mean(), 2),
                    "low": round(prev["low"].mean(), 2),
                    "close": round(prev["close"].mean(), 2),
                    "volume": round(prev["volume"].mean(), 2),
                    "commodity_symbol": tkr
                }

                df_new = pd.concat([df_new, pd.DataFrame([avg_row])], ignore_index=True)
            else:
                print(f"Not enough history to synthesize today's row for {tkr}. Skipping.")
                continue
        # Exclude weekends
        df_new = df_new[df_new["date"].dt.weekday < 5]  # weekdays are 0 to 4 (Mon-Fri)
        if not existing.empty:
            prev_rows = (
                existing[existing["commodity_symbol"] == tkr]
                .sort_values("date")
                .tail(roll_window)
            )
            df = pd.concat([prev_rows, df_new], ignore_index=True)
        else:
            df = df_new.copy()

        # Rolling features (unchanged)
        df["daily_return"] = df["close"].pct_change()
        df["log_return"] = np.log(df["close"] / df["close"].shift(1))
        df["vol_20d"] = df["daily_return"].rolling(20).std() * np.sqrt(252)
        df["VaR_95"] = df["daily_return"].rolling(roll_window).quantile(0.05)
        df["VaR_99"] = df["daily_return"].rolling(roll_window).quantile(0.01)
        df["pnl"] = df["close"].diff()

        # Keep only rows after last_date to append to CSV
        if last_date:
            df = df[df["date"].dt.date > last_date]
            
        records.append(df)

    if not records:
        return "NO_NEW_DATA"

    new_data_to_save = (
        pd.concat(records, ignore_index=True)
        .dropna(subset=["daily_return"])
    )

    if not existing.empty:
        final = pd.concat([existing, new_data_to_save], ignore_index=True)
    else:
        final = new_data_to_save

    final["date"] = pd.to_datetime(final["date"]).dt.strftime("%Y-%m-%d")

    csv_buffer = StringIO()
    final.to_csv(csv_buffer, index=False)
    s3_client.put_object(Bucket=s3_bucket, Key=s3_key, Body=csv_buffer.getvalue())

    return f"UPDATED_{len(new_data_to_save)}_ROWS"
