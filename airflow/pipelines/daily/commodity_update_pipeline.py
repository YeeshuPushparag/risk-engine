import pandas as pd
import numpy as np
import yfinance as yf
import boto3
from tqdm import tqdm
from datetime import datetime, timedelta
from io import BytesIO


def run_commodities_update(
    s3_bucket="pushparag-commodity-bucket",
    s3_key="commodities_daily.parquet",
    tickers=None,
    roll_window=60
):
    """
    Incrementally fetch commodity data, compute rolling features,
    and update parquet file in S3.
    Returns status string for Airflow.
    """

    if tickers is None:
        tickers = ["GC=F", "CL=F", "SI=F", "NG=F", "ZC=F"]

    today = datetime.today().date()

    s3_client = boto3.client("s3")

    # =============================
    # LOAD EXISTING PARQUET FROM S3
    # =============================
    try:
        obj = s3_client.get_object(Bucket=s3_bucket, Key=s3_key)
        existing = pd.read_parquet(BytesIO(obj["Body"].read()))

        existing["date"] = pd.to_datetime(existing["date"])
        last_date = existing["date"].max().date()

        if last_date >= today:
            return "NO_UPDATE_NEEDED"

    except Exception:
        existing = pd.DataFrame()
        last_date = None

    # =============================
    # DOWNLOAD NEW DATA
    # =============================
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

    # =============================
    # PROCESS EACH TICKER
    # =============================
    for tkr in tqdm(tickers, desc="Processing", ncols=100):

        df_new = data[tkr].reset_index()
        df_new.columns = ["date", "open", "high", "low", "close", "volume"]

        df_new["commodity_symbol"] = tkr

        df_new = df_new.dropna(subset=["close"]).sort_values("date")

        df_new["date"] = pd.to_datetime(df_new["date"])

        # Remove rows equal to last_date
        if last_date:
            df_new = df_new[df_new["date"].dt.date > last_date]

        # =============================
        # SYNTHETIC ROW IF TODAY MISSING
        # =============================
        if df_new.empty or df_new["date"].dt.date.max() < today:

            print(f"Today's data missing for {tkr}. Filling with last 3-day averages...")

            prev = (
                existing[existing["commodity_symbol"] == tkr]
                .sort_values("date")
                .tail(3)
            )

            if not prev.empty:

                avg_row = {
                    "date": pd.Timestamp(today),
                    "open": round(prev["open"].mean(), 2),
                    "high": round(prev["high"].mean(), 2),
                    "low": round(prev["low"].mean(), 2),
                    "close": round(prev["close"].mean(), 2),
                    "volume": round(prev["volume"].mean(), 2),
                    "commodity_symbol": tkr,
                }

                df_new = pd.concat(
                    [df_new, pd.DataFrame([avg_row])],
                    ignore_index=True,
                )

            else:
                print(f"Not enough history to synthesize today's row for {tkr}. Skipping.")
                continue

        # =============================
        # REMOVE WEEKENDS
        # =============================
        df_new = df_new[df_new["date"].dt.weekday < 5]

        # =============================
        # CONCAT WITH PREVIOUS WINDOW
        # =============================
        if not existing.empty:

            prev_rows = (
                existing[existing["commodity_symbol"] == tkr]
                .sort_values("date")
                .tail(roll_window)
            )

            df = pd.concat([prev_rows, df_new], ignore_index=True)

        else:

            df = df_new.copy()

        # =============================
        # ROLLING FEATURES
        # =============================
        df["daily_return"] = df["close"].pct_change()

        df["log_return"] = np.log(df["close"] / df["close"].shift(1))

        df["vol_20d"] = (
            df["daily_return"].rolling(20).std() * np.sqrt(252)
        )

        df["VaR_95"] = (
            df["daily_return"].rolling(roll_window).quantile(0.05)
        )

        df["VaR_99"] = (
            df["daily_return"].rolling(roll_window).quantile(0.01)
        )

        df["pnl"] = df["close"].diff()

        # Keep only rows after last_date
        if last_date:
            df = df[df["date"].dt.date > last_date]

        records.append(df)

    # =============================
    # NO NEW DATA
    # =============================
    if not records:
        return "NO_NEW_DATA"

    new_data_to_save = (
        pd.concat(records, ignore_index=True)
        .dropna(subset=["daily_return"])
    )

    # =============================
    # APPEND TO EXISTING DATA
    # =============================
    if not existing.empty:

        final = pd.concat(
            [existing, new_data_to_save],
            ignore_index=True
        )

    else:

        final = new_data_to_save

    # Ensure date column stays datetime
    final["date"] = pd.to_datetime(final["date"])

    # =============================
    # SAVE PARQUET TO S3
    # =============================
    buffer = BytesIO()

    final.to_parquet(
        buffer,
        index=False,
        engine="pyarrow"
    )

    buffer.seek(0)

    s3_client.put_object(
        Bucket=s3_bucket,
        Key=s3_key,
        Body=buffer.getvalue()
    )

    return f"UPDATED_{len(new_data_to_save)}_ROWS"

