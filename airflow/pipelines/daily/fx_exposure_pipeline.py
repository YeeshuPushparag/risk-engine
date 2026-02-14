import pandas as pd
import yfinance as yf
from fredapi import Fred
import os
from datetime import datetime, timedelta
import boto3
from io import BytesIO


FRED_API_KEY = os.getenv("FRED_API_KEY")


# S3 settings — unchanged
BUCKET = "pushparag-fx-bucket"
PREFIX = "historical-fx"
INPUT_KEY = f"{PREFIX}/final_merged.parquet"
OUTPUT_KEY = f"{PREFIX}/fx_exposure_with_interest_diff.parquet"
MACRO_KEY = f"{PREFIX}/macro_data.csv"

s3 = boto3.client("s3")


# -------- S3 utility functions (no prints) -------- #

def read_parquet_s3(bucket, key):
    obj = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_parquet(BytesIO(obj['Body'].read()))

def write_parquet_s3(df, bucket, key):
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    s3.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())

def read_csv_s3(bucket, key):
    obj = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(BytesIO(obj['Body'].read()))


# ======================================================
# Main function for Airflow DAG
# ======================================================
def run_fx_exposure_pipeline():

    today = datetime.today().date()


    # STEP 1: Map ticker to FX exposure
    df = read_parquet_s3(BUCKET, INPUT_KEY)[["ticker", "sector", "industry"]]
    df_map = df.drop_duplicates(subset=["ticker"]).reset_index(drop=True)

    sector_to_currency_pair = {
        "Technology": "USDEUR", "Healthcare": "USDCHF", "Consumer Cyclical": "USDEUR",
        "Financial Services": "USDJPY", "Consumer Defensive": "USDCAD", "Utilities": "USDGBP",
        "Basic Materials": "USDAUD", "Industrials": "USDCNY", "Real Estate": "USDJPY",
        "Energy": "USDCAD", "Communication Services": "USDEUR"
    }

    foreign_ratio = {
        "USDEUR": 0.45, "USDCHF": 0.30, "USDJPY": 0.25,
        "USDCAD": 0.33, "USDGBP": 0.10, "USDAUD": 0.40, "USDCNY": 0.35
    }

    df_map["currency_pair"] = df_map["sector"].map(sector_to_currency_pair)
    df_map["foreign_revenue_ratio"] = df_map["currency_pair"].map(foreign_ratio)

    # STEP 2: Determine new date range
    try:
        old_fx = read_parquet_s3(BUCKET, OUTPUT_KEY)
        old_fx["date"] = pd.to_datetime(old_fx["date"]).dt.date
        last_date = old_fx["date"].max()

        if last_date >= today:
            return "NO_NEW_FX_UPDATE"

        fx_start = last_date + timedelta(days=1)

    except Exception:
        old_fx = pd.DataFrame()
        fx_start = today - timedelta(days=7)  # at least 1 week of context

    fx_end = today + timedelta(days=1)

    # STEP 3: Download FX data
    pairs = df_map["currency_pair"].dropna().unique().tolist()
    yahoo_map = {p: p + "=X" for p in pairs}

    data = yf.download(list(yahoo_map.values()), start=fx_start, end=fx_end)
    if isinstance(data.columns, pd.MultiIndex):
        data.columns = ["_".join(col).strip() for col in data.columns.values]

    close_cols = [c for c in data.columns if "Close" in c]
    fx = data[close_cols].copy()

    for p, ysym in yahoo_map.items():
        for col in fx.columns:
            if ysym in col:
                fx.rename(columns={col: p}, inplace=True)

    fx.reset_index(inplace=True)
    fx.rename(columns={"Date": "date"}, inplace=True)
    fx.ffill(inplace=True)

    fx_long = fx.melt(id_vars=["date"], var_name="currency_pair", value_name="fx_rate")
    company_fx = df_map.merge(fx_long, on="currency_pair", how="left")

    # STEP 4: Interest differential from FRED
    fred = Fred(api_key=FRED_API_KEY)
    six_months_ago = today - timedelta(days=182)

    rate_series = {
        "USD": "FEDFUNDS", "EUR": "ECBDFR", "GBP": "IR3TIB01GBM156N",
        "AUD": "IR3TIB01AUM156N", "CAD": "IR3TIB01CAM156N",
        "JPY": "IR3TIB01JPM156N", "CHF": "IR3TIB01CHM156N", "CNY": "IR3TIB01CNQ156N"
    }

    all_rates = {}
    for cur, sid in rate_series.items():
        try:
            values = fred.get_series(sid, observation_start=six_months_ago, observation_end=today)
            if values is not None and len(values) > 0:
                all_rates[cur] = pd.DataFrame({"date": values.index, cur: values.values})
        except:
            pass

    rates_df = None
    for cur, df_rates in all_rates.items():
        rates_df = df_rates if rates_df is None else rates_df.merge(df_rates, on="date", how="outer")

    rates_df.ffill(inplace=True)
    rates_df.bfill(inplace=True)

    for cur in rate_series:
        if cur != "USD" and cur in rates_df.columns:
            rates_df[f"USD{cur}"] = rates_df["USD"] - rates_df[cur]
    rates_df["USDUSD"] = 0

    rates_df["date"] = pd.to_datetime(rates_df["date"], errors="coerce")
    company_fx["date"] = pd.to_datetime(company_fx["date"], errors="coerce")
    # Exclude weekends (Saturday=5, Sunday=6)
    rates_df = rates_df[rates_df["date"].dt.weekday < 5]
    company_fx = company_fx[company_fx["date"].dt.weekday < 5]

    # Exclude rows already present dates in old df
    rates_df = rates_df[rates_df["date"].dt.date > last_date]
    company_fx = company_fx[company_fx["date"].dt.date > last_date]

    rate_cols = [c for c in rates_df.columns if c.startswith("USD")]
    rates_long = rates_df.melt(
        id_vars=["date"], value_vars=rate_cols,
        var_name="currency_pair", value_name="interest_diff"
    )

    company_fx = company_fx.dropna(subset=["date", "currency_pair"]).sort_values("date")
    rates_long = rates_long.dropna(subset=["date", "currency_pair"]).sort_values("date")

    merged = pd.merge_asof(
        company_fx, rates_long,
        on="date", by="currency_pair", direction="backward"
    )



    # STEP 6: Merge revenue
    funda = read_parquet_s3(BUCKET, INPUT_KEY)[["ticker", "revenue"]]
    funda = funda.drop_duplicates(subset=["ticker"])
    df_final = merged.merge(funda, on="ticker", how="inner")

    # STEP 7: Merge macro data
    macro = read_csv_s3(BUCKET, MACRO_KEY)
    df_final["month"] = df_final["date"].dt.to_period("M")
    macro["month"] = pd.to_datetime(macro["date"]).dt.to_period("M")

    df_final = df_final.merge(
        macro.drop(columns=["date"]),
        on="month",
        how="inner"
    ).drop(columns=["month"])

    # STEP 8: Combine & remove duplicates
    if not old_fx.empty:
        old_fx["date"] = pd.to_datetime(old_fx["date"]).dt.date
        df_final["date"] = pd.to_datetime(df_final["date"]).dt.date
        combined = pd.concat([old_fx, df_final], ignore_index=True)
        combined.drop_duplicates(subset=["date", "ticker", "currency_pair"], inplace=True)
    else:
        combined = df_final

    write_parquet_s3(combined, BUCKET, OUTPUT_KEY)

    return f"UPDATED_ROWS_{len(df_final)}"
