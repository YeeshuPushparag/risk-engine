import os
import pandas as pd
import boto3
from io import StringIO
from fredapi import Fred

# ============================================================
# CONFIG
# ============================================================
S3_BUCKET = "yeeshu-loan-bucket"
S3_KEY = "macro_data.csv"

s3 = boto3.client("s3")


# ============================================================
# DATE WINDOW (2 YEARS, MONTH-END ANCHORED)
# ============================================================
def get_window(years=2):
    today = pd.Timestamp.today().normalize()

    # always last day of current month
    end_date = today + pd.offsets.MonthEnd(0)

    # 2-year lookback
    start_date = end_date - pd.DateOffset(years=years)

    return start_date.date(), end_date.date()


# ============================================================
# MAIN PIPELINE
# ============================================================
def fetch_macro_data():

    # --------------------------
    # WINDOW
    # --------------------------
    start_date, end_date = get_window(2)

    print(f"[MACRO PIPELINE] {start_date} -> {end_date}")

    # --------------------------
    # FRED INIT (AWS STYLE)
    # --------------------------
    fred = Fred(api_key=os.getenv("FRED_API_KEY"))

    # --------------------------
    # FETCH SERIES
    # --------------------------
    gdp = fred.get_series(
        "GDP",
        start=start_date,
        end=end_date
    )

    unrate = fred.get_series(
        "UNRATE",
        start=start_date,
        end=end_date
    )

    cpi = fred.get_series(
        "CPIAUCSL",
        start=start_date,
        end=end_date
    )

    fedfunds = fred.get_series(
        "FEDFUNDS",
        start=start_date,
        end=end_date
    )

    # --------------------------
    # MONTHLY INDEX
    # --------------------------
    monthly_index = pd.date_range(start=start_date, end=end_date, freq="ME")

    # ============================================================
    # GDP (Quarterly -> Monthly)
    # ============================================================
    gdp = gdp.to_frame("gdp")
    gdp.index = pd.to_datetime(gdp.index)

    gdp_m = (
        gdp.resample("ME").ffill()
        .reindex(monthly_index)
        .ffill()
        .bfill()
    )

    # ============================================================
    # MONTHLY SERIES (clean + safe fill)
    # ============================================================
    unrate_m = (
        unrate.resample("ME").last()
        .reindex(monthly_index)
        .ffill()
        .bfill()
    )

    cpi_m = (
        cpi.resample("ME").last()
        .reindex(monthly_index)
        .ffill()
        .bfill()
    )

    fedfunds_m = (
        fedfunds.resample("ME").last()
        .reindex(monthly_index)
        .ffill()
        .bfill()
    )

    # ============================================================
    # FINAL DATAFRAME
    # ============================================================
    df = pd.DataFrame({
        "date": monthly_index,
        "gdp": gdp_m["gdp"].values,
        "unrate": unrate_m.values,
        "cpi": cpi_m.values,
        "fedfunds": fedfunds_m.values
    })

    # ============================================================
    # WRITE TO S3 (OVERWRITE - NO INCREMENTAL)
    # ============================================================
    buffer = StringIO()
    df.to_csv(buffer, index=False)
    buffer.seek(0)

    s3.put_object(
        Bucket=S3_BUCKET,
        Key=S3_KEY,
        Body=buffer.getvalue()
    )

    print(f"[SUCCESS] Uploaded -> s3://{S3_BUCKET}/{S3_KEY}")

    return df


# ============================================================
# AIRFLOW ENTRYPOINT
# ============================================================
def run_macro_pipeline(**context):
    return fetch_macro_data()