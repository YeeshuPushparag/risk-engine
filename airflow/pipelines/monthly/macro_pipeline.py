import os
import time
import pandas as pd
import boto3
import fredapi
from fredapi import Fred
from io import StringIO

# ============================================================
# CONFIG
# ============================================================
S3_BUCKET = "yeeshu-loan-bucket"
S3_KEY = "macro_data.csv"

s3 = boto3.client("s3")


# ============================================================
# DATE WINDOW
# ============================================================
def get_window(years=2):
    today = pd.Timestamp.today().normalize()

    end_date = today + pd.offsets.MonthEnd(0)
    start_date = end_date - pd.DateOffset(years=years)

    return start_date.date(), end_date.date()


# ============================================================
# FRED RETRY WRAPPER
# ============================================================
def get_series_safe(
    fred,
    series_id,
    start_date,
    end_date,
    max_retries=5
):

    for attempt in range(1, max_retries + 1):

        try:

            print(
                f"[FRED] {series_id} "
                f"attempt={attempt} "
                f"window={start_date}->{end_date}"
            )

            data = fred.get_series(
                series_id,
                start=start_date,
                end=end_date
            )

            print(
                f"[FRED SUCCESS] {series_id} "
                f"rows={len(data)}"
            )

            return data

        except Exception as e:

            print(
                f"[FRED ERROR] {series_id} "
                f"attempt={attempt}"
            )

            print(repr(e))

            if "429" in str(e) or "Too Many Requests" in str(e):

                wait = attempt * 15

                print(
                    f"[RATE LIMIT] sleeping {wait}s"
                )

                time.sleep(wait)

                continue

            raise

    raise RuntimeError(
        f"Failed fetching {series_id} after {max_retries} retries"
    )


# ============================================================
# MAIN PIPELINE
# ============================================================
def fetch_macro_data():

    start_date, end_date = get_window(2)

    print(
        f"[MACRO PIPELINE] "
        f"{start_date} -> {end_date}"
    )

    # --------------------------------------------------------
    # DEBUG
    # --------------------------------------------------------
    key = os.getenv("FRED_API_KEY")

    print(
        f"[DEBUG] fredapi={fredapi.__version__}"
    )

    print(
        f"[DEBUG] key_present={key is not None}"
    )

    if key:
        print(
            f"[DEBUG] key_prefix={key[:8]}"
        )

    fred = Fred(api_key=key)

    # --------------------------------------------------------
    # SIMPLE CONNECTIVITY TEST
    # --------------------------------------------------------
    try:

        print("[TEST] GDP no filters")

        test = fred.get_series("GDP")

        print(
            f"[TEST SUCCESS] rows={len(test)}"
        )

    except Exception as e:

        print(
            "[TEST FAILED]"
        )

        print(repr(e))

        raise

    # --------------------------------------------------------
    # FETCH SERIES
    # --------------------------------------------------------
    gdp = get_series_safe(
        fred,
        "GDP",
        start_date,
        end_date
    )

    unrate = get_series_safe(
        fred,
        "UNRATE",
        start_date,
        end_date
    )

    cpi = get_series_safe(
        fred,
        "CPIAUCSL",
        start_date,
        end_date
    )

    fedfunds = get_series_safe(
        fred,
        "FEDFUNDS",
        start_date,
        end_date
    )

    # --------------------------------------------------------
    # MONTHLY INDEX
    # --------------------------------------------------------
    monthly_index = pd.date_range(
        start=start_date,
        end=end_date,
        freq="ME"
    )

    # --------------------------------------------------------
    # GDP
    # --------------------------------------------------------
    gdp = gdp.to_frame("gdp")
    gdp.index = pd.to_datetime(gdp.index)

    gdp_m = (
        gdp.resample("ME").ffill()
        .reindex(monthly_index)
        .ffill()
        .bfill()
    )

    # --------------------------------------------------------
    # MONTHLY SERIES
    # --------------------------------------------------------
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

    # --------------------------------------------------------
    # FINAL DF
    # --------------------------------------------------------
    df = pd.DataFrame({
        "date": monthly_index,
        "gdp": gdp_m["gdp"].values,
        "unrate": unrate_m.values,
        "cpi": cpi_m.values,
        "fedfunds": fedfunds_m.values
    })

    # --------------------------------------------------------
    # S3 WRITE
    # --------------------------------------------------------
    buffer = StringIO()

    df.to_csv(
        buffer,
        index=False
    )

    buffer.seek(0)

    s3.put_object(
        Bucket=S3_BUCKET,
        Key=S3_KEY,
        Body=buffer.getvalue()
    )

    print(
        f"[SUCCESS] "
        f"s3://{S3_BUCKET}/{S3_KEY}"
    )

    return df


# ============================================================
# AIRFLOW ENTRYPOINT
# ============================================================
def run_macro_pipeline(**context):
    return fetch_macro_data()