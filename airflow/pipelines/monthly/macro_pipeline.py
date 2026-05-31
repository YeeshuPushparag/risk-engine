import os
import time
import json
import boto3
import pandas as pd
import requests

from io import StringIO
from datetime import datetime
from fredapi import Fred


# ============================================================
# CONFIG
# ============================================================

S3_BUCKET = "yeeshu-loan-bucket"
S3_KEY = "macro_data.csv"

s3 = boto3.client("s3")

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")


# ============================================================
# SLACK ALERTS
# ============================================================

def send_slack_alert(
    message: str,
    status: str,
    context: dict = None,
):

    if not SLACK_WEBHOOK_URL:
        return

    emoji = (
        "✅"
        if status == "SUCCESS"
        else "❌"
    )

    text = (
        f"{emoji} "
        f"*[{status}]* macro_pipeline\n"
        f"message: {message}\n"
    )

    if context:
        text += (
            f"context: "
            f"{json.dumps(context, default=str)}\n"
        )

    text += (
        f"time: "
        f"{datetime.utcnow().isoformat()}"
    )

    try:

        requests.post(
            SLACK_WEBHOOK_URL,
            json={"text": text},
            timeout=5,
        )

    except Exception:
        pass


# ============================================================
# DATE WINDOW
# ============================================================

def get_window(years=2):

    today = pd.Timestamp.today().normalize()

    end_date = (
        today
        + pd.offsets.MonthEnd(0)
    )

    start_date = (
        end_date
        - pd.DateOffset(years=years)
    )

    return (
        start_date.date(),
        end_date.date(),
    )


# ============================================================
# FRED RETRY WRAPPER
# ============================================================

def get_series_safe(
    fred,
    series_id,
    start_date,
    end_date,
    max_retries=5,
):

    for attempt in range(max_retries):

        try:

            return fred.get_series(
                series_id,
                start=start_date,
                end=end_date,
            )

        except Exception as e:

            if (
                "429" in str(e)
                or
                "Too Many Requests" in str(e)
            ):

                wait = 15 * (attempt + 1)

                time.sleep(wait)

                continue

            raise

    raise RuntimeError(
        f"Failed fetching "
        f"{series_id}"
    )


# ============================================================
# MAIN PIPELINE
# ============================================================

def fetch_macro_data():

    pipeline_start = time.time()

    start_date, end_date = get_window(2)

    print(f"\n{'='*66}")
    print(f"  MACRO PIPELINE START")
    print(f"  Date range: {start_date} -> {end_date}")
    print(f"{'='*66}\n")

    fred = Fred(
        api_key=os.getenv("FRED_API_KEY")
    )

    # --------------------------------------------------------
    # FETCH FRED SERIES
    # --------------------------------------------------------
    print("[STEP 1] Fetching FRED data...")

    try:

        gdp = get_series_safe(
            fred,
            "GDP",
            start_date,
            end_date,
        )
        print(f"  GDP fetched: {len(gdp)} observations")

        unrate = get_series_safe(
            fred,
            "UNRATE",
            start_date,
            end_date,
        )
        print(f"  UNRATE fetched: {len(unrate)} observations")

        cpi = get_series_safe(
            fred,
            "CPIAUCSL",
            start_date,
            end_date,
        )
        print(f"  CPI fetched: {len(cpi)} observations")

        fedfunds = get_series_safe(
            fred,
            "FEDFUNDS",
            start_date,
            end_date,
        )
        print(f"  FEDFUNDS fetched: {len(fedfunds)} observations")

    except Exception as e:

        print(f"[FAILED] FRED fetch failed: {e}")
        send_slack_alert(
            message="FRED fetch failed",
            status="FAILURE",
            context={
                "error": str(e),
                "start_date": str(start_date),
                "end_date": str(end_date),
            },
        )
        raise

    # --------------------------------------------------------
    # MONTHLY INDEX
    # --------------------------------------------------------
    print("\n[STEP 2] Processing data to monthly frequency...")

    monthly_index = pd.date_range(
        start=start_date,
        end=end_date,
        freq="ME",
    )
    print(f"  Monthly index created: {len(monthly_index)} months")

    # --------------------------------------------------------
    # GDP QUARTERLY -> MONTHLY
    # --------------------------------------------------------

    gdp = gdp.to_frame("gdp")
    gdp.index = pd.to_datetime(gdp.index)

    gdp_m = (
        gdp.resample("ME")
        .ffill()
        .reindex(monthly_index)
        .ffill()
        .bfill()
    )
    print(f"  GDP processed: quarterly -> monthly")

    # --------------------------------------------------------
    # MONTHLY SERIES
    # --------------------------------------------------------

    unrate_m = (
        unrate.resample("ME")
        .last()
        .reindex(monthly_index)
        .ffill()
        .bfill()
    )
    print(f"  UNRATE processed")

    cpi_m = (
        cpi.resample("ME")
        .last()
        .reindex(monthly_index)
        .ffill()
        .bfill()
    )
    print(f"  CPI processed")

    fedfunds_m = (
        fedfunds.resample("ME")
        .last()
        .reindex(monthly_index)
        .ffill()
        .bfill()
    )
    print(f"  FEDFUNDS processed")

    # --------------------------------------------------------
    # FINAL DATAFRAME
    # --------------------------------------------------------

    df = pd.DataFrame({
        "date": monthly_index,
        "gdp": gdp_m["gdp"].values,
        "unrate": unrate_m.values,
        "cpi": cpi_m.values,
        "fedfunds": fedfunds_m.values,
    })

    print(f"\n  Final DataFrame:")
    print(f"    Rows: {len(df)}")
    print(f"    Date range: {df['date'].min().date()} -> {df['date'].max().date()}")
    print(f"    Columns: {list(df.columns)}")

    # --------------------------------------------------------
    # ATOMIC S3 WRITE
    # --------------------------------------------------------
    print("\n[STEP 3] Writing to S3 (atomic write)...")

    try:

        buffer = StringIO()

        df.to_csv(
            buffer,
            index=False,
        )

        buffer.seek(0)

        temp_key = (
            f"_temp/"
            f"{S3_KEY}."
            f"{int(time.time())}"
        )

        s3.put_object(
            Bucket=S3_BUCKET,
            Key=temp_key,
            Body=buffer.getvalue(),
        )
        print(f"  Temp file written: {temp_key}")

        s3.copy_object(
            Bucket=S3_BUCKET,
            CopySource={
                "Bucket": S3_BUCKET,
                "Key": temp_key,
            },
            Key=S3_KEY,
        )
        print(f"  Copied to final: {S3_KEY}")

        s3.delete_object(
            Bucket=S3_BUCKET,
            Key=temp_key,
        )
        print(f"  Temp file deleted")

        print(f"  Successfully written to s3://{S3_BUCKET}/{S3_KEY}")

    except Exception as e:

        print(f"[FAILED] S3 write failed: {e}")
        send_slack_alert(
            message="S3 write failed",
            status="FAILURE",
            context={
                "error": str(e),
                "bucket": S3_BUCKET,
                "key": S3_KEY,
            },
        )
        raise

    # --------------------------------------------------------
    # SUCCESS ALERT
    # --------------------------------------------------------

    duration = round(
        time.time() - pipeline_start,
        2,
    )

    print(f"\n{'='*66}")
    print(f"  MACRO PIPELINE SUCCESS")
    print(f"  Date range: {start_date} -> {end_date}")
    print(f"  Rows written: {len(df)}")
    print(f"  S3 path: s3://{S3_BUCKET}/{S3_KEY}")
    print(f"  Duration: {duration}s")
    print(f"{'='*66}\n")

    send_slack_alert(
        message=(
            "Macro data pipeline "
            "completed successfully"
        ),
        status="SUCCESS",
        context={
            "rows": len(df),
            "date_range":
                f"{start_date} -> {end_date}",
            "s3_path":
                f"s3://{S3_BUCKET}/{S3_KEY}",
            "duration_s": duration,
        },
    )

    return df


# ============================================================
# AIRFLOW ENTRYPOINT
# ============================================================

def run_macro_pipeline(**context):
    return fetch_macro_data()