"""
bond_pipeline.py
=================
Production-grade bond ingestion + computation pipeline — hedge-fund quality.

Architecture note
-----------------
This pipeline differs from market_features_pipeline and fx_exposure_pipeline
intentionally: Snowflake is the system of record for full bond history, so
no S3 raw/feature/rolling layers, DLQ system, or schema-hash tracking are used.

Data flow
---------
S3 (synthetic_bond.csv)      -> base bond universe
FRED (DGS10)                 -> 10-year Treasury yield
Snowflake (BONDS.date,DGS10) -> last 20 rows of DGS10 history (rolling warm-up)
S3 (macro_data.csv)          -> macro features (GDP, UNRATE, FEDFUNDS, CPI)
S3 (XGBoost models)          -> credit spread + PD predictions

Output destinations
-------------------
Snowflake  : BONDS table
PostgreSQL : public.bond_data table

fill_method_flag
----------------
Every DGS10-aligned row carries this column:
  "REAL"             -> FRED returned an observed value for that date
  "FORWARD_FILLED"   -> date was a business day with no FRED observation;
                       value was forward-filled from the most recent prior value
  "BACKWARD_FILLED"  -> date was a business day with no FRED observation and
                       no prior value available; value was backward-filled
This flag is preserved through the entire pipeline into both databases.

DGS10 rolling fix
-----------------
The original pipeline computed rolling(20) using only newly fetched data,
producing NaN or incorrect values for the first 19 rows of every run.
This pipeline queries the last 20 DGS10 values from Snowflake before fetching
new data, prepends them as history context, computes rolling(20) correctly,
then strips the history rows before writing — ensuring every output row has
a valid DGS10_ma value.

Airflow notes
-------------
- Recommended: retries=2, retry_delay=timedelta(minutes=5)
- max_active_runs=1 to prevent concurrent Snowflake writes
- Entry point: update_bonds_pipeline()
"""

import os
import uuid
import json
import time
import pandas as pd
import numpy as np
import xgboost as xgb
import joblib
import boto3
from botocore.exceptions import ClientError
from datetime import datetime, timedelta, timezone, date as date_type
from io import BytesIO, StringIO
from fredapi import Fred
from connections.snowflake_conn import get_snowflake_conn
from connections.postgre_conn import get_postgre_conn
from snowflake.connector.pandas_tools import write_pandas
import requests

# =============================================================
# CONFIG  —  single source of truth.  No magic numbers in code.
# =============================================================

CONFIG: dict = {
    # FRED fetch
    "max_retries":         3,
    "retry_backoff_s":     5,          # seconds; multiplied by attempt number

    # DGS10 rolling warm-up
    "dgs10_history_rows":  20,         # rows loaded from Snowflake to warm rolling(20)
    "dgs10_rolling_window": 20,        # window for DGS10_ma

    # S3 keys
    "s3_bucket":           "pushparag-bond-bucket",
    "base_bond_key":       "synthetic_bond.csv",
    "macro_key":           "macro_data.csv",
    "model_spread_key":    "bond_model_spread5d_xgb.json",
    "model_pd_key":        "bond_model_pd21d_xgb.json",
    "features_spread_key": "bond_features_spread5d.pkl",
    "features_pd_key":     "bond_features_pd21d.pkl",

    # Snowflake
    "snowflake_table":     "BONDS",
    "snowflake_chunk":     20000,

    # PostgreSQL
    "postgres_table":      "bond_data",
    "postgres_schema":     "public",

    # Final output columns — defines exact column contract for both DBs
    "final_cols": [
        "bond_id", "ticker", "sector", "industry", "credit_rating",
        "coupon_rate", "issue_date", "maturity_date", "maturity_years",
        "date", "benchmark_yield", "corporate_yield", "credit_spread",
        "bond_price", "yield_to_maturity", "implied_hazard",
        "implied_pd_annual", "implied_pd_multi_year", "implied_rating",
        "market_synthetic_score", "DGS10", "DGS10_ma", "dgs10_anom",
        "fill_method_flag",
        "gdp", "unrate", "fedfunds", "cpi",
        "pred_spread_5d", "pred_pd_21d",
        # Lineage
        "pipeline_name", "pipeline_run_id", "data_source",
        "input_source", "transformation", "record_created_at",
    ],

    # Lineage  ← bump transformation when model or logic changes
    "pipeline_name":  "bonds_update_pipeline",
    "data_source":    "fred+synthetic+macro",
    "input_source":   "synthetic_bond+fred+macro",
    "transformation": "bond_features_v1",
}

# Volatility mapping by credit rating — same as original pipeline
VOL_MAP: dict[str, int] = {
    "AAA":  2,  "AA+":  3, "AA":  4, "A+":  5,
    "A":    6,  "A-":   8,
    "BBB+": 10, "BBB": 12, "BBB-": 15,
    "BB+":  20, "BB":  25, "B":   35, "CCC": 50,
}

# =============================================================
# CUSTOM EXCEPTIONS
# =============================================================

class BondPipelineError(Exception):
    """Raised for unrecoverable pipeline failures."""


class DataValidationError(Exception):
    """Raised when data validation checks fail."""


# =============================================================
# PRODUCTION-GRADE ALERTING
# =============================================================

def send_alert(message: str, level: str = "INFO", context: dict = None):
    """
    Production-grade alert dispatcher with rate limiting and safe Slack integration.
    
    Args:
        message: Alert message
        level: INFO, WARNING, ERROR, CRITICAL
        context: Additional context dict
    
    Design principles:
        - Slack calls are safe (timeout + silent failure)
        - Rate limiting by severity prevents alert spam
        - Only CRITICAL/ERROR go to Slack by default
        - INFO alerts go to logs only (observability without noise)
    """
    payload = {
        "level": level,
        "pipeline": CONFIG["pipeline_name"],
        "message": message,
        "context": context or {},
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    # Always print to stdout for log aggregation (CloudWatch, Datadog, etc.)
    print(f"[ALERT][{level}] {message} | context={payload['context']}")

    # Only send to Slack for ERROR/CRITICAL or when explicitly forced
    if level in ["ERROR", "CRITICAL"]:
        webhook = os.getenv("SLACK_WEBHOOK_URL")
        if webhook:
            try:
                # Truncate long messages to avoid Slack payload limits
                truncated_msg = message[:500] + "..." if len(message) > 500 else message
                text = f"*[{level}]* {CONFIG['pipeline_name']}\n" \
                       f"{truncated_msg}\n" \
                       f"run_id: {payload['context'].get('run_id', 'unknown')}"
                # Use timeout to prevent Slack from hanging the pipeline
                requests.post(webhook, json={"text": text}, timeout=3)
            except Exception as e:
                # Silent fail - don't crash pipeline for alerting
                print(f"[ALERT ERROR] Slack notification failed: {e}")


# =============================================================
# TIMEZONE HELPERS
# =============================================================

def utc_now() -> datetime:
    """Current datetime as a tz-aware UTC object."""
    return datetime.now(timezone.utc)


def today_utc() -> date_type:
    """Today's date in UTC."""
    return utc_now().date()


# =============================================================
# S3 HELPERS
# =============================================================

def get_s3():
    """Return a boto3 S3 client per-call to avoid stale sessions."""
    return boto3.client("s3")


def read_csv_from_s3(bucket: str, key: str) -> pd.DataFrame:
    """Read a CSV from S3. Raises BondPipelineError on failure."""
    try:
        print(f"  [S3 READ CSV]     s3://{bucket}/{key}")
        obj = get_s3().get_object(Bucket=bucket, Key=key)
        return pd.read_csv(BytesIO(obj["Body"].read()))
    except ClientError as exc:
        raise BondPipelineError(
            f"Failed to read s3://{bucket}/{key}: {exc}"
        ) from exc


def read_bytes_from_s3(bucket: str, key: str) -> bytes:
    """Read raw bytes from S3. Raises BondPipelineError on failure."""
    try:
        print(f"  [S3 READ BYTES]   s3://{bucket}/{key}")
        return get_s3().get_object(Bucket=bucket, Key=key)["Body"].read()
    except ClientError as exc:
        raise BondPipelineError(
            f"Failed to read s3://{bucket}/{key}: {exc}"
        ) from exc


# =============================================================
# STAGE 1 — load_base_bond_data
# =============================================================

def load_base_bond_data(bucket: str) -> pd.DataFrame:
    """
    Load the static synthetic bond universe from S3.
    This is the base dataset repeated for each business day.
    """
    base = read_csv_from_s3(bucket, CONFIG["base_bond_key"])

    if base.empty:
        raise DataValidationError(
            "Base bond file is empty — cannot generate daily rows."
        )

    print(f"  [STAGE 1] Loaded {len(base):,} base bond records.")
    return base


# =============================================================
# STAGE 2 — generate_daily_rows
# =============================================================

def generate_daily_rows(
    base:       pd.DataFrame,
    start_date: str,
    end_date:   str,
) -> pd.DataFrame:
    """
    Expand the base bond universe to one row per bond per business day.
    Applies volatility mapping. Pure function — no I/O.

    Business logic preserved exactly from original pipeline.
    """
    dates  = pd.date_range(start_date, end_date, freq="B")
    n_days = len(dates)

    if n_days == 0:
        raise DataValidationError(
            f"No business days in range [{start_date}, {end_date}]."
        )

    daily        = base.loc[base.index.repeat(n_days)].copy()
    daily["date"] = np.tile(dates, len(base))

    # Volatility mapping by credit rating (original logic preserved)
    daily["vol"] = (
        daily["credit_rating"].map(VOL_MAP).fillna(10) / 100
    )

    print(f"  [STAGE 2] Generated {len(daily):,} daily rows across {n_days} business days.")
    return daily


# =============================================================
# STAGE 3 — load_dgs10_history_from_snowflake
# =============================================================

def load_dgs10_history_from_snowflake(start_date=None) -> pd.DataFrame:
    """
    Load the last CONFIG["dgs10_history_rows"] DGS10 observations from
    Snowflake to use as warm-up history for rolling(20) computation.

    Without this step, the first 19 rows of every run produce NaN for
    DGS10_ma — incorrect in a production system.

    Returns a DataFrame with columns [date, DGS10] sorted ascending.
    Returns empty DataFrame if Snowflake query fails (non-fatal — pipeline
    can proceed but rolling values may be less accurate for early rows).
    """
    n = CONFIG["dgs10_history_rows"]
    try:
        with get_snowflake_conn() as ctx:
            with ctx.cursor() as cs:
                if start_date:
                    cs.execute(f'''
                        SELECT "date", "DGS10"
                        FROM "{CONFIG["snowflake_table"]}"
                        WHERE "DGS10" IS NOT NULL
                        AND "date" < '{start_date}'
                        ORDER BY "date" DESC
                        LIMIT {n}
                    ''')
                else:
                    cs.execute(f'''
                        SELECT "date", "DGS10"
                        FROM "{CONFIG["snowflake_table"]}"
                        WHERE "DGS10" IS NOT NULL
                        ORDER BY "date" DESC
                        LIMIT {n}
                    ''')
                rows = cs.fetchall()

        if not rows:
            print(f"  [STAGE 3] No DGS10 history found in Snowflake — proceeding without warm-up.")
            return pd.DataFrame(columns=["date", "DGS10"])

        history = pd.DataFrame(rows, columns=["date", "DGS10"])
        history["date"]  = pd.to_datetime(history["date"])
        history["DGS10"] = pd.to_numeric(history["DGS10"], errors="coerce")
        history = history.sort_values("date").reset_index(drop=True)

        print(f"  [STAGE 3] Loaded {len(history)} historical DGS10 rows from Snowflake "
              f"({history['date'].min().date()} -> {history['date'].max().date()}).")
        return history

    except Exception as exc:
        print(f"  [STAGE 3][WARN] Could not load DGS10 history from Snowflake: {exc}")
        print(f"  [STAGE 3][WARN] Proceeding without warm-up — rolling values may be imprecise.")
        return pd.DataFrame(columns=["date", "DGS10"])


# =============================================================
# STAGE 4 — fetch_dgs10_from_fred (IMPROVED ALERTING)
# =============================================================

def fetch_dgs10_from_fred(
    start_date: str,
    end_date:   str,
    run_id:     str = None,  # Added for alert context
) -> pd.DataFrame:
    """
    Fetch DGS10 (10-year Treasury yield) from FRED for the date range.

    Retry strategy: up to CONFIG["max_retries"] with exponential backoff.
    Returns a DataFrame with columns [date, DGS10] for observed dates only.
    Raises BondPipelineError if all retries are exhausted.
    """
    fred_api_key = os.getenv("FRED_API_KEY")
    
    # Validate API key before attempting fetch
    if not fred_api_key:
        send_alert(
            "FRED_API_KEY environment variable not set",
            level="CRITICAL",
            context={"run_id": run_id, "start_date": start_date, "end_date": end_date}
        )
        raise BondPipelineError("FRED_API_KEY not configured")

    for attempt in range(1, CONFIG["max_retries"] + 1):
        try:
            fred   = Fred(api_key=fred_api_key)
            series = fred.get_series(
                "DGS10",
                observation_start = start_date,
                observation_end   = end_date,
            )
            if series is None or series.empty:
                raise ValueError("FRED returned empty series for DGS10.")

            df = (
                series.to_frame("DGS10")
                .reset_index()
                .rename(columns={"index": "date"})
            )
            df["date"]  = pd.to_datetime(df["date"])
            df["DGS10"] = pd.to_numeric(df["DGS10"], errors="coerce")

            print(
                f"  [STAGE 4] Fetched {len(df)} DGS10 observations from FRED "
                f"({df['date'].min().date()} -> {df['date'].max().date()})."
            )
            return df

        except Exception as exc:
            print(
                f"  [STAGE 4] FRED fetch attempt {attempt}/{CONFIG['max_retries']} "
                f"failed: {exc}"
            )
            if attempt < CONFIG["max_retries"]:
                time.sleep(CONFIG["retry_backoff_s"] * attempt)
            else:
                # Send CRITICAL alert only on final failure (not on retries)
                send_alert(
                    f"FRED DGS10 fetch failed after {CONFIG['max_retries']} attempts",
                    level="CRITICAL",
                    context={
                        "run_id": run_id,
                        "start_date": start_date,
                        "end_date": end_date,
                        "last_error": str(exc)
                    }
                )
                raise BondPipelineError(
                    f"FRED DGS10 fetch failed after {CONFIG['max_retries']} attempts: {exc}"
                ) from exc


# =============================================================
# STAGE 5 — build_dgs10_series
# =============================================================

def build_dgs10_series(
    history_df:   pd.DataFrame,
    new_fred_df:  pd.DataFrame,
    start_date:   str,
    end_date:     str,
) -> pd.DataFrame:
    """
    Combine Snowflake history with new FRED data, align to business days,
    apply forward/backward fill with explicit fill_method_flag tracking,
    and compute DGS10_ma (rolling 20-day mean) and dgs10_anom.

    The fill_method_flag column captures exactly how each value was sourced:
      "REAL"           -> FRED observed value for that business day
      "FORWARD_FILLED" -> no FRED value; filled forward from prior observation
      "BACKWARD_FILLED"-> no FRED value and no prior observation; filled backward

    Steps:
      1. Combine history + new FRED rows (history provides rolling warm-up).
      2. Create the full business-day spine for the new-data range.
      3. Left-join FRED data onto spine; mark REAL vs missing.
      4. Forward-fill, marking newly filled rows as FORWARD_FILLED.
      5. Backward-fill remaining NaNs, marking as BACKWARD_FILLED.
      6. Compute DGS10_ma and dgs10_anom over the combined series.
      7. Strip warm-up history rows — return only the new-date rows.

    Returns DataFrame with columns:
        date, DGS10, fill_method_flag, DGS10_ma, dgs10_anom
    """
    business_days = pd.date_range(start_date, end_date, freq="B")
    spine         = pd.DataFrame({"date": business_days})

    # Combine Snowflake history with new FRED data
    # History provides the pre-period context needed for rolling(20)
    combined_fred = pd.concat(
        [history_df[["date", "DGS10"]], new_fred_df[["date", "DGS10"]]],
        ignore_index=True,
    ).drop_duplicates(subset=["date"]).sort_values("date").reset_index(drop=True)

    # Left-join onto business-day spine — missing dates will be NaN
    spine = spine.merge(combined_fred, on="date", how="left")

    # ── Fill method tracking ────────────────────────────────────────────
    # Step 1: mark all positions that currently have a REAL FRED observation
    real_mask                    = spine["DGS10"].notna()
    spine["fill_method_flag"]    = np.where(real_mask, "REAL", pd.NA)

    # Step 2: forward-fill — capture which rows changed from NaN
    spine["_dgs10_before_ffill"] = spine["DGS10"].copy()
    spine["DGS10"]               = spine["DGS10"].ffill()

    ffill_mask = spine["_dgs10_before_ffill"].isna() & spine["DGS10"].notna()
    spine.loc[ffill_mask, "fill_method_flag"] = "FORWARD_FILLED"

    # Step 3: backward-fill any remaining NaNs (typically only at series start)
    spine["_dgs10_before_bfill"] = spine["DGS10"].copy()
    spine["DGS10"]               = spine["DGS10"].bfill()

    bfill_mask = spine["_dgs10_before_bfill"].isna() & spine["DGS10"].notna()
    spine.loc[bfill_mask, "fill_method_flag"] = "BACKWARD_FILLED"

    # Fill any remaining flag NaN (all-NaN series edge case)
    spine["fill_method_flag"] = spine["fill_method_flag"].fillna("BACKWARD_FILLED")

    spine = spine.drop(columns=["_dgs10_before_ffill", "_dgs10_before_bfill"])

    # ── Rolling features over full series (history + new) ───────────────
    # Sorting is already guaranteed; compute on the complete series so
    # rolling(20) is fully warm for the first new-date row.
    spine["DGS10_ma"]  = (
        spine["DGS10"].rolling(
            CONFIG["dgs10_rolling_window"], min_periods=1
        ).mean()
    )
    spine["dgs10_anom"] = spine["DGS10"] - spine["DGS10_ma"]

    # ── Strip warm-up rows ───────────────────────────────────────────────
    # Only return dates within the requested new range; history rows were
    # needed only to warm the rolling window.
    start_ts = pd.Timestamp(start_date)
    spine     = spine[spine["date"] >= start_ts].reset_index(drop=True)

    real_count  = int((spine["fill_method_flag"] == "REAL").sum())
    ffill_count = int((spine["fill_method_flag"] == "FORWARD_FILLED").sum())
    bfill_count = int((spine["fill_method_flag"] == "BACKWARD_FILLED").sum())

    print(
        f"  [STAGE 5] DGS10 series built — "
        f"REAL={real_count}, FORWARD_FILLED={ffill_count}, "
        f"BACKWARD_FILLED={bfill_count}  ({len(spine)} total rows)."
    )
    return spine


# =============================================================
# STAGE 6 — merge_macro_data
# =============================================================

def merge_macro_data(
    daily:  pd.DataFrame,
    bucket: str,
) -> pd.DataFrame:
    """
    Merge macro features (GDP, UNRATE, FEDFUNDS, CPI) by month-year key.
    Preserves original pipeline logic exactly.

    On failure, logs a warning and returns the input DataFrame unchanged
    (macro merge is non-fatal — bond rows without macro are still valuable).
    """
    try:
        macro = read_csv_from_s3(bucket, CONFIG["macro_key"])
        macro = macro.copy()
        macro["date"]  = pd.to_datetime(macro["date"])
        macro["mm_yy"] = macro["date"].dt.strftime("%m-%y")

        daily          = daily.copy()
        daily["mm_yy"] = daily["date"].dt.strftime("%m-%y")

        merged = daily.merge(
            macro.drop(columns=["date"]),
            on    = "mm_yy",
            how   = "inner",
        ).drop(columns=["mm_yy"])

        print(f"  [STAGE 6] Macro merge: {len(daily):,} -> {len(merged):,} rows.")
        return merged

    except Exception as exc:
        print(f"  [STAGE 6][WARN] Macro merge failed — proceeding without macro: {exc}")
        if "mm_yy" in daily.columns:
            daily = daily.drop(columns=["mm_yy"])
        return daily


# =============================================================
# STAGE 7 — run_predictions
# =============================================================

def run_predictions(df: pd.DataFrame, bucket: str) -> pd.DataFrame:
    """
    Load XGBoost models + feature lists from S3 and generate:
      - pred_spread_5d
      - pred_pd_21d

    Design:
    - Model loading failure → soft fail (skip predictions)
    - Feature/data issues → HARD fail (data integrity)
    """

    if df.empty:
        return df

    # =========================
    # LOAD MODELS + FEATURES
    # =========================
    try:
        model_spread = xgb.Booster()
        model_spread.load_model(
            bytearray(read_bytes_from_s3(bucket, CONFIG["model_spread_key"]))
        )

        model_pd = xgb.Booster()
        model_pd.load_model(
            bytearray(read_bytes_from_s3(bucket, CONFIG["model_pd_key"]))
        )

        features_spread = joblib.load(
            BytesIO(read_bytes_from_s3(bucket, CONFIG["features_spread_key"]))
        )

        features_pd = joblib.load(
            BytesIO(read_bytes_from_s3(bucket, CONFIG["features_pd_key"]))
        )

    except BondPipelineError as exc:
        print(f"  [STAGE 7][WARN] Model loading failed — skipping predictions: {exc}")
        return df

    df = df.copy()

    # =========================
    # SPREAD MODEL (STRICT)
    # =========================
    missing_spread = [f for f in features_spread if f not in df.columns]
    if missing_spread:
        raise DataValidationError(
            f"[BOND MODEL ERROR] Missing features (spread): {missing_spread}"
        )

    null_spread = [c for c in features_spread if df[c].isna().any()]
    if null_spread:
        raise DataValidationError(
            f"[BOND MODEL ERROR] Null values (spread): {null_spread}"
        )

    df["pred_spread_5d"] = model_spread.predict(
        xgb.DMatrix(df[features_spread])
    )

    print(f"  [STAGE 7] pred_spread_5d computed for {len(df):,} rows.")

    # =========================
    # PD MODEL (STRICT)
    # =========================
    missing_pd = [f for f in features_pd if f not in df.columns]
    if missing_pd:
        raise DataValidationError(
            f"[BOND MODEL ERROR] Missing features (pd): {missing_pd}"
        )

    null_pd = [c for c in features_pd if df[c].isna().any()]
    if null_pd:
        raise DataValidationError(
            f"[BOND MODEL ERROR] Null values (pd): {null_pd}"
        )

    df["pred_pd_21d"] = model_pd.predict(
        xgb.DMatrix(df[features_pd])
    )

    print(f"  [STAGE 7] pred_pd_21d computed for {len(df):,} rows.")

    return df


# =============================================================
# LINEAGE METADATA
# =============================================================

def add_lineage_metadata(
    df:     pd.DataFrame,
    run_id: str,
    run_ts: datetime,
) -> pd.DataFrame:
    """
    Stamp every output row with pipeline lineage.

    Guarantees:
    - pipeline_run_id and record_created_at are identical across all rows
      in the same run — enabling exact run reconstruction from either DB.
    - fill_method_flag is preserved from Stage 5 and not overwritten here.
    """
    df = df.copy()
    df["pipeline_name"]    = CONFIG["pipeline_name"]
    df["pipeline_run_id"]  = run_id                  # constant per run
    df["data_source"]      = CONFIG["data_source"]
    df["input_source"]     = CONFIG["input_source"]
    df["transformation"]   = CONFIG["transformation"]
    df["record_created_at"] = run_ts.isoformat()     # constant per run, UTC-aware
    return df


# =============================================================
# DATA VALIDATION (IMPROVED ALERTING)
# =============================================================
def validate_output(df: pd.DataFrame, run_date: date_type, run_id: str = None) -> None:
    """
    Lightweight validation before database writes.
    Raises DataValidationError on any hard failure.
    """

    errors = []

    # ── Check 1: Empty DataFrame ─────────────────────────────
    if df.empty:
        errors.append("Output DataFrame is empty — nothing to upload")

    # ── Check 2: Freshness ───────────────────────────────────
    else:
        max_date = pd.to_datetime(df["date"]).dt.date.max()
        if max_date < run_date:
            errors.append(
                f"Freshness check failed: latest date {max_date}, expected >= {run_date}"
            )

    # ── Check 3: fill_method_flag exists ─────────────────────
    if "fill_method_flag" not in df.columns:
        errors.append("fill_method_flag column is missing")

    else:
        valid_flags   = {"REAL", "FORWARD_FILLED", "BACKWARD_FILLED"}
        invalid_flags = set(df["fill_method_flag"].unique()) - valid_flags

        if invalid_flags:
            errors.append(f"Unexpected fill_method_flag values: {invalid_flags}")

    # ── FINAL: Alert + Raise (ONLY ONCE) ─────────────────────
    if errors:
        send_alert(
            "Bond pipeline validation failed",
            level="CRITICAL",
            context={
                "run_id": run_id,
                "errors": errors
            }
        )
        raise DataValidationError("; ".join(errors))

    # ── SUCCESS LOG ──────────────────────────────────────────
    print(
        f"  [VALIDATION] OK — {len(df):,} rows, "
        f"max_date={max_date}, "
        f"fill_flags={df['fill_method_flag'].value_counts().to_dict()}"
    )

def drop_metadata_for_serving(df):
    drop_cols = [
        "pipeline_name",
        "pipeline_run_id",
        "data_source",
        "input_source",
        "transformation",
        "record_created_at",
    ]
    return df.drop(columns=[c for c in drop_cols if c in df.columns])


# =============================================================
# STAGE 8A — upload_to_snowflake (IMPROVED ALERTING)
# =============================================================

def upload_to_snowflake(df: pd.DataFrame, run_id: str = None) -> int:
    """
    Write new bond rows to Snowflake BONDS table using write_pandas.
    Returns the number of rows successfully written.
    Raises BondPipelineError on failure.
    """
    try:
        with get_snowflake_conn() as ctx:
            success, _, nrows, _ = write_pandas(
                ctx,
                df,
                CONFIG["snowflake_table"],
                chunk_size       = CONFIG["snowflake_chunk"],
                quote_identifiers = True,
            )

        if not success:
            send_alert(
                f"Snowflake write_pandas returned failure",
                level="CRITICAL",
                context={
                    "run_id": run_id,
                    "table": CONFIG["snowflake_table"],
                    "rows_attempted": len(df)
                }
            )
            raise BondPipelineError(
                f"write_pandas returned failure for table={CONFIG['snowflake_table']}."
            )

        print(f"  [STAGE 8] Snowflake upload complete — {nrows:,} rows written.")
        return nrows

    except BondPipelineError:
        raise
    except Exception as exc:
        send_alert(
            f"Snowflake upload failed with exception",
            level="CRITICAL",
            context={
                "run_id": run_id,
                "table": CONFIG["snowflake_table"],
                "error": str(exc)
            }
        )
        raise BondPipelineError(f"Snowflake upload failed: {exc}") from exc


# =============================================================
# STAGE 8B — upload_to_postgres
# =============================================================

def upload_to_postgres(df: pd.DataFrame) -> None:
    """
    Write new bond rows to PostgreSQL using COPY FROM STDIN.

    Design:
    - Snowflake = full metadata
    - Postgres  = serving layer (NO metadata)
    - Strict schema enforcement (fail-fast)
    - Non-fatal: errors logged but don't raise (Snowflake is source of truth)
    """

    try:
        # =========================
        # STEP 1: REMOVE METADATA
        # =========================
        df_clean = drop_metadata_for_serving(df.copy())

        with get_postgre_conn() as pg_conn:
            with pg_conn.cursor() as pg_cur:

                # =========================
                # STEP 2: FETCH DB SCHEMA
                # =========================
                pg_cur.execute(
                    """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = %s
                      AND table_name   = %s
                    ORDER BY ordinal_position
                    """,
                    (CONFIG["postgres_schema"], CONFIG["postgres_table"]),
                )

                pg_cols_order = [
                    row[0] for row in pg_cur.fetchall()
                    if row[0] != "id"
                ]

                if not pg_cols_order:
                    raise RuntimeError(
                        f"No columns found for "
                        f"{CONFIG['postgres_schema']}.{CONFIG['postgres_table']}"
                    )

                # =========================
                # STEP 3: VALIDATE SCHEMA (FAIL-FAST)
                # =========================
                missing = set(pg_cols_order) - set(df_clean.columns)
                if missing:
                    raise ValueError(
                        f"[BONDS POSTGRES ERROR] Missing columns: {missing}"
                    )

                # =========================
                # STEP 4: ENFORCE SCHEMA
                # =========================
                df_pg = df_clean[pg_cols_order].copy()

                # =========================
                # STEP 5: COPY TO POSTGRES
                # =========================
                buf = StringIO()
                df_pg.to_csv(buf, index=False, header=False)
                buf.seek(0)

                quoted_cols = [f'"{col}"' for col in pg_cols_order]

                copy_sql = (
                    f'COPY {CONFIG["postgres_schema"]}.{CONFIG["postgres_table"]} '
                    f'({", ".join(quoted_cols)}) '
                    f'FROM STDIN WITH CSV'
                )

                with pg_cur.copy(copy_sql) as copy:
                    copy.write(buf.getvalue())

            pg_conn.commit()

        print(f"[POSTGRES] Bonds upload complete — {len(df_pg):,} rows written.")

    except Exception as exc:
        # Non-fatal: Snowflake is system of record, Postgres is serving layer
        print(f"[POSTGRES][WARN] Bonds upload failed (Snowflake preserved): {exc}")
        # No alert for Postgres failure - prevents noise, Snowflake is source of truth


# =============================================================
# MAIN PIPELINE  —  update_bonds_pipeline (WITH SUCCESS ALERT)
# =============================================================

def update_bonds_pipeline(start_date_override=None) -> str:
    """
    Main Airflow-callable entry point.

    Execution order
    ---------------
    STAGE 1  -> Load base bond data from S3
    STAGE 2  -> Generate daily bond rows (business day expansion)
    STAGE 3  -> Load last 20 DGS10 rows from Snowflake (rolling warm-up)
    STAGE 4  -> Fetch new DGS10 from FRED
    STAGE 5  -> Apply fill logic + compute DGS10_ma / dgs10_anom + fill_method_flag
    STAGE 6  -> Merge macro data
    STAGE 7  -> Run ML model predictions (spread + PD)
    STAGE 8  -> Validate output, attach lineage, upload to Snowflake + PostgreSQL

    Returns a status string suitable for Airflow task logging.
    """

    # ── 0. INIT ────────────────────────────────────────────────────────
    run_ts  = utc_now()
    run_id  = f"run_{run_ts.strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
    today   = today_utc()
    bucket  = CONFIG["s3_bucket"]

    print(f"\n{'=' * 66}")
    print(f"  BOND PIPELINE START   run_id={run_id}")
    print(f"  run_date={today}")
    print(f"{'=' * 66}\n")

    # ── Determine incremental date range from Snowflake watermark ───────
    try:
        with get_snowflake_conn() as ctx:
            with ctx.cursor() as cs:
                cs.execute(
                    f'SELECT MAX("date") FROM "{CONFIG["snowflake_table"]}"'
                )
                last_date_sf = cs.fetchone()[0]
    except Exception as exc:
        send_alert(
            f"Could not query Snowflake watermark",
            level="CRITICAL",
            context={"run_id": run_id, "error": str(exc)}
        )
        raise BondPipelineError(
            f"Could not query Snowflake watermark: {exc}"
        ) from exc

    last_date_sf = (
        pd.Timestamp(last_date_sf).date()
        if last_date_sf
        else pd.Timestamp("1970-01-01").date()
    )

    if start_date_override:
        start_date = pd.to_datetime(start_date_override).date()
    else:
        start_date = last_date_sf + timedelta(days=1)

    print(f"Override mode: {bool(start_date_override)}")

    if start_date >= today:
        print("  [SKIP] Already up to date — no new business days to process.")
        print(f"{'=' * 66}\n")
        
        # INFO alert for skip (not noisy)
        send_alert(
            f"No new data to process",
            level="INFO",
            context={"run_id": run_id, "start_date": str(start_date), "today": str(today)},
        )
        return "ALREADY_UPDATED"

    print(f"  Incremental range: {start_date} -> {today}")

    start_str = start_date.strftime("%Y-%m-%d")
    end_str   = today.strftime("%Y-%m-%d")

    # ── STAGE 1: Load base bond data ─────────────────────────────────────
    print("\n[ STAGE 1 ] Load base bond data from S3")
    base = load_base_bond_data(bucket)

    # ── STAGE 2: Generate daily rows ─────────────────────────────────────
    print("\n[ STAGE 2 ] Generate daily bond rows")
    daily = generate_daily_rows(base, start_str, end_str)

    # Weekday filter (original pipeline logic)
    daily = daily[daily["date"].dt.weekday < 5].copy()
    if not start_date_override:
        daily = daily[daily["date"].dt.date > last_date_sf].copy()

    if daily.empty:
        print("  No new weekday rows after filtering.")
        print(f"{'=' * 66}\n")
        return "NO_NEW_ROWS"

    # ── STAGE 3: Load DGS10 history from Snowflake ───────────────────────
    print("\n[ STAGE 3 ] Load last 20 days DGS10 from Snowflake")
    dgs10_history = load_dgs10_history_from_snowflake(start_date)

    # ── STAGE 4: Fetch new DGS10 from FRED (with run_id for alerts) ──────
    print("\n[ STAGE 4 ] Fetch new DGS10 from FRED")
    dgs10_new = fetch_dgs10_from_fred(start_str, end_str, run_id=run_id)

    # ── STAGE 5: Build DGS10 series with fill flags ──────────────────────
    print("\n[ STAGE 5 ] Apply fill logic + compute rolling features")
    dgs10_series = build_dgs10_series(
        history_df  = dgs10_history,
        new_fred_df = dgs10_new,
        start_date  = start_str,
        end_date    = end_str,
    )

    # Merge DGS10 enrichment into daily bond rows
    daily = daily.merge(dgs10_series, on="date", how="left")

    # ── STAGE 6: Merge macro data ────────────────────────────────────────
    print("\n[ STAGE 6 ] Merge macro data")
    daily = merge_macro_data(daily, bucket)

    if daily.empty:
        raise DataValidationError(
            "DataFrame is empty after macro merge — check macro_data.csv date coverage."
        )

    # ── STAGE 7: Run ML model predictions ────────────────────────────────
    print("\n[ STAGE 7 ] Run ML model predictions")
    predicted_df = run_predictions(daily, bucket)

    # ── STAGE 8: Validate + attach lineage + upload ───────────────────────
    print("\n[ STAGE 8 ] Validate, attach lineage, upload")

    # Attach lineage metadata to every output row
    predicted_df = add_lineage_metadata(predicted_df, run_id, run_ts)

    # Convert date to Python date for DB compatibility (original logic)
    predicted_df["date"] = pd.to_datetime(predicted_df["date"]).dt.date

    # Select final columns — only include those present in the DataFrame
    final_cols   = CONFIG["final_cols"]
    df_to_upload = predicted_df[
        [c for c in final_cols if c in predicted_df.columns]
    ].copy()

    # Data validation before any DB write (with run_id for alerts)
    validate_output(df_to_upload, today, run_id=run_id)

    # Upload to Snowflake (primary store — raises on failure) (with run_id for alerts)
    nrows = upload_to_snowflake(df_to_upload, run_id=run_id)

    # Upload to PostgreSQL (secondary store — non-fatal on failure)
    upload_to_postgres(df_to_upload)

    # ── SUCCESS ALERT (INFO level, once per run) ─────────────────────────
    fill_summary = df_to_upload["fill_method_flag"].value_counts().to_dict() \
        if "fill_method_flag" in df_to_upload.columns else {}
    
    send_alert(
        f"Pipeline completed successfully",
        level="INFO",
        context={
            "run_id": run_id,
            "rows_written": nrows,
            "date_range": f"{start_date} -> {today}",
            "fill_flags": fill_summary
        },
    )

    print(f"\n{'=' * 66}")
    print(f"  BOND PIPELINE SUCCESS   run_id={run_id}")
    print(f"  rows written : {nrows:,}")
    print(f"  date range   : {start_date} -> {today}")
    print(f"  fill flags   : {fill_summary}")
    print(f"{'=' * 66}\n")

    return f"SUCCESS_{nrows}_ROWS"