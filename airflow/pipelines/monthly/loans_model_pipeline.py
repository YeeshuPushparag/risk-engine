import os
import time
import hashlib
from io import BytesIO, StringIO
import numpy as np
import pandas as pd
import xgboost as xgb
import joblib
import boto3
import requests
import json
from snowflake.connector.pandas_tools import write_pandas
from connections.snowflake_conn import get_snowflake_conn
from connections.postgre_conn import get_postgre_conn
from datetime import datetime, timedelta

S3_BUCKET = "yeeshu-loan-bucket"
OUTPUT_TABLE = "LOANS"
HISTORY_TABLE = "LOANS_HISTORY"
s3 = boto3.client("s3")


# ============================================================
# PRODUCTION-GRADE ALERTING (CRITICAL + SUCCESS)
# ============================================================
def send_critical_alert(message: str, context: dict = None):
    """
    Send CRITICAL alert to Slack for unrecoverable pipeline failures.
    
    Triggers only for:
    - S3 loan enriched data load failure
    - Snowflake watermark query failure
    - Model/feature/encoder loading failures
    - Snowflake table write failures (system of record)
    - Complete pipeline failures
    """
    payload = {
        "level": "CRITICAL",
        "pipeline": "loans_model_pipeline",
        "message": message,
        "context": context or {},
        "timestamp": datetime.utcnow().isoformat(),
    }
    
    # Always print to stdout for log aggregation
    print(f"[CRITICAL] {message} | context={payload['context']}")
    
    # Send to Slack (safe, non-blocking)
    webhook = os.getenv("SLACK_WEBHOOK_URL")
    if webhook:
        try:
            truncated_msg = message[:500] + "..." if len(message) > 500 else message
            run_id = context.get('run_id', 'unknown') if context else 'unknown'
            text = f"*[CRITICAL]* loans_model_pipeline\n" \
                   f"{truncated_msg}\n" \
                   f"run_id: {run_id}"
            requests.post(webhook, json={"text": text}, timeout=3)
        except Exception as e:
            print(f"[ALERT ERROR] Slack notification failed: {e}")


def send_success_alert(context: dict = None):
    """
    Send SUCCESS alert to Slack when pipeline completes successfully.
    This is the FINAL loan pipeline - DAG ran successfully.
    """
    payload = {
        "level": "SUCCESS",
        "pipeline": "loans_model_pipeline",
        "message": "Pipeline completed successfully",
        "context": context or {},
        "timestamp": datetime.utcnow().isoformat(),
    }
    
    # Always print to stdout
    print(f"[SUCCESS] Pipeline completed | context={payload['context']}")
    
    # Send to Slack (safe, non-blocking)
    webhook = os.getenv("SLACK_WEBHOOK_URL")
    if webhook:
        try:
            run_id = context.get('run_id', 'unknown') if context else 'unknown'
            snowflake_rows = context.get('snowflake_rows', 0)
            history_rows = context.get('history_rows', 0)
            postgres_status = context.get('postgres_status', 'UNKNOWN')
            duration = context.get('duration_seconds', 0)
            mode = context.get('mode', 'incremental')
            
            text = f"*[SUCCESS]* loans_model_pipeline\n" \
                   f"FINAL LOAN PIPELINE completed successfully\n" \
                   f"run_id: {run_id}\n" \
                   f"mode: {mode}\n" \
                   f"Snowflake clean rows: {snowflake_rows:,}\n" \
                   f"Snowflake history rows: {history_rows:,}\n" \
                   f"Postgres: {postgres_status}\n" \
                   f"duration: {duration}s"
            requests.post(webhook, json={"text": text}, timeout=3)
        except Exception as e:
            print(f"[ALERT ERROR] Slack notification failed: {e}")


# ============================================================
# RUN SUMMARY WRITER
# ============================================================
def write_run_summary(summary: dict) -> None:
    """
    Write pipeline run summary to S3 for observability and audit.

    Args:
        summary: Dictionary containing run metadata including:
            - pipeline_run_id
            - pipeline_name
            - run_ts
            - status (SUCCESS/FAILED)
            - mode
            - start_date
            - rows_processed_clean
            - rows_processed_history
            - processing_time_s
            - tables_written (list)
            - error (if failed)
    """
    key = (
        f"metadata/loans_model_pipeline/"
        f"pipeline_run_id={summary['pipeline_run_id']}/"
        f"run_summary.json"
    )

    try:
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=key,
            Body=json.dumps(
                summary,
                default=str,
                indent=2,
            ),
        )
        print(f"  [RUN SUMMARY] Wrote {key}")
    except Exception as e:
        # Don't fail the pipeline if summary write fails, but log the error
        print(f"  [RUN SUMMARY][WARN] Failed to write summary to S3: {e}")

# ============================================================
# RETRY DECORATOR (PRODUCTION-GRADE)
# ============================================================
def retry_with_backoff(func, retries=3, backoff_factor=2, exceptions=(Exception,), 
                        critical_name=None, run_id=None):
    """
    Retry a function with exponential backoff.
    Only sends CRITICAL alert if all retries are exhausted and critical_name provided.
    """
    last_exception = None
    
    for attempt in range(retries + 1):
        try:
            return func()
        except exceptions as e:
            last_exception = e
            if attempt < retries:
                wait_time = backoff_factor ** attempt
                print(f"  Retry {attempt + 1}/{retries} after {wait_time}s: {e}")
                time.sleep(wait_time)
            else:
                print(f"  All {retries} retries exhausted: {e}")
    
    # Only send critical alert if this is a critical operation
    if critical_name:
        send_critical_alert(
            f"{critical_name} failed after {retries} retries",
            context={"run_id": run_id, "error": str(last_exception)}
        )
    
    raise last_exception


# ============================================================
# SNOWFLAKE WAREHOUSE HELPERS (DETERMINISTIC + TRANSACTIONAL)
# ============================================================
def write_to_snowflake_history(df, table_name, run_id=None, chunk_size=100000):
    """
    Append-only write to history table.
    ALWAYS appends - NEVER deletes.
    Contains run_mode and full lineage metadata.
    """
    if df.empty:
        return 0
    
    try:
        with get_snowflake_conn() as ctx:
            with ctx.cursor() as cs:
                # Write to history table (append-only)
                write_pandas(ctx, df, table_name, chunk_size=chunk_size, quote_identifiers=True, use_logical_type=True)
                
                # Get row count
                cs.execute(f"SELECT COUNT(*) FROM {table_name}")
                total_rows = cs.fetchone()[0]
                
                print(f"  Snowflake HISTORY append {table_name}: {len(df)} rows added (total: {total_rows})")
                return len(df)
                
    except Exception as exc:
        send_critical_alert(
            f"Snowflake HISTORY write failed for table {table_name}",
            context={"run_id": run_id, "table": table_name, "rows": len(df), "error": str(exc)}
        )
        raise


def transactional_delete_insert_snowflake_clean(df, table_name, target_months, run_id=None, chunk_size=100000):
    """
    TRANSACTIONAL delete + insert for Snowflake clean table.
    
    Args:
        df: DataFrame to insert
        table_name: Target table name
        target_months: List of Period objects to delete/replace
        run_id: Run identifier for alerts
    
    Returns:
        Number of rows inserted
    
    Transaction guarantees:
        - BEGIN transaction
        - DELETE rows for target_months
        - INSERT new rows
        - COMMIT on success
        - ROLLBACK on failure
    """
    if df.empty:
        return 0

    # =========================================================
    # DUPLICATE BUSINESS KEY VALIDATION
    # =========================================================

    key_columns = [
        "loan_id",
        "date",
    ]

    dupes = (
        df.groupby(key_columns)
        .size()
        .reset_index(name="cnt")
    )

    dup_rows = dupes[dupes["cnt"] > 1]

    if not dup_rows.empty:

        sample = dup_rows.head(10)

        raise ValueError(
            "Duplicate loan business keys detected "
            f"before transactional replace. Sample:\n{sample}"
        )
    
    if not target_months:
        # No months specified - simple insert
        return write_to_snowflake_clean(df, table_name, run_id, chunk_size)
    
    try:
        with get_snowflake_conn() as ctx:
            with ctx.cursor() as cs:
                # Begin transaction
                cs.execute("BEGIN")
                
                try:
                    # Delete rows for target months
                    month_conditions = []
                    for period in target_months:
                        start_date = period.start_time.strftime("%Y-%m-%d")
                        end_date = period.end_time.strftime("%Y-%m-%d")
                        month_conditions.append(f'("date" >= \'{start_date}\' AND "date" <= \'{end_date}\')')
                    
                    delete_condition = " OR ".join(month_conditions)
                    delete_sql = f'DELETE FROM {table_name} WHERE {delete_condition}'
                    cs.execute(delete_sql)
                    deleted_rows = cs.rowcount
                    print(f"    Deleted {deleted_rows} rows from {table_name} for {len(target_months)} months")
                    
                    # Insert new rows
                    write_pandas(ctx, df, table_name, chunk_size=chunk_size, quote_identifiers=True,use_logical_type=True)
                    inserted_rows = len(df)
                    print(f"    Inserted {inserted_rows} rows into {table_name}")
                    
                    # Commit transaction
                    cs.execute("COMMIT")
                    print(f"  TRANSACTION COMMITTED: {table_name} updated successfully")
                    return inserted_rows
                    
                except Exception as e:
                    # Rollback on any error
                    cs.execute("ROLLBACK")
                    print(f"  TRANSACTION ROLLED BACK: {table_name} update failed")
                    raise e
                
    except Exception as exc:
        send_critical_alert(
            f"Snowflake transactional DELETE+INSERT failed for table {table_name}",
            context={"run_id": run_id, "table": table_name, "rows": len(df), "target_months": [str(m) for m in target_months], "error": str(exc)}
        )
        raise


def write_to_snowflake_clean(df, table_name, run_id=None, chunk_size=100000):
    """
    Simple insert to clean table (non-transactional, assumes no conflict).
    Used for incremental append where months are guaranteed new.
    """
    if df.empty:
        return 0
    
    try:
        with get_snowflake_conn() as ctx:
            with ctx.cursor() as cs:
                write_pandas(ctx, df, table_name, chunk_size=chunk_size, quote_identifiers=True,use_logical_type=True)
                print(f"  Snowflake CLEAN insert {table_name}: {len(df)} rows added")
                return len(df)
                
    except Exception as exc:
        send_critical_alert(
            f"Snowflake CLEAN write failed for table {table_name}",
            context={"run_id": run_id, "table": table_name, "rows": len(df), "error": str(exc)}
        )
        raise


def get_processed_months_from_clean_table(table_name, months_to_check, run_id=None):
    """
    Check which months already exist in clean table.
    Returns set of months that are already present.
    """
    if not months_to_check:
        return set()
    
    try:
        with get_snowflake_conn() as ctx:
            with ctx.cursor() as cs:
                # Build month list for IN clause
                month_strs = [m.strftime("%Y-%m") for m in months_to_check]
                month_conditions = [f"TO_VARCHAR(DATE_TRUNC('MONTH', \"date\"), 'YYYY-MM') = '{ms}'" for ms in month_strs]
                where_clause = " OR ".join(month_conditions)
                
                query = f"""
                    SELECT DISTINCT TO_VARCHAR(DATE_TRUNC('MONTH', "date"), 'YYYY-MM') as month
                    FROM {table_name}
                    WHERE {where_clause}
                """
                cs.execute(query)
                existing_month_strs = [row[0] for row in cs.fetchall()]
                
                # Convert back to Period
                existing_months = {pd.Period(ms, freq="M") for ms in existing_month_strs}
                return existing_months
                
    except Exception as exc:
        send_critical_alert(
            f"Snowflake month check failed for {table_name}",
            context={"run_id": run_id, "table": table_name, "error": str(exc)}
        )
        raise


# ============================================================
# POSTGRES — SERVING LAYER (consistency-first: RAISES on failure)
# ============================================================

def write_to_postgres(df, mode, retries=3):
    """
    Write loan rows to the Postgres serving layer.

    Rules:
    - If incoming DF has >2 unique months:
        -> trim in memory to latest 2 months
    - If final DF has 2 unique months:
        -> FULL REPLACE (DELETE + INSERT)
        -> NO trim needed
    - If final DF has 1 unique month:
        -> INCREMENTAL APPEND
        -> then trim table to latest 2 months
    """

    if df.empty:
        print("  [POSTGRES] Empty DataFrame — nothing to write.")
        return

    # ─────────────────────────────────────────────
    # Prepare serving-layer DataFrame
    # ─────────────────────────────────────────────
    df_pg = drop_metadata_for_serving(df.copy())

    df_pg["month"] = (
        pd.to_datetime(df_pg["date"])
        .dt.to_period("M")
        .dt.to_timestamp()
        .dt.date
    )

    # ─────────────────────────────────────────────
    # Step 1: Trim in memory if >2 unique months
    # ─────────────────────────────────────────────
    unique_months = sorted(df_pg["month"].unique())

    if len(unique_months) > 2:

        latest_2_months = unique_months[-2:]

        df_pg = df_pg[
            df_pg["month"].isin(latest_2_months)
        ].copy()

        print(
            f"  [POSTGRES] Trimmed incoming DF "
            f"to latest 2 months: {latest_2_months}"
        )

    # Recalculate after trimming
    unique_months = sorted(df_pg["month"].unique())
    unique_month_count = len(unique_months)

    print(
        f"  [POSTGRES] Final incoming DF has "
        f"{unique_month_count} unique month(s)"
    )

    # ─────────────────────────────────────────────
    # Type fixes for Postgres
    # ─────────────────────────────────────────────
    if "stage" in df_pg.columns:

        df_pg["stage"] = (
            df_pg["stage"]
            .astype(float)
            .round()
            .astype(int)
        )

    for col in [
        "loan_age_months",
        "time_to_maturity_months"
    ]:

        if col in df_pg.columns:

            df_pg[col] = (
                df_pg[col]
                .fillna(0)
                .astype(float)
                .round()
                .astype(int)
            )

    df_final = df_pg.copy()

    # Drop Snowflake-only column if needed
    if "date" in df_final.columns:
        df_final = df_final.drop(columns=["date"])

    last_error = None

    for attempt in range(retries + 1):

        try:

            with get_postgre_conn() as pg_conn:

                with pg_conn.cursor() as pg_cur:

                    # ─────────────────────────────────────────────
                    # Step 2: Validate live schema
                    # ─────────────────────────────────────────────
                    pg_cur.execute("""
                        SELECT column_name
                        FROM information_schema.columns
                        WHERE table_schema = 'public'
                          AND table_name = 'loan_data'
                        ORDER BY ordinal_position
                    """)

                    pg_cols = [
                        r[0]
                        for r in pg_cur.fetchall()
                        if r[0].lower() != "id"
                    ]

                    if not pg_cols:

                        raise RuntimeError(
                            "Schema query returned 0 columns "
                            "for public.loan_data"
                        )

                    missing = (
                        set(pg_cols)
                        - set(df_final.columns)
                    )

                    if missing:

                        raise ValueError(
                            f"DataFrame missing Postgres columns: "
                            f"{missing}"
                        )

                    # Exact column order
                    df_ordered = df_final[
                        pg_cols
                    ].copy()

                    # ─────────────────────────────────────────────
                    # Step 3: Write strategy
                    # ─────────────────────────────────────────────
                    if unique_month_count == 2:

                        # FULL REPLACE
                        pg_cur.execute(
                            "DELETE FROM public.loan_data"
                        )

                        print(
                            "  [POSTGRES] Deleted all existing rows "
                            "(2-month refresh)"
                        )

                    elif unique_month_count == 1:

                        load_month = unique_months[0]

                        if mode == "incremental":

                            print(
                                "  [POSTGRES] Incremental append mode "
                                "(1-month load)"
                            )

                        else:

                            # BACKFILL / REFRESH MODE FOR SINGLE MONTH
                            pg_cur.execute("""
                                DELETE FROM public.loan_data
                                WHERE month = %s
                            """, (load_month,))

                            deleted = (
                                pg_cur.rowcount
                                if pg_cur.rowcount is not None
                                else 0
                            )

                            print(
                                f"  [POSTGRES] Backfill mode: deleted {deleted:,} rows "
                                f"for month {load_month}"
                            )

                    else:

                        raise ValueError(
                            f"Unexpected unique_month_count="
                            f"{unique_month_count}"
                        )

                    # ─────────────────────────────────────────────
                    # Step 4: Bulk INSERT via COPY
                    # ─────────────────────────────────────────────
                    quoted = [f'"{c}"' for c in pg_cols]
                    copy_sql = f"COPY public.loan_data ({','.join(quoted)}) FROM STDIN WITH CSV"

                    buf = StringIO()

                    df_ordered.to_csv(
                        buf,
                        index=False,
                        header=False
                    )

                    buf.seek(0)

                    with pg_cur.copy(copy_sql) as copy:
                        copy.write(buf.getvalue())

                    # ─────────────────────────────────────────────
                    # Step 5: Trim ONLY for incremental loads
                    # ─────────────────────────────────────────────
                    if (
                        unique_month_count == 1
                        and mode == "incremental"
                    ):

                        pg_cur.execute("""
                            DELETE FROM public.loan_data
                            WHERE month < (
                                SELECT MAX(month)
                                FROM public.loan_data
                            ) - INTERVAL '1 month'
                        """)

                        trimmed = (
                            pg_cur.rowcount
                            if pg_cur.rowcount is not None
                            else 0
                        )

                        print(
                            f"  [POSTGRES] Trimmed "
                            f"{trimmed:,} old rows"
                        )

                    else:

                        print(
                            "  [POSTGRES] No trim needed "
                        )

                # Commit transaction
                pg_conn.commit()

            print(
                f"  [POSTGRES] Success — inserted "
                f"{len(df_ordered):,} rows from "
                f"{unique_month_count} unique month(s)"
            )

            return

        except Exception as e:

            last_error = e

            if attempt < retries:

                wait_time = 2 ** attempt

                print(
                    f"  [POSTGRES] Retry "
                    f"{attempt + 1}/{retries} "
                    f"after {wait_time}s: {e}"
                )

                time.sleep(wait_time)

            else:

                print(
                    f"  [POSTGRES] All "
                    f"{retries} retries exhausted: {e}"
                )

    # ─────────────────────────────────────────────
    # Final failure
    # ─────────────────────────────────────────────
    send_critical_alert(
        f"Postgres write to loan_data "
        f"failed after {retries} retries",
        context={
            "error": str(last_error)
        },
    )

    raise RuntimeError(
        f"Postgres write to loan_data failed: "
        f"{last_error}"
    )
    
# ============================================================
# S3 LOAD WITH RETRY (CRITICAL)
# ============================================================
def load_parquet_from_s3_with_retry(bucket, key, run_id=None, retries=3):
    """Load parquet from S3 with retry logic. CRITICAL if fails."""
    def _load():
        obj = s3.get_object(Bucket=bucket, Key=key)
        return pd.read_parquet(BytesIO(obj["Body"].read()))
    
    return retry_with_backoff(_load, retries=retries,
                               critical_name=f"S3 parquet load: {key}",
                               run_id=run_id)


def load_csv_from_s3_with_retry(bucket, key, run_id=None, retries=3):
    """Load CSV from S3 with retry logic. SOFT FAIL - no alert."""
    def _load():
        obj = s3.get_object(Bucket=bucket, Key=key)
        return pd.read_csv(BytesIO(obj["Body"].read()))
    
    return retry_with_backoff(_load, retries=retries)


def load_model_from_s3_with_retry(bucket, key, run_id=None, retries=3):
    """Load XGBoost model from S3 with retry logic. CRITICAL if fails."""
    def _load():
        model = xgb.XGBRegressor()
        model.load_model(
            bytearray(
                s3.get_object(Bucket=bucket, Key=key)["Body"].read()
            )
        )
        return model
    
    return retry_with_backoff(_load, retries=retries,
                               critical_name=f"Model load: {key}",
                               run_id=run_id)


def load_pickle_from_s3_with_retry(bucket, key, run_id=None, retries=3):
    """Load pickle file from S3 with retry logic. CRITICAL if fails."""
    def _load():
        obj = s3.get_object(Bucket=bucket, Key=key)
        return joblib.load(BytesIO(obj["Body"].read()))
    
    return retry_with_backoff(_load, retries=retries,
                               critical_name=f"Pickle load: {key}",
                               run_id=run_id)


# ============================================================
# SAFE LABEL ENCODING
# ============================================================
def safe_label_transform(encoder, series):
    """Transform labels safely with unknown handling."""
    series = series.astype(str)
    known = set(encoder.classes_)
    series = series.apply(lambda x: x if x in known else "__UNK__")
    if "__UNK__" not in encoder.classes_:
        encoder.classes_ = np.append(encoder.classes_, "__UNK__")
    return encoder.transform(series)


# ============================================================
# ENHANCE LOAN RISK METRICS
# ============================================================
def enhance_loan_risk_metrics(df):
    """Enhance loan data with risk metrics."""
    df = df.copy()
    required_cols = ["close", "pred_credit_spread", "credit_spread",
                     "coupon_rate", "notional_usd", "loan_age_months",
                     "time_to_maturity_months"]
    for col in required_cols:
        if col not in df.columns:
            df[col] = np.nan

    df["loan_age_months"].fillna(0, inplace=True)
    df["time_to_maturity_months"].fillna(0, inplace=True)
    df["PD"] = np.clip(df["pred_credit_spread"] / 10, 0.01, 0.25)
    df["LGD"] = 0.45
    df["EAD"] = df["notional_usd"]
    df["Expected_Loss"] = df["PD"] * df["LGD"] * df["EAD"]
    df["carry_pnl_current"] = (df["coupon_rate"] * df["EAD"]) / 12
    df["carry_pnl_cumulative"] = df["carry_pnl_current"] * df["loan_age_months"]
    df["spread_diff"] = df["pred_credit_spread"] - df["credit_spread"]
    df["spread_pnl"] = df["spread_diff"] * df["EAD"] / 10000
    df["total_pnl"] = df["carry_pnl_current"] + df["spread_pnl"]
    df["RAROC"] = np.where(df["Expected_Loss"] > 0, df["total_pnl"] / df["Expected_Loss"], np.nan)
    df["PD_change_ratio"] = np.where(df["credit_spread"] > 0,
                                     df["pred_credit_spread"] / df["credit_spread"], 1.0)
    df["stage"] = np.where(df["PD_change_ratio"] > 1.5, 2, 1)
    df.drop(["spread_diff", "PD_change_ratio"], axis=1, inplace=True)
    return df


# ============================================================
# HARDEN TYPES FOR SNOWFLAKE
# ============================================================
def enforce_snowflake_types(df):
    """Enforce Snowflake-compatible data types."""
    df = df.copy()
    for col in ["date", "issue_date", "maturity_date"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime("%Y-%m-%d").fillna("")

    numeric_cols = df.select_dtypes(include=["number"]).columns
    df[numeric_cols] = df[numeric_cols].fillna(0)

    object_cols = df.select_dtypes(include=["object"]).columns
    df[object_cols] = df[object_cols].fillna("").astype(str)

    if "stage" in df.columns:
        df["stage"] = df["stage"].astype(str).fillna("0")

    df = df.reset_index(drop=True)
    return df



def drop_old_pipeline_metrics(df):
    """Drop first-pipeline observability columns that don't belong in the output."""
    drop_cols = [
        "pipeline_name",
        "pipeline_run_id",
        "data_source",
        "input_source",
        "transformation",
        "record_created_at",
        "run_mode",
    ]
    existing = [c for c in drop_cols if c in df.columns]
    return df.drop(columns=existing)

def drop_metadata_for_serving(df):
    """Remove metadata columns for serving layer (Postgres)."""
    drop_cols = [
        "pipeline_name",
        "pipeline_run_id",
        "data_source",
        "input_source",
        "transformation",
        "record_created_at",
        "run_mode",
    ]
    return df.drop(columns=[c for c in drop_cols if c in df.columns])


def add_pipeline_metadata(df, run_id, mode):
    """Add pipeline metadata to DataFrame."""
    df["pipeline_name"] = "loans_model_pipeline"
    df["pipeline_run_id"] = run_id
    df["run_mode"] = mode
    df["data_source"] = "s3 + snowflake"
    df["input_source"] = "loan_enriched + macro"
    df["transformation"] = "loan_model_v1"
    df["record_created_at"] = datetime.utcnow()
    return df


def remove_run_mode_from_clean(df):
    """Remove run_mode column for clean table (history-only column)."""
    if "run_mode" in df.columns:
        return df.drop(columns=["run_mode"])
    return df


# ============================================================
# WATERMARK HELPERS
# ============================================================
def get_watermark_from_clean_table(table_name, run_id=None):
    """
    Get watermark (latest processed month) from LOANS clean table.
    Returns Period or None if table empty.
    """
    try:
        with get_snowflake_conn() as ctx:
            cur = ctx.cursor()
            cur.execute(f'SELECT COALESCE(MAX("date"), \'1900-01-01\') FROM "{table_name}"')
            last_date = cur.fetchone()[0]
            
            if last_date and last_date != '1900-01-01':
                return pd.Period(last_date, freq="M")
            return None
            
    except Exception as exc:
        send_critical_alert(
            f"Snowflake watermark query failed for {table_name}",
            context={"run_id": run_id, "table": table_name, "error": str(exc)}
        )
        raise


# ============================================================
# MAIN PIPELINE (WITH DETERMINISTIC + TRANSACTIONAL WRITES)
# ============================================================
def run_loans_model_pipeline(
    start_date_override=None,
    replay=False,
    airflow_metadata=None,
):
    """
    Complete production-grade loans model pipeline with deterministic replay/backfill.
    
    Args:
        start_date_override: datetime or date string (YYYY-MM-DD) for backfill/replay start
        replay: If True, force full recompute from start_date_override
    
    Returns:
        str: Pipeline status
    
    Mode Determination:
        - replay=True: Full deterministic recompute from start_date_override
        - start_date_override provided: Backfill from start_date_override
        - both False: Incremental append-only mode
    
    Clean Table (LOANS):
        - Deterministic current-state table
        - NO run_mode column
        - NO duplicate replay states
        - TRANSACTIONAL delete+insert for replay/backfill
        - TRANSACTIONAL delete+insert for incremental (idempotent)
    
    History Table (LOANS_HISTORY):
        - Append-only
        - NEVER delete
        - ALWAYS append
        - Contains run_mode and full lineage
    
    Postgres Serving Layer:
        - Date-aware optimal strategy for MONTHLY data:
            * 1 month:  Append + trim to last 2 months
            * 2 months: Replace entire table (exactly 2 months, no trim)
            * >2 months: Trim to latest 2 months in memory, then replace
        - NEVER writes data just to delete it later
        - NO mode-specific logic needed
    """
    pipeline_start = time.time()
    run_ts = datetime.utcnow()
    run_id = run_ts.isoformat()
    np.random.seed(42)
    
    # ============================================================
    # MODE DETERMINATION
    # ============================================================
    if replay:
        if not start_date_override:
            raise ValueError("replay=True requires start_date_override")
        mode = "replay"
        print(f"\n{'='*66}")
        print(f"  LOANS MODEL PIPELINE - REPLAY MODE")
        print(f"  run_id={run_id}")
        print(f"  replay_start={start_date_override}")
        print(f"{'='*66}\n")
    elif start_date_override:
        mode = "backfill"
        print(f"\n{'='*66}")
        print(f"  LOANS MODEL PIPELINE - BACKFILL MODE")
        print(f"  run_id={run_id}")
        print(f"  backfill_start={start_date_override}")
        print(f"{'='*66}\n")
    else:
        mode = "incremental"
        print(f"\n{'='*66}")
        print(f"  LOANS MODEL PIPELINE - INCREMENTAL MODE")
        print(f"  run_id={run_id}")
        print(f"{'='*66}\n")
    
    # Convert start_date_override to period if provided
    recompute_start_period = None
    if start_date_override:
        if isinstance(start_date_override, str):
            start_date_override = pd.to_datetime(start_date_override)
        recompute_start_period = pd.Period(start_date_override, freq="M")
    
    snowflake_clean_rows = 0
    snowflake_history_rows = 0
    postgres_success = False
    macro_loaded = True
    
    try:
        # ============================================================
        # STEP 1: Load loan enriched data from S3 (CRITICAL) and drop old metrics
        # ============================================================
        print("[STEP 1] Loading loan enriched data from S3...")
        
        loans = load_parquet_from_s3_with_retry(
            S3_BUCKET,
            "loan_enriched_fx_bonds_commod_derivatives_collateral.parquet",
            run_id=run_id,
            retries=3
        )
        print(f"  Loaded {len(loans)} rows")
        loans = drop_old_pipeline_metrics(loans)
        
        # ============================================================
        # STEP 2: Validate dates
        # ============================================================
        print("[STEP 2] Validating dates...")
        
        for col in ["date", "issue_date", "maturity_date"]:
            if col in loans.columns:
                loans[col] = pd.to_datetime(loans[col], errors="coerce")
        
        loans = loans[loans["date"].notna()]
        
        today_period = pd.Period(pd.Timestamp.today(), freq="M")
        loans = loans[loans["date"].dt.to_period("M") <= today_period]
        print(f"  Rows after date filtering: {len(loans)}")
        
        # ============================================================
        # STEP 3: Load and merge macro data (SOFT FAIL - continues without)
        # ============================================================
        print("[STEP 3] Loading and merging macro data...")
        
        try:
            macro = load_csv_from_s3_with_retry(S3_BUCKET, "macro_data.csv", run_id=run_id, retries=2)
            macro["date"] = pd.to_datetime(macro["date"], errors="coerce")
            macro = macro[macro["date"].notna()]
            
            loans["month_year"] = loans["date"].dt.to_period("M").astype(str)
            macro["month_year"] = macro["date"].dt.to_period("M").astype(str)
            
            macro = macro.drop(columns=["date"]).drop_duplicates(subset=["month_year"])
            loans = loans.merge(macro, on="month_year", how="left").drop(columns=["month_year"])
            
            print("  Macro data merged successfully")
        except Exception as e:
            macro_loaded = False
            print(f"  WARNING: Macro data processing failed: {e} (continuing without macro)")
        
        # ============================================================
        # STEP 4: Determine months to process based on mode
        # ============================================================
        print("[STEP 4] Determining months to process...")
        
        # Get all available months in the data
        all_months = loans["date"].dt.to_period("M").sort_values().unique()
        
        if mode in ["replay", "backfill"]:
            # For replay/backfill: process ALL months >= start_date_override
            if recompute_start_period:
                months_to_process = [m for m in all_months if m >= recompute_start_period]
                print(f"  Replay/backfill mode: processing {len(months_to_process)} months >= {recompute_start_period}")
            else:
                months_to_process = list(all_months)
                print(f"  Replay/backfill mode: processing all {len(months_to_process)} months")
        else:
            # Incremental mode: get watermark from LOANS clean table
            watermark = get_watermark_from_clean_table(OUTPUT_TABLE, run_id=run_id)
            if watermark:
                months_to_process = [m for m in all_months if m > watermark]
                print(f"  Incremental mode: processing {len(months_to_process)} months > {watermark}")
            else:
                months_to_process = list(all_months)
                print(f"  Incremental mode: no watermark found - processing all {len(months_to_process)} months")
        
        if not months_to_process:
            print("  No months to process")
            processing_time = round(time.time() - pipeline_start, 2)
            print(f"\n{'='*66}")
            print(f"  LOANS PIPELINE COMPLETE - No new data")
            print(f"  Duration: {processing_time}s")
            print(f"{'='*66}\n")
            return "NO_NEW_DATA"
        
        # Filter loans to only months we need to process
        months_set = set(months_to_process)
        loans = loans[loans["date"].dt.to_period("M").isin(months_set)]
        print(f"  Filtered to {len(loans)} rows for {len(months_to_process)} months")
        
        # ============================================================
        # STEP 5: Feature Engineering
        # ============================================================
        print("[STEP 5] Feature engineering...")
        
        loans["spread_rate"] = loans.get("spread_bps", 0) / 10000.0
        loans["loan_age_months"] = ((loans["date"] - loans["issue_date"]).dt.days / 30).clip(lower=0)
        loans["time_to_maturity_months"] = ((loans["maturity_date"] - loans["date"]).dt.days / 30).clip(lower=0)
        loans["interest_rate_monthly"] = (loans.get("coupon_rate", 0) + loans["spread_rate"]) / 12.0
        loans["interest_income"] = loans.get("notional_usd", 0) * loans["interest_rate_monthly"]
        
        if "collateral_value" not in loans.columns and "exposure_before_collateral" in loans.columns:
            loans["collateral_value"] = loans["exposure_before_collateral"] * loans.get("collateral_ratio", 0)
        loans["exposure_pct_collateralized"] = (
            loans.get("collateral_value", 0) / loans.get("exposure_before_collateral", 1)
        ).replace([np.inf, -np.inf], np.nan).fillna(0)
        
        # ============================================================
        # STEP 6: Rolling features
        # ============================================================
        print("[STEP 6] Computing rolling features...")
        
        loans = loans.sort_values(["loan_id", "date"])
        WINDOW = 3
        
        for col, new_col in [("credit_spread", "cs_roll_std"),
                             ("fx_volatility", "fxv_roll_std"),
                             ("vol_20d", "cmd_roll_std")]:
            if col in loans.columns:
                loans[new_col] = loans.groupby("loan_id")[col].transform(lambda s: s.rolling(WINDOW, min_periods=2).std())
                mu, sd = loans[new_col].mean(), loans[new_col].std(ddof=0)
                loans[new_col] = (loans[new_col] - mu) / sd if sd > 0 else 0
            else:
                loans[new_col] = 0
        
        loans["volatility_index"] = loans[["cs_roll_std", "fxv_roll_std", "cmd_roll_std"]].mean(axis=1)
        
        # ============================================================
        # STEP 7: Load ML model, features, and encoders (CRITICAL)
        # ============================================================
        print("[STEP 7] Loading ML model, features, and encoders...")
        
        model = load_model_from_s3_with_retry(S3_BUCKET, "loans_model_creditspread_xgb.json", run_id=run_id, retries=2)
        print("  Model loaded successfully")
        
        features = load_pickle_from_s3_with_retry(S3_BUCKET, "loans_features.pkl", run_id=run_id, retries=2)
        print(f"  Features loaded: {len(features)} features")
        
        encoders = load_pickle_from_s3_with_retry(S3_BUCKET, "loans_label_encoders.pkl", run_id=run_id, retries=2)
        print(f"  Encoders loaded: {len(encoders)} encoders")
        
        # ============================================================
        # STEP 8: Safe model prediction (WARNING, not HARD FAIL)
        # ============================================================
        print("[STEP 8] Running model prediction...")
        
        loans_orig = loans.copy()
        
        # Apply label encoders safely
        for col, enc in encoders.items():
            if col in loans.columns:
                loans[col] = safe_label_transform(enc, loans[col])
        
        # Add missing features with default value 0
        for f in features:
            if f not in loans.columns:
                loans[f] = 0
        
        # Check for nulls and fill with 0
        null_cols = [f for f in features if f in loans.columns and loans[f].isna().any()]
        if null_cols:
            print(f"  WARNING: Null values in features: {null_cols[:5]} (filling with 0)")
            for col in null_cols:
                loans[col] = loans[col].fillna(0)
        
        # Predict
        try:
            loans["pred_credit_spread"] = model.predict(loans[features].values)
            print("  Model prediction completed")
        except Exception as e:
            print(f"  WARNING: Model prediction failed: {e} (using default spread)")
            loans["pred_credit_spread"] = loans.get("credit_spread", 0.05)
        
        # Restore original encoded columns
        for col in encoders.keys():
            if col in loans_orig.columns:
                loans[col] = loans_orig[col]
        
        # ============================================================
        # STEP 9: Enhance risk metrics
        # ============================================================
        print("[STEP 9] Enhancing risk metrics...")
        loans = enhance_loan_risk_metrics(loans)
        
        # Remove duplicate business keys created during joins
        loans = (
            loans
            .sort_values("date")
            .drop_duplicates(
                subset=["loan_id", "date"],
                keep="last",
            )
        )
        
        # ============================================================
        # STEP 10: Strict schema projection
        # ============================================================
        print("[STEP 10] Projecting to final schema...")
        
        allowed_columns = [
            "loan_id", "ticker", "sector", "industry", "currency", "date", "issue_date", "maturity_date",
            "rate_type", "coupon_rate", "spread_bps", "spread_rate", "notional_usd", "credit_rating",
            "credit_spread", "yield_to_maturity", "fx_rate", "fx_volatility", "carry_daily", "close",
            "vol_20d", "gdp", "unrate", "cpi", "fedfunds", "loan_age_months", "time_to_maturity_months",
            "interest_income", "exposure_pct_collateralized", "macro_stress_score", "volatility_index",
            "credit_spread_ratio", "profitability_ratio", "utilization_ratio", "counterparty", "funding_cost",
            "liquidity_score", "pred_credit_spread", "PD", "LGD", "EAD", "Expected_Loss",
            "carry_pnl_current", "carry_pnl_cumulative", "spread_pnl", "total_pnl", "RAROC", "stage",
        ]
        loans = loans[[c for c in allowed_columns if c in loans.columns]]
        loans = loans.loc[:, ~loans.columns.duplicated()]
        
        # ============================================================
        # STEP 11: Add pipeline metadata (includes run_mode for history)
        # ============================================================
        print("[STEP 11] Adding pipeline metadata...")
        loans_with_metadata = add_pipeline_metadata(loans, run_id, mode)
        
        # ============================================================
        # STEP 12: Write to Snowflake HISTORY (append-only, includes run_mode)
        # ============================================================
        print("[STEP 12] Writing to Snowflake HISTORY (append-only)...")
        
        loans_history = enforce_snowflake_types(loans_with_metadata)
        
        if not loans_history.empty:
            history_rows = write_to_snowflake_history(
                loans_history,
                HISTORY_TABLE,
                run_id=run_id,
                chunk_size=100000
            )
            snowflake_history_rows = history_rows
            print(f"  History table: {history_rows} rows appended")
        
        # ============================================================
        # STEP 13: Write to Snowflake CLEAN (TRANSACTIONAL delete+insert)
        # ============================================================
        print("[STEP 13] Writing to Snowflake CLEAN (transactional)...")
        
        # Remove run_mode for clean table
        loans_clean = remove_run_mode_from_clean(loans_with_metadata.copy())
        loans_clean = enforce_snowflake_types(loans_clean)
        
        if not loans_clean.empty:
            # For ALL modes (incremental, backfill, replay), use transactional delete+insert
            # This ensures idempotency and prevents duplicate states
            clean_rows = transactional_delete_insert_snowflake_clean(
                loans_clean,
                OUTPUT_TABLE,
                months_to_process,  # Always replace exactly the months we're processing
                run_id=run_id,
                chunk_size=100000
            )
            snowflake_clean_rows = clean_rows
            print(f"  Clean table: {clean_rows} rows written (transactional replace for {len(months_to_process)} months)")
        
        # ══════════════════════════════════════════════════════════════
        # STEP 14 — Write to Postgres
        # Rules based on unique months in DataFrame:
        #   - 1 month:  Append + trim to last 2 months
        #   - 2 months: Replace entire table (no trim needed)
        #   - >2 months: Trim to latest 2 months in memory, then replace
        # Failure here -> pipeline FAILS (consistency-first).
        # ══════════════════════════════════════════════════════════════
        print("[STEP 14] Writing to Postgres...")

        # Prepare for Postgres (drop ALL metadata columns including run_mode)
        df_pg = drop_metadata_for_serving(loans_with_metadata.copy())

        # Type fixes for Postgres
        if "stage" in df_pg.columns:
            df_pg["stage"] = df_pg["stage"].astype(float).round().astype(int)

        for c in ["loan_age_months", "time_to_maturity_months"]:
            if c in df_pg.columns:
                df_pg[c] = df_pg[c].fillna(0).astype(float).round().astype(int)

  

        # ONE FUNCTION CALL - handles everything (unique months check, trim, delete, insert)
        write_to_postgres(df_pg, mode=mode)
        postgres_success = True
        
        # ============================================================
        # STEP 15: Success - Send COMPLETORY Slack Alert
        # ============================================================
        processing_time = round(time.time() - pipeline_start, 2)
        pg_rows = len(df_pg) if 'df_pg' in locals() else 0
        
        # Send SUCCESS alert to Slack (FINAL pipeline completed)
        if postgres_success:
            send_success_alert({
                "run_id": run_id,
                "mode": mode,
                "snowflake_rows": snowflake_clean_rows,
                "history_rows": snowflake_history_rows,
                "postgres_status": "SUCCESS",
                "postgres_rows": pg_rows,
                "macro_loaded": macro_loaded,
                "duration_seconds": processing_time
            })
        else:
            # Still send success but note Postgres failure
            print(f"  WARNING: Postgres failed but Snowflake succeeded - sending partial success alert")
            send_success_alert({
                "run_id": run_id,
                "mode": mode,
                "snowflake_rows": snowflake_clean_rows,
                "history_rows": snowflake_history_rows,
                "postgres_status": f"FAILED",
                "postgres_rows": pg_rows,
                "macro_loaded": macro_loaded,
                "duration_seconds": processing_time
            })
        
        print(f"\n{'='*66}")
        print(f"  LOANS PIPELINE SUCCESS - run_id={run_id}")
        print(f"  Mode: {mode}")
        print(f"  Snowflake clean rows: {snowflake_clean_rows}")
        print(f"  Snowflake history rows: {snowflake_history_rows}")
        print(f"  Postgres: {'SUCCESS' if postgres_success else 'FAILED'} ({pg_rows} rows)")
        print(f"  Macro loaded: {macro_loaded}")
        print(f"  Months processed: {len(months_to_process)}")
        print(f"  Duration: {processing_time}s")
        print(f"{'='*66}\n")
        
        # Write run summary to S3
        run_summary = {
            "pipeline_run_id": run_id,
            "pipeline_name": "loans_model_pipeline",
            "run_ts": run_ts.isoformat(),
            "status": "SUCCESS",
            "mode": mode,
            "start_date": str(start_date_override) if start_date_override else None,
            "rows_processed_clean": snowflake_clean_rows,
            "rows_processed_history": snowflake_history_rows,
            "postgres_rows": pg_rows,
            "processing_time_s": processing_time,
            "macro_loaded": macro_loaded,
            "months_processed": [str(m) for m in months_to_process],
            "tables_written": [
                OUTPUT_TABLE,
                HISTORY_TABLE,
                "loan_data",
            ],
            "airflow": airflow_metadata,
        }
        write_run_summary(run_summary)
        
        return f"SUCCESS_{mode}_CLEAN_{snowflake_clean_rows}_HISTORY_{snowflake_history_rows}_PG_{pg_rows}"
    
    except Exception as e:
        processing_time = round(time.time() - pipeline_start, 2)
        
        # Send CRITICAL alert for any unhandled exception
        send_critical_alert(
            f"Pipeline failed with unhandled exception",
            context={"run_id": run_id, "mode": mode, "error": str(e)}
        )
        
        print(f"\n{'='*66}")
        print(f"  LOANS PIPELINE FAILED - run_id={run_id}")
        print(f"  Mode: {mode}")
        print(f"  Error: {str(e)}")
        print(f"  Duration: {processing_time}s")
        print(f"{'='*66}\n")
        
        # Write failure summary to S3
        run_summary = {
            "pipeline_run_id": run_id,
            "pipeline_name": "loans_model_pipeline",
            "run_ts": run_ts.isoformat(),
            "status": "FAILED",
            "mode": mode,
            "start_date": str(start_date_override) if start_date_override else None,
            "processing_time_s": processing_time,
            "error": str(e),
            "airflow": airflow_metadata,
        }
        try:
            write_run_summary(run_summary)
        except Exception:
            pass  # Don't mask the original exception
        
        # Re-raise for Airflow to catch
        raise