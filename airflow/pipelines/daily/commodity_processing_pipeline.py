"""
commodity_processing_pipeline.py
==================================
Production-grade commodity processing pipeline — consistency-first architecture.

This is the SECOND commodity pipeline. It consumes the rolling commodity
features file written by commodity_update_pipeline.py (first pipeline),
merges it with MTM data from the EQUITY Snowflake table, applies sector-
commodity exposure logic, merges macro data, runs XGBoost predictions, and
writes results to two Snowflake tables and a Postgres serving layer.

Storage architecture
--------------------
Snowflake COMMODITY_HISTORY  (append-only, audit trail)
    - Always INSERT; never DELETE or UPDATE
    - Includes run_mode column for lineage tracing
    - Duplicates across runs are EXPECTED and intentional

Snowflake COMMODITY
    - Replay / Backfill:  DELETE window -> INSERT fresh rows (transactional)
    - Incremental:        MERGE latest-state rows using business key
                          (asset_manager, ticker, commodity, date)
    - Must NEVER contain conflicting rows for same (ticker, commodity, date)

Postgres commodity_data      (serving layer — NOT source of truth)
    - Rules:
        * 1 unique date:  Append + trim to last 2 days
        * 2 unique dates: Replace entire table (no trim needed)
        * >2 unique dates: Trim to latest 2 days in memory, then replace
    - Never writes data just to delete it later

Failure semantics
-----------------
Pipeline is consistency-first, NOT availability-first.
  - Snowflake HISTORY failure -> FAIL
  - Snowflake CLEAN failure   -> FAIL
  - Postgres failure          -> FAIL
Partial success is NOT allowed under any mode.

Mode detection
--------------
Derived exclusively from function parameters. Never inferred from data columns.
    replay=True                   -> mode = "replay"
    start_date_override provided  -> mode = "backfill"
    default                       -> mode = "incremental"

DAG integration
---------------
master_dag.py calls process_commodities(start_date_override, replay).
Signature matches exactly.
"""

import os
import time
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import joblib
import xgboost as xgb
from connections.snowflake_conn import get_snowflake_conn
from snowflake.connector.pandas_tools import write_pandas
import boto3
from io import BytesIO, StringIO
from connections.postgre_conn import get_postgre_conn
import requests
import json
from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

# =============================================================
# CONSTANTS
# =============================================================
S3_BUCKET = "yeeshu-commodity-bucket"
s3 = boto3.client("s3")

BASE_PREFIX    = "historical-commodity/"
ROLLING_PREFIX = BASE_PREFIX + "rolling/"
MODEL_PREFIX   = "models/"

INPUT_COMMOD = ROLLING_PREFIX + "commodities_70d.parquet"
SYM          = BASE_PREFIX + "unique_tickers_sector.csv"

MACRO_BUCKET   = "yeeshu-loan-bucket"
MACRO_KEY        = "macro_data.csv"

MODEL_FILE   = MODEL_PREFIX + "commodities_model_vol21_xgb.json"
FEATURE_FILE = MODEL_PREFIX + "commodities_features_vol21.pkl"

# Snowflake table names — single source of truth for table identifiers
SNOWFLAKE_CLEAN_TABLE   = "COMMODITY"          # deterministic latest-state table
SNOWFLAKE_HISTORY_TABLE = "COMMODITY_HISTORY"  # append-only audit/lineage table


# =============================================================
# PRODUCTION-GRADE ALERTING (CRITICAL ONLY)
# =============================================================
def send_critical_alert(message: str, context: dict = None):
    """
    Send CRITICAL alert to Slack only for unrecoverable pipeline failures.

    Triggers for:
    - Snowflake write failure (HISTORY or CLEAN)
    - Postgres write failure
    - S3 data load failure (after all retries)
    - Complete pipeline failure

    Does NOT trigger for:
    - Individual retry attempts (only on final exhaustion)
    - Model/feature load failures (soft-fail: prediction column falls back
      to historical volatility, never blocks the pipeline)
    """
    payload = {
        "level":    "CRITICAL",
        "pipeline": "commodity_processing_pipeline",
        "message":  message,
        "context":  context or {},
        "timestamp": datetime.utcnow().isoformat(),
    }

    # Always emit to stdout for log aggregation (CloudWatch, Datadog, etc.)
    print(f"[CRITICAL] {message} | context={payload['context']}")

    webhook = os.getenv("SLACK_WEBHOOK_URL")
    if webhook:
        try:
            truncated = message[:500] + "..." if len(message) > 500 else message
            run_id    = context.get("run_id", "unknown") if context else "unknown"
            text = (
                f"*[CRITICAL]* commodity_processing_pipeline\n"
                f"{truncated}\n"
                f"run_id: {run_id}"
            )
            requests.post(webhook, json={"text": text}, timeout=3)
        except Exception as e:
            print(f"[ALERT ERROR] Slack notification failed: {e}")


# =============================================================
# RUN SUMMARY WRITER
# =============================================================
def write_run_summary(summary):
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
            - end_date
            - rows_processed
            - processing_time_s
            - tables_written (list)
            - error (if failed)
    """
    key = (
        f"historical-commodity/commodity-risk/metadata/"
        f"pipeline_run_id={summary['pipeline_run_id']}/"
        f"run_summary.json"
    )

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

# =============================================================
# RETRY WITH EXPONENTIAL BACKOFF
# =============================================================
def retry_with_backoff(
    func,
    retries=3,
    backoff_factor=2,
    exceptions=(Exception,),
    critical_name=None,
    run_id=None,
):
    """
    Retry a callable with exponential backoff.

    Sends CRITICAL alert only when all retries are exhausted AND
    critical_name is provided. Individual retry attempts log to stdout only.

    Args:
        func:           Zero-argument callable to retry.
        retries:        Maximum number of retry attempts (not counting first).
        backoff_factor: Wait = backoff_factor ** attempt.
        exceptions:     Exception types to catch and retry.
        critical_name:  Human-readable operation name for alert messages.
                        Pass None for non-critical / soft-fail operations.
        run_id:         Pipeline run identifier for alert context.

    Returns:
        Return value of func() on success.

    Raises:
        Last caught exception when all retries are exhausted.
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

    if critical_name:
        send_critical_alert(
            f"{critical_name} failed after {retries} retries",
            context={"run_id": run_id, "error": str(last_exception)},
        )

    raise last_exception


# =============================================================
# S3 / MODEL LOAD HELPERS
# =============================================================
def load_csv_from_s3_with_retry(bucket, key, parse_dates=None, dayfirst=True, retries=3, run_id=None):
    """Load CSV from S3 with retry logic. CRITICAL if all retries fail."""
    def _load():
        obj = s3.get_object(Bucket=bucket, Key=key)
        return pd.read_csv(BytesIO(obj["Body"].read()), parse_dates=parse_dates, dayfirst=dayfirst)

    return retry_with_backoff(
        _load,
        retries=retries,
        critical_name=f"S3 CSV load: {key}",
        run_id=run_id,
    )


def load_parquet_from_s3_with_retry(bucket, key, retries=3, run_id=None):
    """Load parquet from S3 with retry logic. CRITICAL if all retries fail."""
    def _load():
        obj = s3.get_object(Bucket=bucket, Key=key)
        return pd.read_parquet(BytesIO(obj["Body"].read()))

    return retry_with_backoff(
        _load,
        retries=retries,
        critical_name=f"S3 parquet load: {key}",
        run_id=run_id,
    )


def get_latest_feature_keys(start_date, end_date):
    """
    Return latest feature parquet key per date.

    For each date partition:
    - list all run_id folders
    - pick latest modified parquet
    - return only latest version
    """

    paginator = s3.get_paginator("list_objects_v2")

    latest_per_date = {}

    current = pd.Timestamp(start_date)

    while current <= pd.Timestamp(end_date):

        prefix = (
            f"{BASE_PREFIX}features/"
            f"year={current.year}/"
            f"month={current.month:02d}/"
            f"day={current.day:02d}/"
        )

        latest_obj = None

        for page in paginator.paginate(
            Bucket=S3_BUCKET,
            Prefix=prefix,
        ):

            for obj in page.get("Contents", []):

                key = obj["Key"]

                if not key.endswith(".parquet"):
                    continue

                if (
                    latest_obj is None or
                    obj["LastModified"] > latest_obj["LastModified"]
                ):
                    latest_obj = obj

        if latest_obj:
            latest_per_date[current.date()] = latest_obj["Key"]

        current += timedelta(days=1)

    return list(latest_per_date.values())


def load_commodity_features_from_layer2(
    start_date,
    end_date,
    run_id=None,
):

    keys = get_latest_feature_keys(start_date, end_date)

    if not keys:
        return pd.DataFrame()

    frames = []

    for key in keys:

        print(f"  [LAYER2] Loading: {key}")

        df_part = load_parquet_from_s3_with_retry(
            S3_BUCKET,
            key,
            run_id=run_id,
        )

        frames.append(df_part)

    commod_base = pd.concat(frames, ignore_index=True)

    commod_base["date"] = pd.to_datetime(
        commod_base["date"],
        utc=True,
    )

    return commod_base

def load_model_from_s3_with_retry(bucket, key, retries=2):
    """Load XGBoost model from S3. Soft-fail (no critical alert)."""
    def _load():
        model_obj = s3.get_object(Bucket=bucket, Key=key)
        booster   = xgb.Booster()
        booster.load_model(bytearray(model_obj["Body"].read()))
        return booster

    # No critical_name -> soft fail; pipeline continues without predictions
    return retry_with_backoff(_load, retries=retries)


def load_features_from_s3_with_retry(bucket, key, retries=2):
    """Load feature list from S3. Soft-fail (no critical alert)."""
    def _load():
        feat_obj = s3.get_object(Bucket=bucket, Key=key)
        return joblib.load(BytesIO(feat_obj["Body"].read()))

    return retry_with_backoff(_load, retries=retries)


# =============================================================
# METADATA HELPERS (UNCHANGED)
# =============================================================
def rename_source_lineage(df):
    """Rename first-pipeline lineage columns to source_* namespace."""
    rename_map = {
        "pipeline_name":     "source_pipeline",
        "pipeline_run_id":   "source_run_id",
        "data_source":       "source_data_source",
        "input_source":      "source_input_source",
        "transformation":    "source_transformation",
        "record_created_at": "source_created_at",
        "feature_version":   "source_feature_version",
        "schema_version":    "source_schema_version",
        "schema_hash":       "source_schema_hash",
    }
    existing = {k: v for k, v in rename_map.items() if k in df.columns}
    return df.rename(columns=existing)


def drop_old_pipeline_metrics(df):
    """Drop first-pipeline observability columns that don't belong in the output."""
    drop_cols = [
        "data_date",
        "ingestion_start_date",
        "ticker_universe_size",
        "input_rows",
        "output_rows",
        "processing_time_s",
        "replay_mode",
        "outlier_flag",
        "record_id",
        "run_mode",
    ]
    existing = [c for c in drop_cols if c in df.columns]
    return df.drop(columns=existing)


def drop_metadata_for_serving(df):
    """Strip all pipeline lineage columns before writing to Postgres serving layer."""
    drop_cols = [
        "source_pipeline", "source_run_id", "source_data_source",
        "source_input_source", "source_transformation", "source_created_at",
        "source_feature_version", "source_schema_version", "source_schema_hash",
        "pipeline_name", "pipeline_run_id", "data_source", "input_source",
        "transformation", "record_created_at", "data_quality_flag"
    ]
    return df.drop(columns=[c for c in drop_cols if c in df.columns])


# =============================================================
# SNOWFLAKE HISTORY TABLE (append-only, always INSERT)
# =============================================================
def write_to_snowflake_history(df, run_mode, run_id=None, chunk_size=20_000):
    """
    Append rows to COMMODITY_HISTORY. Never updates or deletes existing rows.

    Adds a run_mode column so every row is traceable to the exact pipeline
    run that produced it. Duplicates across runs are expected by design —
    this table is an immutable audit trail.

    Raises on failure (after retries). Pipeline does NOT continue if this
    write fails.

    Args:
        df:         Fully enriched and transformed DataFrame.
        run_mode:   One of "incremental", "backfill", "replay".
        run_id:     Pipeline run identifier for alert context.
        chunk_size: Rows per write_pandas chunk.
    """
    df_hist             = df.copy()
    df_hist["run_mode"] = run_mode   # lineage column — history table only

    def do_insert():
        with get_snowflake_conn() as ctx:
            success, nchunks, nrows, _ = write_pandas(
                ctx,
                df_hist,
                SNOWFLAKE_HISTORY_TABLE,
                chunk_size=chunk_size,
                quote_identifiers=True,
                use_logical_type=True,
            )
            if not success:
                raise RuntimeError(
                    f"write_pandas returned success=False for {SNOWFLAKE_HISTORY_TABLE}"
                )
            print(f"  [HISTORY] Inserted {nrows:,} rows into {SNOWFLAKE_HISTORY_TABLE}")
            return nrows

    try:
        return retry_with_backoff(
            do_insert,
            retries=3,
            critical_name=f"Snowflake HISTORY insert ({SNOWFLAKE_HISTORY_TABLE})",
            run_id=run_id,
        )
    except Exception as exc:
        send_critical_alert(
            f"Snowflake HISTORY insert failed for table {SNOWFLAKE_HISTORY_TABLE}",
            context={
                "run_id": run_id,
                "table":  SNOWFLAKE_HISTORY_TABLE,
                "rows":   len(df),
                "error":  str(exc),
            },
        )
        raise


# =============================================================
# SNOWFLAKE CLEAN TABLE — DELETE + INSERT (replay / backfill)
# =============================================================
def _snowflake_clean_delete_insert(
    df,
    start_date,
    end_date,
    run_id=None,
    chunk_size=20_000,
):
    """
    Atomically replace a date window in the COMMODITY clean table.

    Architecture
    ------------
    COMMODITY_HISTORY:
        append-only audit table
        duplicates across runs are expected and allowed

    COMMODITY:
        deterministic latest-state table
        must NEVER contain conflicting rows for the same business key

    Replay/backfill semantics
    -------------------------
    Replay and backfill recompute an entire historical window and replace
    that window atomically inside the clean table.

    Protocol
    --------
    1. Create a TEMP staging table.
    2. Bulk load dataframe into staging via write_pandas().
    3. BEGIN explicit Snowflake transaction.
    4. DELETE existing rows for target replay/backfill window.
    5. INSERT recomputed rows from staging table.
    6. COMMIT transaction.
    7. ROLLBACK automatically on any failure.

    Guarantees
    ----------
    - No partial replay state.
    - No conflicting clean-table rows.
    - Deterministic replay/backfill behavior.
    - Temp staging table disappears automatically after session close.

    Important Notes
    ---------------
    write_pandas() itself is NOT transactional because Snowflake internally
    uses staged COPY operations. Therefore staging must occur BEFORE opening
    the explicit transaction.

    Args
    ----
    df:
        Fully processed dataframe ready for Snowflake clean-table write.

    start_date:
        Inclusive replay/backfill window start.

    end_date:
        Inclusive replay/backfill window end.

    run_id:
        Pipeline run identifier for observability and alerting.

    chunk_size:
        write_pandas batch size.

    Raises
    ------
    Exception:
        Re-raised after rollback and critical alert emission.
    """

    start_str = (
        pd.Timestamp(start_date)
        .strftime("%Y-%m-%d")
    )

    end_str = (
        pd.Timestamp(end_date)
        .strftime("%Y-%m-%d")
    )

    temp_table = (
        f"{SNOWFLAKE_CLEAN_TABLE}_STAGE_"
        f"{int(time.time())}"
    )

    try:

        with get_snowflake_conn() as ctx:

            with ctx.cursor() as cs:

                # =====================================================
                # STEP 1 — CREATE TEMP STAGING TABLE
                # =====================================================

                cs.execute(f"""
                    CREATE TEMPORARY TABLE {temp_table}
                    LIKE "{SNOWFLAKE_CLEAN_TABLE}"
                """)

                # =====================================================
                # STEP 2 — LOAD DATAFRAME INTO STAGING TABLE
                # =====================================================

                write_pandas(
                    conn=ctx,
                    df=df,
                    table_name=temp_table,
                    chunk_size=chunk_size,
                    quote_identifiers=True,
                    auto_create_table=False,
                    use_logical_type=True,
                )

                # =====================================================
                # STEP 3 — OPEN EXPLICIT TRANSACTION
                # =====================================================

                cs.execute("BEGIN")

                try:

                    # =================================================
                    # STEP 4 — DELETE EXISTING WINDOW
                    # =================================================

                    cs.execute(f"""
                        DELETE FROM "{SNOWFLAKE_CLEAN_TABLE}"
                        WHERE "date"
                        BETWEEN '{start_str}'::DATE
                            AND '{end_str}'::DATE
                    """)

                    deleted_rows = (
                        cs.rowcount
                        if cs.rowcount is not None
                        else 0
                    )

                    print(
                        f"  [CLEAN] Deleted "
                        f"{deleted_rows:,} rows from "
                        f"{SNOWFLAKE_CLEAN_TABLE} "
                        f"for window "
                        f"[{start_str}, {end_str}]"
                    )

                    # =================================================
                    # STEP 5 — INSERT RECOMPUTED WINDOW
                    # =================================================

                    cs.execute(f"""
                        INSERT INTO "{SNOWFLAKE_CLEAN_TABLE}"
                        SELECT *
                        FROM {temp_table}
                    """)

                    inserted_rows = (
                        cs.rowcount
                        if cs.rowcount is not None
                        else len(df)
                    )

                    print(
                        f"  [CLEAN] Inserted "
                        f"{inserted_rows:,} rows into "
                        f"{SNOWFLAKE_CLEAN_TABLE}"
                    )

                    # =================================================
                    # STEP 6 — COMMIT TRANSACTION
                    # =================================================

                    cs.execute("COMMIT")

                except Exception:

                    # =============================================
                    # STEP 7 — ROLLBACK ON FAILURE
                    # =============================================

                    cs.execute("ROLLBACK")

                    raise

    except Exception as exc:

        send_critical_alert(
            (
                "Snowflake CLEAN delete+insert failed "
                f"for window [{start_str}, {end_str}]"
            ),
            context={
                "run_id": run_id,
                "table": SNOWFLAKE_CLEAN_TABLE,
                "mode": "delete_insert",
                "start": start_str,
                "end": end_str,
                "rows": len(df),
                "error": str(exc),
            },
        )

        raise


# =============================================================
# SNOWFLAKE CLEAN TABLE — MERGE (incremental)
# =============================================================
def _snowflake_clean_merge(
    df,
    run_id=None,
    chunk_size=20_000,
):
    """
    Incremental UPSERT into COMMODITY clean table.

    Business key:
        (asset_manager, ticker, commodity, date)

    Guarantees:
    - deterministic latest-state rows
    - idempotent retries
    - replay overlap safety
    - no conflicting clean-table rows
    """

    key_columns = [
        "asset_manager",
        "ticker",
        "commodity",
        "date",
    ]

    # =========================================================
    # DUPLICATE VALIDATION
    # =========================================================

    dupes = (
        df.groupby(key_columns)
        .size()
        .reset_index(name="cnt")
    )

    dup_rows = dupes[dupes["cnt"] > 1]

    if not dup_rows.empty:

        sample = dup_rows.head(10)

        raise ValueError(
            "Duplicate commodity business keys detected "
            f"before MERGE. Sample:\n{sample}"
        )

    temp_table = (
        f"{SNOWFLAKE_CLEAN_TABLE}_STAGE_"
        f"{int(time.time())}"
    )

    try:

        with get_snowflake_conn() as ctx:

            with ctx.cursor() as cs:

                # =====================================================
                # CREATE TEMP STAGE TABLE
                # =====================================================

                cs.execute(f'''
                    CREATE TEMPORARY TABLE {temp_table}
                    LIKE "{SNOWFLAKE_CLEAN_TABLE}"
                ''')

                # =====================================================
                # LOAD INTO STAGE TABLE
                # =====================================================

                success, nchunks, nrows, _ = write_pandas(
                    conn=ctx,
                    df=df,
                    table_name=temp_table,
                    chunk_size=chunk_size,
                    quote_identifiers=True,
                    auto_create_table=False,
                    use_logical_type=True,
                )

                if not success:
                    raise RuntimeError(
                        "write_pandas failed for temp stage table"
                    )

                # =====================================================
                # BUILD MERGE SQL
                # =====================================================

                all_columns = list(df.columns)

                update_columns = [
                    c for c in all_columns
                    if c not in key_columns
                ]

                merge_on = " AND ".join([
                    f't."{c}" = s."{c}"'
                    for c in key_columns
                ])

                update_set = ", ".join([
                    f't."{c}" = s."{c}"'
                    for c in update_columns
                ])

                insert_columns = ", ".join([
                    f'"{c}"'
                    for c in all_columns
                ])

                insert_values = ", ".join([
                    f's."{c}"'
                    for c in all_columns
                ])

                merge_sql = f"""
                    MERGE INTO "{SNOWFLAKE_CLEAN_TABLE}" t
                    USING {temp_table} s
                    ON {merge_on}

                    WHEN MATCHED THEN
                        UPDATE SET
                            {update_set}

                    WHEN NOT MATCHED THEN
                        INSERT ({insert_columns})
                        VALUES ({insert_values})
                """

                # =====================================================
                # EXECUTE MERGE
                # =====================================================

                cs.execute(merge_sql)
                affected_rows = cs.rowcount if cs.rowcount is not None else len(df)

                print(
                    f"  [CLEAN] MERGE complete — "
                    f"{affected_rows:,} rows affected in "
                    f"{SNOWFLAKE_CLEAN_TABLE}"
                )

    except Exception as exc:

        send_critical_alert(
            "Snowflake CLEAN MERGE failed",
            context={
                "run_id": run_id,
                "table": SNOWFLAKE_CLEAN_TABLE,
                "mode": "merge",
                "rows": len(df),
                "error": str(exc),
            },
        )

        raise

# =============================================================
# SNOWFLAKE CLEAN TABLE — UNIFIED WRITE DISPATCHER
# =============================================================
def write_to_snowflake_clean(df, mode, start_date, end_date, run_id=None, chunk_size=20_000):
    """
    Route to the correct Snowflake CLEAN write strategy based on mode.

    - "replay" / "backfill": DELETE window then INSERT (atomic transaction).
    - "incremental":         MERGE (upsert on business key).

    Wraps the strategy call in retry_with_backoff. Raises on exhaustion.

    Args:
        df:         Fully processed DataFrame.
        mode:       "incremental" | "backfill" | "replay".
        start_date: Window start used for replay/backfill DELETE.
        end_date:   Window end used for replay/backfill DELETE.
        run_id:     Pipeline run identifier for alert context.
        chunk_size: Rows per write_pandas chunk.
    """
    if mode in ("replay", "backfill"):
        strategy_name = f"Snowflake CLEAN delete+insert ({mode})"

        def do_write():
            _snowflake_clean_delete_insert(df, start_date, end_date, run_id, chunk_size)

    else:  # incremental
        strategy_name = "Snowflake CLEAN MERGE (incremental)"

        def do_write():
            _snowflake_clean_merge(df, run_id, chunk_size)

    # Execute the write with retry logic for ALL modes
    # IMPORTANT: This must be outside the if-else block to work for both backfill AND incremental
    retry_with_backoff(
        do_write,
        retries=3,
        critical_name=strategy_name,
        run_id=run_id,
    )


# =============================================================
# POSTGRES SERVING LAYER (consistency-first: RAISES on failure)
# =============================================================

# Columns that must be stored as integers in Postgres
_PG_INTEGER_COLS = ["volume", "exposure_amount", "mtm_value"]


def write_to_postgres(df, mode, retries=3):
    """
    Write enriched commodity rows to the Postgres serving layer.

    Rules:
    - If incoming DF has >2 unique dates:
        -> trim in memory to latest 2 dates
    - If final DF has 2 unique dates:
        -> FULL REPLACE (DELETE + INSERT)
        -> NO trim needed
    - If final DF has 1 unique date:
        -> INCREMENTAL APPEND
        -> then trim table to latest 2 dates
    """

    if df.empty:
        print("  [POSTGRES] Empty DataFrame — nothing to write.")
        return

    # ─────────────────────────────────────────────
    # Prepare serving-layer DataFrame
    # ─────────────────────────────────────────────
    df_pg = drop_metadata_for_serving(df.copy())

    # ─────────────────────────────────────────────
    # Step 1: Trim in memory if >2 unique dates
    # ─────────────────────────────────────────────
    unique_dates = sorted(df_pg["date"].unique())

    if len(unique_dates) > 2:

        latest_2_dates = unique_dates[-2:]

        df_pg = df_pg[
            df_pg["date"].isin(latest_2_dates)
        ].copy()

        print(
            f"  [POSTGRES] Trimmed incoming DF "
            f"to latest 2 dates: {latest_2_dates}"
        )

    # Recalculate after trimming
    unique_dates = sorted(df_pg["date"].unique())
    unique_date_count = len(unique_dates)

    print(
        f"  [POSTGRES] Final incoming DF has "
        f"{unique_date_count} unique date(s)"
    )

    last_error = None

    for attempt in range(retries + 1):

        try:

            # Fresh copy per retry
            df_attempt = df_pg.copy()

            # ─────────────────────────────────────────────
            # Cast integer columns
            # ─────────────────────────────────────────────
            for col in _PG_INTEGER_COLS:

                if col in df_attempt.columns:

                    df_attempt[col] = (
                        df_attempt[col]
                        .fillna(0)
                        .astype(float)
                        .round()
                        .astype(int)
                    )

            with get_postgre_conn() as pg_conn:

                with pg_conn.cursor() as pg_cur:

                    # ─────────────────────────────────────────────
                    # Step 2: Validate live schema
                    # ─────────────────────────────────────────────
                    pg_cur.execute(
                        """
                        SELECT column_name
                        FROM information_schema.columns
                        WHERE table_schema = 'public'
                          AND table_name = 'commodity_data'
                        ORDER BY ordinal_position
                        """
                    )

                    pg_cols_order = [
                        row[0]
                        for row in pg_cur.fetchall()
                        if row[0].lower() != "id"
                    ]

                    if not pg_cols_order:

                        raise RuntimeError(
                            "Schema query returned 0 columns "
                            "for public.commodity_data"
                        )

                    missing = (
                        set(pg_cols_order)
                        - set(df_attempt.columns)
                    )

                    if missing:

                        raise ValueError(
                            f"DataFrame missing PG columns: "
                            f"{missing}"
                        )

                    # Exact column order
                    df_ordered = df_attempt[
                        pg_cols_order
                    ].copy()

                    # ─────────────────────────────────────────────
                    # Step 3: Write strategy
                    # ─────────────────────────────────────────────
                    if unique_date_count == 2:

                        # FULL REPLACE
                        pg_cur.execute(
                            "DELETE FROM public.commodity_data"
                        )

                        print(
                            "  [POSTGRES] Deleted all existing rows "
                            "(2-day refresh)"
                        )

                    elif unique_date_count == 1:

                        load_date = unique_dates[0]

                        if mode == "incremental":

                            print(
                                "  [POSTGRES] Incremental append mode "
                                "(1-day load)"
                            )

                        else:

                            pg_cur.execute(
                                """
                                DELETE FROM public.commodity_data
                                WHERE date = %s
                                """,
                                (load_date,)
                            )

                            deleted = (
                                pg_cur.rowcount
                                if pg_cur.rowcount is not None
                                else 0
                            )

                            print(
                                f"  [POSTGRES] Deleted {deleted:,} existing rows "
                                f"for {load_date} ({mode} mode)"
                            )

                    else:

                        raise ValueError(
                            f"Unexpected unique_date_count="
                            f"{unique_date_count}"
                        )

                    # ─────────────────────────────────────────────
                    # Step 4: Bulk INSERT via COPY
                    # ─────────────────────────────────────────────
                    columns_quoted = [
                        f'"{col}"'
                        for col in pg_cols_order
                    ]

                    copy_sql = (
                        f"COPY public.commodity_data "
                        f"({', '.join(columns_quoted)}) "
                        f"FROM STDIN WITH CSV"
                    )

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
                        unique_date_count == 1
                        and mode == "incremental"
                    ):

                        pg_cur.execute(
                            """
                                DELETE FROM public.commodity_data
                                WHERE date NOT IN (
                                    SELECT DISTINCT date 
                                    FROM public.commodity_data 
                                    ORDER BY date DESC 
                                    LIMIT 2
                                )
                            """
                        )

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
                            "  [POSTGRES] No trim needed"
                        )

                # Commit transaction
                pg_conn.commit()

            print(
                f"  [POSTGRES] Success — inserted "
                f"{len(df_ordered):,} rows from "
                f"{unique_date_count} unique date(s)"
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
        f"Postgres commodity write failed after {retries} retries",
        context={
            "error": str(last_error),
            "rows": len(df_pg),
        },
    )

    raise RuntimeError(
        f"Postgres write to public.commodity_data "
        f"failed after {retries} retries: "
        f"{last_error}"
    )

# =============================================================
# SECTOR -> COMMODITY MAPPING (UNCHANGED)
# =============================================================
SECTOR_TO_COMMODITIES = {
    "Energy":                 {"CL=F": 0.7, "NG=F": 0.3},
    "Basic Materials":        {"GC=F": 0.3, "SI=F": 0.3, "ZC=F": 0.4},
    "Industrials":            {"CL=F": 0.4, "GC=F": 0.6},
    "Consumer Defensive":     {"ZC=F": 0.6, "GC=F": 0.4},
    "Utilities":              {"NG=F": 0.8, "CL=F": 0.2},
    "Technology":             {"GC=F": 0.7, "SI=F": 0.3},
    "Healthcare":             {"SI=F": 0.4, "GC=F": 0.6},
    "Financial Services":     {"GC=F": 0.8, "CL=F": 0.2},
    "Real Estate":            {"GC=F": 0.5, "ZC=F": 0.5},
    "Communication Services": {"GC=F": 0.5, "CL=F": 0.5},
    "Consumer Cyclical":      {"CL=F": 0.6, "GC=F": 0.4},
}


# =============================================================
# PUSH PIPELINE METRICS
# =============================================================

def push_pipeline_metrics(
    pipeline_name: str,
    run_id: str,
    status: str,
    metrics: dict,
):
    registry = CollectorRegistry()

    labels = {
        "pipeline": pipeline_name,
        "run_id": run_id,
    }

    # SUCCESS / FAILED
    Gauge(
        "pipeline_status",
        "1=SUCCESS 0=FAILED",
        ["pipeline", "run_id"],
        registry=registry,
    ).labels(**labels).set(
        1 if status.upper() == "SUCCESS" else 0
    )

    # Dynamic metrics
    for metric_name, metric_value in metrics.items():
        Gauge(
            f"pipeline_{metric_name}",
            f"Pipeline metric: {metric_name}",
            ["pipeline", "run_id"],
            registry=registry,
        ).labels(**labels).set(metric_value)

    try:
        pushgateway_url = os.getenv("PUSHGATEWAY_URL")

        if not pushgateway_url:
            raise ValueError("PUSHGATEWAY_URL environment variable is not set")

        push_to_gateway(
            pushgateway_url,
            job=f"{pipeline_name}_{run_id}",
            registry=registry,
        )

        print(
            f"[PROMETHEUS] Metrics pushed successfully | "
            f"pipeline={pipeline_name} | run_id={run_id}"
        )

    except Exception as e:
        print(
            f"[PROMETHEUS] Pushgateway FAILED | "
            f"pipeline={pipeline_name} | run_id={run_id} | error={e}"
        )
        raise


# =============================================================
# MAIN PIPELINE ENTRY POINT
# =============================================================
def process_commodities(
    start_date_override: str  = None,
    replay:              bool = False,
    airflow_metadata:    dict = None,
):
    """
    Production-grade commodity processing pipeline.

    Mode detection (derived from parameters ONLY — never from data columns):
    ┌──────────────────────────────────┬──────────────┐
    │ Condition                        │ mode         │
    ├──────────────────────────────────┼──────────────┤
    │ replay = True                    │ "replay"     │
    │ start_date_override provided     │ "backfill"   │
    │ default (neither)                │ "incremental"│
    └──────────────────────────────────┴──────────────┘

    Date filtering:
    ┌──────────────┬────────────────────────────────────────────────────────┐
    │ Mode         │ Filter applied to loaded commodity rolling file        │
    ├──────────────┼────────────────────────────────────────────────────────┤
    │ replay       │ date BETWEEN start_date AND end_date (today)           │
    │ backfill     │ date BETWEEN start_date AND end_date (today)           │
    │ incremental  │ date > last_date_from_snowflake (COMMODITY clean table)│
    └──────────────┴────────────────────────────────────────────────────────┘

    Snowflake writes (TWO TABLES):
    - COMMODITY_HISTORY (append-only):  always INSERT, includes run_mode column
    - COMMODITY (clean):                DELETE window + INSERT (replay/backfill)
                                        incremental -> MERGE latest-state rows
  
    Postgres writes (serving layer):
    - Rules:
        * 1 unique date:  Append + trim to last 2 days
        * 2 unique dates: Replace entire table (no trim needed)
        * >2 unique dates: Trim to latest 2 days in memory, then replace
    - Never writes data just to delete it later

    Failure contract:
    - Pipeline fails if EITHER Snowflake write OR Postgres write fails.
    - Partial success is NOT allowed.
    - Model/feature failures are soft-fail (fallback to historical
      volatility), never hard failures.

    Args:
        start_date_override: "YYYY-MM-DD" string. If provided and
                             replay is False -> backfill mode.
        replay:              True -> replay mode (deterministic re-run
                             from the S3 commodity features partitions).

    Returns:
        str: Status string for Airflow XCom
             ("NO_NEW_MTM_ROWS" | "NO_NEW_COMMOD_ROWS" | "NO_OUTPUT_ROWS" |
              "UPLOAD_SUCCESS_<n>_ROWS")

    Raises:
        Any unhandled exception triggers a CRITICAL alert and re-raises
        for Airflow to mark the task as FAILED.
    """
    pipeline_start = time.time()
    run_ts = datetime.utcnow()
    run_id = run_ts.isoformat()

    # ──────────────────────────────────────────
    # MODE DETECTION — single, explicit decision
    # ──────────────────────────────────────────
    if replay:
        mode = "replay"
    elif start_date_override:
        mode = "backfill"
    else:
        mode = "incremental"

    today    = pd.Timestamp.today().normalize()
    end_date = today   # processing window always ends at today

    print(f"\n{'=' * 66}")
    print(f"  COMMODITY PROCESSING PIPELINE START")
    print(f"  run_id              : {run_id}")
    print(f"  mode                : {mode}")
    print(f"  start_date_override : {start_date_override}")
    print(f"  replay              : {replay}")
    print(f"  end_date            : {end_date.date()}")
    print(f"{'=' * 66}\n")

    try:
        # ══════════════════════════════════════════════════════════════
        # STEP 1 — Load company and macro reference data from S3
        # ══════════════════════════════════════════════════════════════
        print("  [STEP 1] Loading company and macro data from S3...")

        companies = load_csv_from_s3_with_retry(
            S3_BUCKET, SYM, dayfirst=True, retries=3, run_id=run_id
        )
        macro = load_csv_from_s3_with_retry(
            MACRO_BUCKET, MACRO_KEY, parse_dates=["date"], dayfirst=True, retries=3, run_id=run_id
        )

        print(f"  Companies loaded : {len(companies):,}")
        print(f"  Macro rows loaded: {len(macro):,}")

        # ══════════════════════════════════════════════════════════════
        # STEP 2 — Determine processing window
        # ══════════════════════════════════════════════════════════════
        print("\n  [STEP 2] Determining processing window...")

        if mode in ("replay", "backfill"):
            # Date range comes entirely from the parameter — deterministic
            if not start_date_override:
                raise ValueError(
                    f"mode='{mode}' requires start_date_override to be provided."
                )
            start_date = pd.Timestamp(start_date_override).normalize()
            print(
                f"  [{mode.upper()}] Processing window: "
                f"{start_date.date()} -> {end_date.date()}"
            )

        else:  # incremental
            # Watermark: last date successfully written to the COMMODITY clean table
            def get_last_commodity_date():
                with get_snowflake_conn() as ctx:
                    with ctx.cursor() as cs:
                        cs.execute(f'SELECT MAX("date") FROM "{SNOWFLAKE_CLEAN_TABLE}"')
                        return cs.fetchone()[0]

            last_date_sf = retry_with_backoff(
                get_last_commodity_date,
                retries=2,
                critical_name="Snowflake COMMODITY watermark query",
                run_id=run_id,
            )

            if last_date_sf is not None:
                # Snowflake returns VARCHAR, convert to naive datetime
                last_date_sf = pd.to_datetime(last_date_sf).normalize()
                # Ensure no timezone
                if hasattr(last_date_sf, 'tz') and last_date_sf.tz is not None:
                    last_date_sf = last_date_sf.tz_localize(None)
            else:
                last_date_sf = pd.Timestamp("1970-01-01")

            print(f"  [INCREMENTAL] Last processed date: {last_date_sf.date()}")
            print(f"  [INCREMENTAL] Today              : {today.date()}")

            if last_date_sf.date() >= today.date():
                print("  [INCREMENTAL] Watermark is current — no new data to process.")
                return "NO_UPDATE_NEEDED"

            start_date = last_date_sf  # filter uses date > last_date_sf

        # ══════════════════════════════════════════════════════════════
        # STEP 3 — Fetch MTM data from Snowflake EQUITY table
        # ══════════════════════════════════════════════════════════════
        print("\n  [STEP 3] Fetching MTM data from Snowflake EQUITY table...")

        def get_mtm_data():
            if mode in ("replay", "backfill"):
                start_str = start_date.strftime("%Y-%m-%d")
                end_str   = end_date.strftime("%Y-%m-%d")
                where_clause = f'"date" BETWEEN \'{start_str}\'::DATE AND \'{end_str}\'::DATE'
            else:  # incremental
                last_str = last_date_sf.strftime("%Y-%m-%d")
                where_clause = f'"date" > \'{last_str}\'::DATE'

            query = f"""
                SELECT "ticker", "date", "mtm_value", "asset_manager"
                FROM "EQUITY"
                WHERE {where_clause}
            """

            with get_snowflake_conn() as ctx:
                with ctx.cursor() as cs:
                    cs.execute(query)
                    rows = cs.fetchall()
                    cols = ["ticker", "date", "mtm_value", "asset_manager"]
                    df   = pd.DataFrame(rows, columns=cols)
                    df["date"] = pd.to_datetime(df["date"], utc=True)
                    return df

        mtm = retry_with_backoff(
            get_mtm_data,
            retries=2,
            critical_name="Snowflake EQUITY MTM query",
            run_id=run_id,
        )

        if mtm.empty:
            print("  No new MTM rows to process — pipeline complete.")
            return "NO_NEW_MTM_ROWS"

        print(f"  MTM rows fetched: {len(mtm):,}")

        # ══════════════════════════════════════════════════════════════
        # STEP 4 — Load commodity feature data from S3
        # ══════════════════════════════════════════════════════════════
        print("\n  [STEP 4] Loading commodity feature data from S3...")

        if mode in ("replay", "backfill"):

            print(
                f"  [{mode.upper()}] Loading latest Layer 2 feature partitions "
                f"from {start_date.date()} to {end_date.date()}"
            )

            commod_base = load_commodity_features_from_layer2(
                start_date=start_date,
                end_date=end_date,
                run_id=run_id,
            )

            if commod_base.empty:
                raise RuntimeError(
                    f"No feature partitions found for {mode} range "
                    f"[{start_date.date()}, {end_date.date()}]"
                )

        else:

            commod_base = load_parquet_from_s3_with_retry(
                S3_BUCKET,
                INPUT_COMMOD,
                retries=3,
                run_id=run_id,
            )



        print(f"  Commodity raw rows loaded: {len(commod_base):,}")

        # Apply lineage column transformations from first pipeline
        commod_base = rename_source_lineage(commod_base)
        commod_base = drop_old_pipeline_metrics(commod_base)
        commod_base["date"] = pd.to_datetime(commod_base["date"], utc=True)

        # ── Date filtering — driven by mode and window, never by data flags ──
        if mode in ("replay", "backfill"):
            start_d = start_date.date()
            end_d   = end_date.date()
            commod_base = commod_base[
                (commod_base["date"].dt.date >= start_d) &
                (commod_base["date"].dt.date <= end_d)
            ].copy()
            print(
                f"  [{mode.upper()}] Filtered to window "
                f"[{start_d}, {end_d}]: {len(commod_base):,} rows"
            )
        else:  # incremental
            last_d = last_date_sf.date()
            commod_base = commod_base[
                commod_base["date"].dt.date > last_d
            ].copy()
            print(
                f"  [INCREMENTAL] Filtered rows with date > {last_d}: "
                f"{len(commod_base):,} rows"
            )

        if commod_base.empty:
            print("  No new commodity feature rows after date filter — pipeline complete.")
            return "NO_NEW_COMMOD_ROWS"

        # Normalise column name for downstream consistency
        commod_base.rename(columns={"commodity_symbol": "commodity"}, inplace=True)

        # ══════════════════════════════════════════════════════════════
        # STEP 5 — Build sector -> commodity exposure mapping (UNCHANGED)
        # ══════════════════════════════════════════════════════════════
        print("\n  [STEP 5] Building sector-commodity exposure mapping...")

        exposure_rows = []
        for _, r in companies.iterrows():
            mapping = SECTOR_TO_COMMODITIES.get(r["sector"])
            if not mapping:
                continue
            for comm, weight in mapping.items():
                exposure_rows.append({
                    "ticker":    r["ticker"],
                    "sector":    r["sector"],
                    "industry":  r["industry"],
                    "commodity": comm,
                    "sensitivity": weight,
                })

        exp = pd.DataFrame(exposure_rows)
        print(f"  Exposure mapping rows: {len(exp):,}")

        # STEP 6 — Modified: Apply commodity-specific scaling factor
        print("\n  [STEP 6] Merging commodity features and MTM data...")

        seg = exp.merge(commod_base, on="commodity", how="left", validate="m:m")
        seg = seg.merge(mtm, on=["ticker", "date"], how="inner")

        # Convert and clean numeric columns
        seg["mtm_value"]    = pd.to_numeric(seg["mtm_value"], errors="coerce").fillna(0)
        seg["vol_20d"]      = pd.to_numeric(seg["vol_20d"], errors="coerce")
        seg["daily_return"] = pd.to_numeric(seg["daily_return"], errors="coerce").fillna(0.0)

        # Hedge ratio via percentile rank
        seg["hedge_ratio"] = (0.2 + 0.6 * seg["vol_20d"].rank(pct=True)).clip(0, 1)

        # ─────────────────────────────────────────────────────────────
        # FIX: Add commodity-specific scaling factor
        # This ensures commodity exposure ≠ equity exposure
        # ─────────────────────────────────────────────────────────────
        commodity_scale_factors = {
            "GC=F": 0.8,   # Gold - lower than equity
            "CL=F": 1.2,   # Crude Oil - higher than equity
            "SI=F": 0.6,   # Silver - lower
            "NG=F": 0.9,   # Natural Gas - slightly lower
            "ZC=F": 0.5,   # Corn - much lower
        }
        seg["commodity_scale"] = seg["commodity"].map(commodity_scale_factors).fillna(0.7)

        # Exposure calculation with scaling factor
        seg["exposure_amount"] = seg["sensitivity"] * seg["mtm_value"] * seg["commodity_scale"]

        # PnL calculation (unchanged logic)
        seg["commodity_pnl"] = (
            seg["exposure_amount"]
            * seg["daily_return"]
            * (1 - seg["hedge_ratio"])
        )

        print(f"  Rows after merge: {len(seg):,}")
        print(f"  Avg commodity exposure: ${seg['exposure_amount'].mean():,.2f}")
        print(f"  Avg MTM value: ${seg['mtm_value'].mean():,.2f}")

        # ══════════════════════════════════════════════════════════════
        # STEP 7 — Merge macro data (UNCHANGED logic)
        # ══════════════════════════════════════════════════════════════
        print("\n  [STEP 7] Merging macro data...")

        seg["date"]  = pd.to_datetime(seg["date"], utc=True)
        macro["date"] = pd.to_datetime(macro["date"], utc=True)

        seg["mm_yy"]   = seg["date"].dt.strftime("%m-%y")
        macro["mm_yy"] = macro["date"].dt.strftime("%m-%y")

        macro_for_merge = macro.drop(columns=["date"])
        seg = seg.merge(macro_for_merge, on="mm_yy", how="left").drop(columns=["mm_yy"])

        print(f"  Rows after macro merge: {len(seg):,}")

        # ══════════════════════════════════════════════════════════════
        # STEP 8 — Load XGBoost model + features (SOFT FAIL)
        # Failure -> fallback to historical volatility; no pipeline crash
        # ══════════════════════════════════════════════════════════════
        print("\n  [STEP 8] Loading commodity volatility model...")

        try:
            booster      = load_model_from_s3_with_retry(S3_BUCKET, MODEL_FILE,   retries=2)
            feature_cols = load_features_from_s3_with_retry(S3_BUCKET, FEATURE_FILE, retries=2)
            model_loaded = True
        except Exception as e:
            print(
                f"  [WARNING] Could not load commodity model: {e} "
                f"— using historical volatility as fallback"
            )
            model_loaded = False
            booster      = None
            feature_cols = []

        # ══════════════════════════════════════════════════════════════
        # STEP 9 — Commodity volatility prediction (SOFT FAIL)
        # ══════════════════════════════════════════════════════════════
        if model_loaded and feature_cols:
            missing = [f for f in feature_cols if f not in seg.columns]
            if missing:
                print(f"  [WARNING] Missing features for commodity model: {missing[:5]}")
                for col in missing:
                    seg[col] = 0

            null_cols = [
                c for c in feature_cols
                if c in seg.columns and seg[c].isna().any()
            ]
            if null_cols:
                print(
                    f"  [WARNING] Null values in features: {null_cols[:5]} "
                    f"(filling with 0)"
                )
                for col in null_cols:
                    seg[col] = seg[col].fillna(0)

            available_features = [f for f in feature_cols if f in seg.columns]
            X = seg[available_features].copy().fillna(0)

            print("  Running commodity volatility prediction...")
            try:
                seg["pred_vol21"] = booster.predict(xgb.DMatrix(X))
                print("  Commodity model prediction completed")
            except Exception as e:
                print(f"  [WARNING] Commodity model prediction failed: {e} — using fallback")
                seg["pred_vol21"] = seg["vol_20d"].fillna(0.01)
        else:
            print("  [WARNING] Model not available — using historical volatility as fallback")
            seg["pred_vol21"] = seg["vol_20d"].fillna(0.01)

        # ══════════════════════════════════════════════════════════════
        # STEP 10 — Stamp current pipeline metadata
        # (run_mode is NOT in this list — added only for HISTORY table
        #  inside write_to_snowflake_history)
        # ══════════════════════════════════════════════════════════════
        print("\n  [STEP 10] Stamping pipeline metadata...")

        seg = seg.copy()
        seg["pipeline_name"]     = "commodity_processing_pipeline"
        seg["pipeline_run_id"]   = run_id
        seg["data_source"]       = "s3_commodity_features + snowflake_equity"
        seg["input_source"]      = "commodity_features + mtm + macro"
        seg["transformation"]    = "commodity_exposure_v1"
        seg["record_created_at"] = datetime.utcnow()

        # ══════════════════════════════════════════════════════════════
        # STEP 11 — Select and order final columns for Snowflake CLEAN table
        # (run_mode is NOT in this list — it is added only for the HISTORY
        #  table inside write_to_snowflake_history)
        # ══════════════════════════════════════════════════════════════
        print("\n  [STEP 11] Selecting and ordering final columns...")

        final_cols = [
            "ticker", "asset_manager", "sector", "industry", "commodity", "date",
            "open", "high", "low", "close", "volume", "daily_return", "log_return",
            "vol_20d", "sensitivity", "hedge_ratio", "mtm_value", "exposure_amount",
            "commodity_pnl", "VaR_95", "VaR_99", "gdp", "unrate", "cpi", "fedfunds",
            "pred_vol21", "data_quality_flag"
            # Current pipeline lineage
            "pipeline_name", "pipeline_run_id", "data_source", "input_source",
            "transformation", "record_created_at",
            # Source pipeline lineage (from first commodity pipeline)
            "source_pipeline", "source_run_id", "source_data_source",
            "source_input_source", "source_transformation", "source_created_at",
            "source_feature_version", "source_schema_version", "source_schema_hash",
        ]

        available_cols = [c for c in final_cols if c in seg.columns]
        final_new = (
            seg[available_cols]
            .sort_values(["commodity", "date", "ticker"])
            .reset_index(drop=True)
        )

        if final_new.empty:
            print("  No output rows generated — pipeline complete.")
            return "NO_OUTPUT_ROWS"
        
        final_new["date"] = pd.to_datetime(final_new["date"]).dt.strftime("%Y-%m-%d")

        snowflake_rows = len(final_new)
        print(f"  Final rows prepared: {snowflake_rows:,}")

        # ══════════════════════════════════════════════════════════════
        # STEP 12 — Write to Snowflake HISTORY (append-only, always INSERT)
        # Failure here -> pipeline FAILS.
        # ══════════════════════════════════════════════════════════════
        print(
            f"\n  [STEP 12] Writing to Snowflake HISTORY table "
            f"({SNOWFLAKE_HISTORY_TABLE})..."
        )
        write_to_snowflake_history(final_new, run_mode=mode, run_id=run_id)

        # ══════════════════════════════════════════════════════════════
        # STEP 13 — Write to Snowflake CLEAN (deterministic latest state)
        # replay/backfill -> DELETE window + INSERT (transactional)
        # incremental     -> INSERT only (watermark-safe, no duplicates)
        # Failure here -> pipeline FAILS.
        # ══════════════════════════════════════════════════════════════
        print(
            f"\n  [STEP 13] Writing to Snowflake CLEAN table "
            f"({SNOWFLAKE_CLEAN_TABLE}) using mode='{mode}'..."
        )
        write_to_snowflake_clean(
            final_new,
            mode=mode,
            start_date=start_date,
            end_date=end_date,
            run_id=run_id,
        )

        # ══════════════════════════════════════════════════════════════
        # STEP 14 — Write to Postgres serving layer
        # Rules based on unique dates in DataFrame:
        #   - 1 date:  Append + trim to last 2 days
        #   - 2 dates: Replace entire table (no trim needed)
        #   - >2 dates: Trim to latest 2 days in memory, then replace
        # Failure here -> pipeline FAILS (consistency-first).
        # ══════════════════════════════════════════════════════════════
        print(f"\n  [STEP 14] Writing to Postgres serving layer...")
        write_to_postgres(final_new, mode=mode)

        # ══════════════════════════════════════════════════════════════
        # STEP 15 — Pipeline success
        # ══════════════════════════════════════════════════════════════
        processing_time = round(time.time() - pipeline_start, 2)

        print(f"\n{'=' * 66}")
        print(f"  COMMODITY PROCESSING PIPELINE SUCCESS")
        print(f"  run_id           : {run_id}")
        print(f"  mode             : {mode}")
        print(f"  rows processed   : {snowflake_rows:,}")
        print(
            f"  window           : "
            f"{start_date.date() if hasattr(start_date, 'date') else start_date}"
            f" -> {end_date.date()}"
        )
        print(f"  duration         : {processing_time}s")
        print(f"  Snowflake HISTORY: OK")
        print(f"  Snowflake CLEAN  : OK")
        print(f"  Postgres         : OK")
        print(f"{'=' * 66}\n")

        # Write run summary to S3
        run_summary = {
            "pipeline_run_id": run_id,
            "pipeline_name": "commodity_processing_pipeline",
            "run_ts": run_ts.isoformat(),
            "status": "SUCCESS",
            "mode": mode,
            "start_date": str(start_date.date() if hasattr(start_date, 'date') else start_date),
            "end_date": str(end_date.date()),
            "rows_processed": snowflake_rows,
            "processing_time_s": processing_time,
            "source_run_ids": (
                final_new["source_run_id"]
                .dropna()
                .astype(str)
                .unique()
                .tolist()
                if "source_run_id" in final_new.columns
                else []
            ),
            "tables_written": [
                SNOWFLAKE_HISTORY_TABLE,
                SNOWFLAKE_CLEAN_TABLE,
                "commodity_data",
            ],
            "airflow": airflow_metadata,
        }
        write_run_summary(run_summary)

        push_pipeline_metrics(
            pipeline_name="commodity_processing_pipeline",
            run_id=run_id,
            status="SUCCESS",
            metrics={
                "runtime_seconds": processing_time,
                "rows_processed": snowflake_rows,
                "model_loaded": 1 if model_loaded else 0,
                "unique_tickers": final_new["ticker"].nunique(),
                "unique_commodities": final_new["commodity"].nunique(),
                "avg_exposure_amount": float(final_new["exposure_amount"].mean()),
            },
        )

        return f"UPLOAD_SUCCESS_{snowflake_rows}_ROWS"

    except Exception as e:
        processing_time = round(time.time() - pipeline_start, 2)

        send_critical_alert(
            f"Commodity processing pipeline failed with unhandled exception",
            context={
                "run_id":   run_id,
                "mode":     mode,
                "duration": f"{processing_time}s",
                "error":    str(e),
            },
        )

        print(f"\n{'=' * 66}")
        print(f"  COMMODITY PROCESSING PIPELINE FAILED")
        print(f"  run_id   : {run_id}")
        print(f"  mode     : {mode}")
        print(f"  error    : {e}")
        print(f"  duration : {processing_time}s")
        print(f"{'=' * 66}\n")

        # Write failure summary to S3
        run_summary = {
            "pipeline_run_id": run_id,
            "pipeline_name": "commodity_processing_pipeline",
            "run_ts": run_ts.isoformat(),
            "status": "FAILED",
            "mode": mode,
            "processing_time_s": processing_time,
            "error": str(e),
            "airflow": airflow_metadata,
        }
        write_run_summary(run_summary)

        try:
            push_pipeline_metrics(
                pipeline_name="commodity_processing_pipeline",
                run_id=run_id,
                status="FAILED",
                metrics={
                    "runtime_seconds": processing_time,
                },
            )
        except Exception:
            pass

        # Re-raise so Airflow marks the task as FAILED
        raise