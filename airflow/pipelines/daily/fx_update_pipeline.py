"""
fx_update_pipeline.py
======================
Production-grade FX update pipeline — consistency-first architecture.

This is the SECOND FX pipeline. It consumes the rolling FX exposure file
written by fx_exposure_pipeline.py (first pipeline) and enriches it with
financial calculations, volatility modelling, and an XGBoost prediction.

Storage architecture
--------------------
Snowflake FX_HISTORY  (append-only, audit trail)
    - Always INSERT; never DELETE or UPDATE
    - Includes run_mode column for lineage tracing
    - Duplicates across runs are EXPECTED and intentional

Snowflake FX          (clean, deterministic latest state)
    - Replay / Backfill:  DELETE window -> INSERT fresh rows (transactional)
    - Incremental:        MERGE (upsert on ticker + date)
    - Must NEVER contain conflicting rows for same (ticker, date)

Postgres fx_data      (serving layer — NOT source of truth)
    - Rules:
        * 1 unique date:  Append + trim to last 2 days
        * 2 unique dates: Replace entire table (no trim needed)
        * >2 unique dates: Trim to latest 2 days in memory, then replace
    - Never writes data just to delete it later

Failure semantics
-----------------
Pipeline is consistency-first, NOT availability-first.
  - Snowflake FX_HISTORY failure -> FAIL
  - Snowflake FX failure         -> FAIL
  - Postgres failure             -> FAIL
Partial success is NOT allowed under any mode.

Mode detection
--------------
Derived exclusively from function parameters. Never inferred from data columns.
    replay=True                   -> mode = "replay"
    start_date_override provided  -> mode = "backfill"
    default                       -> mode = "incremental"

Enrichment buffer pattern
-------------------------
Rolling window calculations (vol_20d, vol_30d) require BUFFER_DAYS of
lookback history before the first window row. The pipeline always prepends
buffer rows, runs enrichment on the combined frame, then strips them.
fx_enrichment() drops warmup rows via is_warmup automatically.
This mirrors the equity pipeline's tail_df -> feature_input -> new_features
pattern exactly.

DAG integration
---------------
master_dag.py calls update_fx_snowflake(start_date_override, replay).
Signature matches exactly.
"""

import os
import time
import pandas as pd
import numpy as np
import joblib
import xgboost as xgb
import boto3
import requests
import json
from io import BytesIO, StringIO
from datetime import datetime, timedelta
from connections.postgre_conn import get_postgre_conn
from connections.snowflake_conn import get_snowflake_conn
from snowflake.connector.pandas_tools import write_pandas


# =============================================================
# CONSTANTS
# =============================================================
S3_BUCKET = "yeeshu-fx-bucket"
s3 = boto3.client("s3")

TRADING_DAYS       = 252
BASE_HEDGE         = 0.10
W_VOL              = 0.75
W_CARRY            = 0.15
Z_95, Z_99         = 1.65, 2.33
TRADING_DAYS_PER_Q = 63
BUFFER_DAYS        = 40   # lookback rows prepended for rolling-window warmup

# Snowflake table names — single source of truth for table identifiers
SNOWFLAKE_CLEAN_TABLE   = "FX"            # deterministic latest-state table
SNOWFLAKE_HISTORY_TABLE = "FX_HISTORY"    # append-only audit/lineage table


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
        "level":     "CRITICAL",
        "pipeline":  "fx_update_pipeline",
        "message":   message,
        "context":   context or {},
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
                f"*[CRITICAL]* fx_update_pipeline\n"
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
        f"historical-fx/fx-risk/metadata/"
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
            f"historical-fx/features/"
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

        current += pd.Timedelta(days=1)

    return list(latest_per_date.values())


def load_fx_features_from_layer2(start_date, end_date, run_id=None):

    keys = get_latest_feature_keys(start_date, end_date)

    if not keys:
        return pd.DataFrame()

    frames = []

    for key in keys:

        print(f"  [LAYER2] Loading: {key}")

        df = load_parquet_from_s3_with_retry(
            S3_BUCKET,
            key,
            run_id=run_id,
            retries=3,
        )

        frames.append(df)

    fx = pd.concat(frames, ignore_index=True)

    fx["date"] = pd.to_datetime(
        fx["date"],
    ).dt.normalize()

    return fx


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
def load_parquet_from_s3_with_retry(bucket, key, run_id=None, retries=3):
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
# FX ENRICHMENT (UNCHANGED)
# =============================================================
def fx_enrichment(df):
    """
    Core FX enrichment: computes volatility, carry, hedge ratio, VaR, and
    P&L metrics. Drops warmup rows (rows where rolling windows are not yet
    fully populated). UNCHANGED from original implementation.
    """
    df = df.sort_values(["currency_pair", "date"]).reset_index(drop=True)

    df["fx_return"] = df.groupby("currency_pair")["fx_rate"].pct_change()
    df["fx_volatility_20d"] = df.groupby("currency_pair")["fx_return"].transform(
        lambda x: x.rolling(20).std()
    )
    df["fx_volatility_30d"] = df.groupby("currency_pair")["fx_return"].transform(
        lambda x: x.rolling(30).std()
    )

    def downside_risk(x, window=20):
        neg = x.copy()
        neg[neg > 0] = 0
        return neg.rolling(window).std()

    df["downside_risk_20d"] = df.groupby("currency_pair")["fx_return"].transform(
        lambda x: downside_risk(x, 20)
    )
    df["momentum_5d"] = df.groupby("currency_pair")["fx_rate"].transform(
        lambda x: x.pct_change(5)
    )
    df["momentum_20d"] = df.groupby("currency_pair")["fx_rate"].transform(
        lambda x: x.pct_change(20)
    )
    df["sharpe_ratio_20d"] = df["fx_return"] / df["fx_volatility_20d"]

    df.dropna(subset=["fx_return"], inplace=True)

    df["position_size"] = (df["revenue"] / TRADING_DAYS_PER_Q) * df["foreign_revenue_ratio"]
    df.drop(columns=["revenue"], inplace=True)

    vol_20 = df.get("fx_volatility_20d", df.get("fx_volatility", np.nan))
    vol_30 = df.get("fx_volatility_30d", df.get("fx_volatility", np.nan))
    df["fx_volatility_blend"] = 0.7 * vol_20.fillna(0) + 0.3 * vol_30.fillna(0)
    df["fx_volatility"] = df.groupby("currency_pair")["fx_volatility_blend"].transform(
        lambda x: x.ewm(span=10, adjust=False).mean()
    )

    df["carry_daily"]      = (df["interest_diff"] / 100) / TRADING_DAYS
    df["return_carry_adj"] = df["fx_return"] + df["carry_daily"]

    def minmax_grp(s):
        vmin, vmax = s.min(), s.max()
        return (s - vmin) / (vmax - vmin if vmax > vmin else 1.0)

    df["vol_scaled"] = df.groupby("currency_pair")["fx_volatility"].transform(minmax_grp)

    abs_int = abs(df["interest_diff"])
    imin, imax = abs_int.min(), abs_int.max()
    df["int_scaled"] = 1 - (abs_int - imin) / (imax - imin if imax > imin else 1.0)

    df["hedge_ratio_raw"] = BASE_HEDGE + W_VOL * df["vol_scaled"] + W_CARRY * df["int_scaled"]

    med_vol  = df["vol_scaled"].median()
    sigmoid  = 1 / (1 + np.exp(-6 * (df["vol_scaled"] - med_vol)))
    df["hedge_ratio"] = np.clip(0.5 * sigmoid + 0.5 * df["hedge_ratio_raw"], 0, 1)

    df["exposure_amount"] = df["position_size"] * (1 - df["hedge_ratio"])

    df["VaR_95"]        = df["position_size"] * df["fx_volatility"] * Z_95
    df["VaR_99"]        = df["position_size"] * df["fx_volatility"] * Z_99
    df["value_at_risk"] = df["VaR_99"]

    df["volume"]            = 0.25 * df["position_size"]
    df["fx_pnl"]            = df["exposure_amount"] * df["fx_return"]
    df["carry_pnl"]         = df["exposure_amount"] * df["carry_daily"]
    df["total_pnl"]         = df["fx_pnl"] + df["carry_pnl"]
    df["expected_pnl"]      = df["total_pnl"]
    df["sharpe_like_ratio"] = np.where(
        (df["position_size"] * df["fx_volatility"]).abs() < 1e-8,
        np.nan,
        df["total_pnl"] /
        (df["position_size"] * df["fx_volatility"])
    )
    df["is_warmup"] = df[["fx_volatility_20d", "fx_volatility_30d"]].isna().any(axis=1)

    # Drop warmup rows — buffer rows always fall here since they lack enough
    # rolling history. This naturally strips the buffer after enrichment.
    return df[~df["is_warmup"]].copy()


# =============================================================
# METADATA HELPERS (UNCHANGED)
# =============================================================
def rename_source_lineage(df):
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
    drop_cols = [
        "data_date",
        "ingestion_start_date",
        "input_rows",
        "output_rows",
        "processing_time_s",
        "replay_mode",
        "run_mode",
        "record_id",
        "outlier_flag"
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
        "transformation", "record_created_at",
    ]
    return df.drop(columns=[c for c in drop_cols if c in df.columns])


# =============================================================
# SNOWFLAKE HISTORY TABLE (append-only, always INSERT)
# =============================================================
def write_to_snowflake_history(df, run_mode, run_id=None, chunk_size=20_000):
    """
    Append rows to FX_HISTORY. Never updates or deletes existing rows.

    Adds a run_mode column so every row is traceable to the exact pipeline
    run that produced it. Duplicates across runs are expected by design —
    this table is an immutable audit trail.

    Raises on failure (after retries). Pipeline does NOT continue if this
    write fails.

    Args:
        df:        Fully enriched and transformed DataFrame.
        run_mode:  One of "incremental", "backfill", "replay".
        run_id:    Pipeline run identifier for alert context.
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
    Atomically replace a replay/backfill window in the FX clean table.

    Architecture
    ------------
    FX_HISTORY:
        append-only audit table
        duplicates across runs are allowed

    FX:
        deterministic latest-state table
        must NEVER contain conflicting rows for the same business key

    Replay/backfill semantics
    -------------------------
    Recompute the requested window completely and replace it atomically.

    Protocol
    --------
    1. Create a TEMP staging table.
    2. Bulk load dataframe into staging via write_pandas().
    3. BEGIN explicit transaction.
    4. DELETE existing rows for target date window.
    5. INSERT fresh recomputed rows from staging.
    6. COMMIT transaction.
    7. ROLLBACK automatically on any failure.

    Guarantees
    ----------
    - No partial replay/backfill state.
    - No duplicate clean-table rows across reruns.
    - Full deterministic replacement of target window.
    - Temp table automatically disappears after session close.

    Notes
    -----
    write_pandas() itself is not transactional in Snowflake because it uses
    staged COPY operations internally. Therefore staging is done BEFORE
    opening the transaction.

    Args
    ----
    df:
        Fully processed dataframe ready for clean-table write.

    start_date:
        Inclusive replay/backfill window start.

    end_date:
        Inclusive replay/backfill window end.

    run_id:
        Pipeline run identifier for observability / alerting.

    chunk_size:
        write_pandas batch size.

    Raises
    ------
    Exception:
        Re-raised after rollback + critical alert.
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
                # CREATE TEMP STAGING TABLE
                # =====================================================

                cs.execute(f"""
                    CREATE TEMPORARY TABLE {temp_table}
                    LIKE "{SNOWFLAKE_CLEAN_TABLE}"
                """)

                # =====================================================
                # LOAD DATAFRAME INTO STAGING TABLE
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
                # BEGIN EXPLICIT TRANSACTION
                # =====================================================

                cs.execute("BEGIN")

                try:

                    # =================================================
                    # DELETE TARGET WINDOW
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
                    # INSERT RECOMPUTED WINDOW
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
                    # COMMIT ATOMICALLY
                    # =================================================

                    cs.execute("COMMIT")

                except Exception:

                    # =============================================
                    # ROLLBACK ON ANY FAILURE
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
def _snowflake_clean_merge(df, run_id=None, chunk_size=20_000):
    """
    Upsert rows into the FX clean table using a Snowflake MERGE.

    Used for incremental runs only.

    Business uniqueness:
        ticker + currency_pair + date

    For each unique business key:
        - If the row already exists: UPDATE all non-key columns.
        - If the row is new:         INSERT.

    Process overview:
        1. Create a temporary staging table matching the target schema.
        2. Load the incremental DataFrame into the staging table.
        3. Fetch ordered target columns excluding the auto-generated ID.
        4. Build and execute a Snowflake MERGE statement.
        5. Report affected row count after merge completion.

    The MERGE operation is atomic in Snowflake, ensuring no partial state
    is possible.

    Args:
        df:         Fully processed DataFrame for the incremental window.
        run_id:     Pipeline run identifier for alert context.
        chunk_size: Rows per write_pandas chunk to the staging temp table.

    Raises:
        Exception propagated after CRITICAL alert.
    """

    key_columns = [
        "ticker",
        "currency_pair",
        "date",
    ]

    temp_table = (
        f"{SNOWFLAKE_CLEAN_TABLE}_STAGE_"
        f"{int(time.time())}"
    )

    dupes = (
        df.groupby([
            "ticker",
            "currency_pair",
            "date",
        ])
        .size()
        .reset_index(name="cnt")
    )

    dup_rows = dupes[dupes["cnt"] > 1]

    if not dup_rows.empty:

        raise ValueError(
            "Duplicate business keys detected before MERGE: "
            f"{len(dup_rows)} duplicate combinations"
        )


    try:

        with get_snowflake_conn() as ctx:

            with ctx.cursor() as cs:

                # =====================================================
                # CREATE TEMP STAGING TABLE
                # =====================================================

                cs.execute(f"""
                    CREATE TEMPORARY TABLE {temp_table}
                    LIKE "{SNOWFLAKE_CLEAN_TABLE}"
                """)

                # =====================================================
                # WRITE DATAFRAME TO STAGING
                # =====================================================

                write_pandas(
                    ctx,
                    df,
                    temp_table,
                    chunk_size=chunk_size,
                    quote_identifiers=True,
                    auto_create_table=False,
                    use_logical_type=True,
                )

                # =====================================================
                # FETCH TARGET COLUMN ORDER
                # =====================================================

                cs.execute(f"""
                    SELECT COLUMN_NAME
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_NAME = '{SNOWFLAKE_CLEAN_TABLE}'
                      AND COLUMN_NAME != 'ID'
                    ORDER BY ORDINAL_POSITION
                """)

                columns = [
                    row[0]
                    for row in cs.fetchall()
                ]

                # =====================================================
                # BUILD MERGE SQL
                # =====================================================

                merge_condition = " AND ".join([
                    f'target."{col}" = source."{col}"'
                    for col in key_columns
                ])

                update_set = ", ".join([
                    f'target."{col}" = source."{col}"'
                    for col in columns
                ])

                insert_columns = ", ".join([
                    f'"{col}"'
                    for col in columns
                ])

                insert_values = ", ".join([
                    f'source."{col}"'
                    for col in columns
                ])

                merge_sql = f"""
                    MERGE INTO "{SNOWFLAKE_CLEAN_TABLE}" AS target
                    USING {temp_table} AS source

                    ON {merge_condition}

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

                affected_rows = (
                    cs.rowcount
                    if cs.rowcount is not None
                    else len(df)
                )

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
    - "incremental":         MERGE (upsert on ticker + date).

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
_PG_INTEGER_COLS = ["volume", "position_size", "exposure_amount"]


def write_to_postgres(df, mode, retries=3):
    """
    Write enriched FX rows to the Postgres serving layer.

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
                          AND table_name = 'fx_data'
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
                            "for public.fx_data"
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
                            "DELETE FROM public.fx_data"
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
                                DELETE FROM public.fx_data
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
                        f"COPY public.fx_data "
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
                            DELETE FROM public.fx_data
                            WHERE date < (
                                SELECT MAX(date)
                                FROM public.fx_data
                            ) - INTERVAL '1 day'
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
                            "  [POSTGRES] No trim needed "
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
        f"Postgres FX write failed after {retries} retries",
        context={
            "error": str(last_error),
            "rows": len(df_pg),
        },
    )

    raise RuntimeError(
        f"Postgres write to public.fx_data "
        f"failed after {retries} retries: "
        f"{last_error}"
    )

# =============================================================
# MAIN PIPELINE ENTRY POINT
# =============================================================
def update_fx_snowflake(
    start_date_override: str  = None,
    replay:              bool = False,
    airflow_metadata:    dict = None,
):
    """
    Production-grade FX update pipeline.

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
    │ Mode         │ Filter applied to the loaded FX rolling file           │
    ├──────────────┼────────────────────────────────────────────────────────┤
    │ replay       │ date BETWEEN start_date AND end_date (today)           │
    │ backfill     │ date BETWEEN start_date AND end_date (today)           │
    │ incremental  │ date > last_date_from_snowflake (FX clean table)       │
    └──────────────┴────────────────────────────────────────────────────────┘

    Enrichment buffer (ALL modes):
    BUFFER_DAYS rows before the window are prepended so that vol_20d / vol_30d
    are fully populated for the first window rows. fx_enrichment() drops these
    warmup rows automatically via the is_warmup flag.

    Snowflake writes (TWO TABLES):
    - FX_HISTORY (append-only):  always INSERT, includes run_mode column
    - FX (clean):                DELETE window + INSERT (replay/backfill)
                                  MERGE (incremental)

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
                             from the S3 FX rolling file).

    Returns:
        str: Status string for Airflow XCom
             ("NO_UPDATE_NEEDED" | "NO_DATA_FOR_ENRICHMENT" |
              "NO_NEW_ROWS" | "UPLOAD_SUCCESS_<n>_ROWS")

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
    print(f"  FX UPDATE PIPELINE START")
    print(f"  run_id             : {run_id}")
    print(f"  mode               : {mode}")
    print(f"  start_date_override: {start_date_override}")
    print(f"  replay             : {replay}")
    print(f"  end_date           : {end_date.date()}")
    print(f"{'=' * 66}\n")

    try:
        # ══════════════════════════════════════════════════════════════
        # STEP 1 — Determine processing window
        # ══════════════════════════════════════════════════════════════
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
            # Watermark: last date successfully written to the FX clean table
            def get_last_fx_date():
                with get_snowflake_conn() as ctx:
                    with ctx.cursor() as cs:
                        cs.execute(f'SELECT MAX("date") FROM "{SNOWFLAKE_CLEAN_TABLE}"')
                        return cs.fetchone()[0]

            last_date_sf = retry_with_backoff(
                get_last_fx_date,
                retries=2,
                critical_name="Snowflake FX watermark query",
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
        # STEP 2 — Load FX features
        # replay/backfill -> Layer 2 versioned features
        # incremental     -> rolling serving layer
        # ══════════════════════════════════════════════════════════════

        print("  Loading FX feature data from S3...")

        if mode in ("replay", "backfill"):
            # Load extra buffer days before start_date
            buffer_start = start_date - timedelta(days=BUFFER_DAYS)
            
            fx = load_fx_features_from_layer2(
                start_date=buffer_start,
                end_date=end_date,
                run_id=run_id,
            )

        else:

            fx = load_parquet_from_s3_with_retry(
                S3_BUCKET,
                "historical-fx/rolling/fx_exposure_40d.parquet",
                run_id=run_id,
                retries=3,
            )

        # Keep as naive datetime to match Snowflake VARCHAR comparisons
        fx["date"] = pd.to_datetime(fx["date"]).dt.normalize()
        # Remove timezone if present
        if fx["date"].dt.tz is not None:
            fx["date"] = fx["date"].dt.tz_localize(None)

        print(f"  Loaded FX rows: {len(fx):,}")

        # ══════════════════════════════════════════════════════════════
        # STEP 3 — Build enrichment input: buffer rows + window rows
        #
        # Rolling window functions (vol_20d, vol_30d) need BUFFER_DAYS of
        # history before the first window row to output non-NaN values.
        # We prepend up to BUFFER_DAYS buffer rows per ticker, run enrichment
        # on the combined frame, then strip buffer rows after enrichment via
        # the is_warmup flag inside fx_enrichment().
        #
        # This is the same pattern as the equity pipeline's tail_df logic.
        # ══════════════════════════════════════════════════════════════
        if mode in ("replay", "backfill"):
            # Convert to pandas Timestamp for comparison
            start_ts = pd.Timestamp(start_date)
            end_ts = pd.Timestamp(end_date)
            
            # Buffer: up to BUFFER_DAYS rows per ticker strictly before window
            buffer_rows = fx[fx["date"] < start_ts]
            
            # Window rows: target dates to process and write
            window_rows = fx[
                (fx["date"] >= start_ts) &
                (fx["date"] <= end_ts)
            ].copy()

            print(
                f"  [{mode.upper()}] Window rows: {len(window_rows):,} "
                f"| Buffer rows: {len(buffer_rows):,}"
            )

            if window_rows.empty:
                print(
                    "  No FX rows in the processing window "
                    f"[{start_d}, {end_d}] — pipeline complete."
                )
                return "NO_DATA_FOR_ENRICHMENT"

            fx_for_enrichment = (
                pd.concat([buffer_rows, window_rows], ignore_index=True)
                .sort_values(["ticker", "date"])
                .reset_index(drop=True)
            )

        else:  # incremental     
            # Buffer: up to BUFFER_DAYS rows per ticker up to (including) watermark
            buffer_rows = fx[fx["date"] <= last_date_sf]
            buffer_rows = (
                buffer_rows
                .groupby("ticker", group_keys=False)
                .apply(lambda x: x.sort_values("date").tail(BUFFER_DAYS))
                .reset_index(drop=True)
            )

            # New rows: strictly after the watermark
            new_rows = fx[fx["date"] > last_date_sf].copy()

            print(
                f"  [INCREMENTAL] New rows: {len(new_rows):,} "
                f"| Buffer rows: {len(buffer_rows):,}"
            )

            if new_rows.empty:
                print(
                    f"  No new FX rows after watermark ({last_date_sf}) — "
                    "pipeline complete."
                )
                return "NO_NEW_ROWS"

            fx_for_enrichment = (
                pd.concat([buffer_rows, new_rows], ignore_index=True)
                .sort_values(["ticker", "date"])
                .reset_index(drop=True)
            )

        # ══════════════════════════════════════════════════════════════
        # STEP 4 — Rename lineage columns and drop first-pipeline metrics
        # ══════════════════════════════════════════════════════════════
        fx_for_enrichment = rename_source_lineage(fx_for_enrichment)
        fx_for_enrichment = drop_old_pipeline_metrics(fx_for_enrichment)

        # ══════════════════════════════════════════════════════════════
        # STEP 5 — FX enrichment (UNCHANGED logic)
        #
        # fx_enrichment() sorts by (ticker, date), computes rolling windows,
        # and drops warmup rows via is_warmup. Buffer rows are stripped here
        # automatically — no explicit post-enrichment buffer removal needed.
        # ══════════════════════════════════════════════════════════════
        print("  Running FX enrichment...")
        df_enriched = fx_enrichment(fx_for_enrichment)

        # ══════════════════════════════════════════════════════════════
        # STEP 6 — Filter to window-only rows (safety net after enrichment)
        #
        # Buffer rows should already be gone via is_warmup. This explicit
        # filter is a defensive guarantee that no pre-window rows are written.
        # ══════════════════════════════════════════════════════════════
        if mode in ("replay", "backfill"):
            final_rows = df_enriched[
                (df_enriched["date"] >= start_ts) &
                (df_enriched["date"] <= end_ts)
            ].copy()
        else:  # incremental
            final_rows = df_enriched[df_enriched["date"] > last_date_sf].copy()

        if final_rows.empty:
            print("  No new FX rows after enrichment + filter — pipeline complete.")
            return "NO_NEW_ROWS"

        print(f"  Final rows after enrichment + filter: {len(final_rows):,}")

        # ══════════════════════════════════════════════════════════════
        # STEP 7 — Stamp current pipeline metadata
        # ══════════════════════════════════════════════════════════════
        final_rows = final_rows.copy()
        final_rows["pipeline_name"]     = "fx_update_pipeline"
        final_rows["pipeline_run_id"]   = run_id
        final_rows["data_source"]       = "s3_fx_features"
        final_rows["input_source"]      = "fx_features + mtm + macro"
        final_rows["transformation"]    = "fx_enrichment_v1"
        final_rows["record_created_at"] = datetime.utcnow()

        # ══════════════════════════════════════════════════════════════
        # STEP 8 — Load XGBoost model + features (SOFT FAIL)
        # Failure -> fallback to historical volatility; no pipeline crash
        # ══════════════════════════════════════════════════════════════
        print("  Loading FX volatility model...")
        try:
            booster      = load_model_from_s3_with_retry(
                S3_BUCKET, "models/fx_model_vol21_xgb.json", retries=2
            )
            feature_cols = load_features_from_s3_with_retry(
                S3_BUCKET, "models/fx_features_vol21.pkl", retries=2
            )
            model_loaded = True
        except Exception as e:
            print(
                f"  [WARNING] Could not load FX model: {e} "
                f"— using historical volatility as fallback"
            )
            model_loaded = False
            booster      = None
            feature_cols = []

        # ══════════════════════════════════════════════════════════════
        # STEP 9 — FX volatility prediction (SOFT FAIL)
        # ══════════════════════════════════════════════════════════════
        if model_loaded and feature_cols:
            missing = [f for f in feature_cols if f not in final_rows.columns]
            if missing:
                print(f"  [WARNING] Missing features for FX model: {missing[:5]}")
                for col in missing:
                    final_rows[col] = 0

            null_cols = [
                c for c in feature_cols
                if c in final_rows.columns and final_rows[c].isna().any()
            ]
            if null_cols:
                print(
                    f"  [WARNING] Null values in features: {null_cols[:5]} "
                    f"(filling with 0)"
                )
                for col in null_cols:
                    final_rows[col] = final_rows[col].fillna(0)

            available_features = [f for f in feature_cols if f in final_rows.columns]
            X = final_rows[available_features].copy().fillna(0)

            print("\n===== INF CHECK =====")

            numeric_cols = final_rows.select_dtypes(include=[np.number]).columns

            for col in numeric_cols:
                inf_count = np.isinf(final_rows[col]).sum()

                if inf_count > 0:
                    print(col, "INF COUNT =", inf_count)

            print("\n===== MODEL INPUT CHECK =====")

            X = final_rows[feature_cols]

            print("shape:", X.shape)

            null_counts = X.isna().sum()
            null_counts = null_counts[null_counts > 0]

            print("\nNULL FEATURES:")
            print(null_counts)

            for col in X.columns:
                try:
                    max_val = X[col].max()
                    min_val = X[col].min()

                    if pd.notna(max_val) and (
                        abs(max_val) > 1e12 or
                        abs(min_val) > 1e12
                    ):
                        print(
                            f"{col}: min={min_val}, max={max_val}"
                        )
                except Exception:
                    pass

            print("  Running FX volatility prediction...")
            try:
                final_rows["predicted_volatility_21d"] = booster.predict(xgb.DMatrix(X))
                print("  FX model prediction completed")
            except Exception as e:
                print(f"  [WARNING] FX model prediction failed: {e} — using fallback")
                final_rows["predicted_volatility_21d"] = (
                    final_rows["fx_volatility_20d"].fillna(0.01)
                )
        else:
            print("  [WARNING] Model not available — using historical volatility as fallback")
            final_rows["predicted_volatility_21d"] = (
                final_rows["fx_volatility_20d"].fillna(0.01)
            )

        # ══════════════════════════════════════════════════════════════
        # STEP 10 — Select and order final columns for Snowflake CLEAN table
        # (run_mode is NOT in this list — it is added only for the HISTORY
        #  table inside write_to_snowflake_history)
        # ══════════════════════════════════════════════════════════════
        final_cols = [
            "ticker", "sector", "industry", "currency_pair", "foreign_revenue_ratio", "date",
            "fx_rate", "fx_return", "fx_volatility_20d", "fx_volatility_30d", "fx_volatility",
            "interest_diff", "carry_daily", "return_carry_adj",
            "position_size", "hedge_ratio", "exposure_amount",
            "fx_pnl", "carry_pnl", "total_pnl", "expected_pnl",
            "VaR_95", "VaR_99", "value_at_risk",
            "volume", "sharpe_like_ratio", "is_warmup",
            "gdp", "unrate", "fedfunds", "cpi",
            "predicted_volatility_21d",
            "source_pipeline",
            "source_run_id",
            "source_data_source",
            "source_input_source",
            "source_transformation",
            "source_created_at",
            "source_feature_version",
            "source_schema_version",
            "source_schema_hash",
            "pipeline_name",
            "pipeline_run_id",
            "data_source",
            "input_source",
            "transformation",
            "record_created_at",
        ]

        # Keep only columns present in both final_cols and the DataFrame
        df_to_upload = final_rows[
            [c for c in final_cols if c in final_rows.columns]
        ].copy()
        # Force to YYYY-MM-DD string
        df_to_upload["date"] = pd.to_datetime(df_to_upload["date"]).dt.strftime("%Y-%m-%d")
        df_to_upload  = df_to_upload.sort_values(["ticker", "date"])
        snowflake_rows = len(df_to_upload)
        print(f"  Preparing {snowflake_rows:,} rows for Snowflake...")

        # ══════════════════════════════════════════════════════════════
        # STEP 11 — Write to Snowflake HISTORY (append-only, always INSERT)
        # Failure here -> pipeline FAILS.
        # ══════════════════════════════════════════════════════════════
        print(
            f"\n  Writing to Snowflake HISTORY table ({SNOWFLAKE_HISTORY_TABLE})..."
        )
        write_to_snowflake_history(df_to_upload, run_mode=mode, run_id=run_id)

        # ══════════════════════════════════════════════════════════════
        # STEP 12 — Write to Snowflake CLEAN (deterministic latest state)
        # replay/backfill -> DELETE window + INSERT (transactional)
        # incremental     -> MERGE
        # Failure here -> pipeline FAILS.
        # ══════════════════════════════════════════════════════════════
        print(
            f"\n  Writing to Snowflake CLEAN table ({SNOWFLAKE_CLEAN_TABLE}) "
            f"using mode='{mode}'..."
        )
        write_to_snowflake_clean(
            df_to_upload,
            mode=mode,
            start_date=start_date,
            end_date=end_date,
            run_id=run_id,
        )
 
        # ══════════════════════════════════════════════════════════════
        # STEP 13 — Write to Postgres serving layer
        # Rules based on unique dates in DataFrame:
        #   - 1 date:  Append + trim to last 2 days
        #   - 2 dates: Replace entire table (no trim needed)
        #   - >2 dates: Trim to latest 2 days in memory, then replace
        # Failure here -> pipeline FAILS (consistency-first).
        # ══════════════════════════════════════════════════════════════
        print(f"\n  Writing to Postgres serving layer...")
        write_to_postgres(df_to_upload, mode=mode)

        # ══════════════════════════════════════════════════════════════
        # STEP 14 — Pipeline success
        # ══════════════════════════════════════════════════════════════
        processing_time = round(time.time() - pipeline_start, 2)

        print(f"\n{'=' * 66}")
        print(f"  FX PIPELINE SUCCESS")
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
            "pipeline_name": "fx_update_pipeline",
            "run_ts": run_ts.isoformat(),
            "status": "SUCCESS",
            "mode": mode,
            "start_date": str(start_date.date() if hasattr(start_date, 'date') else start_date),
            "end_date": str(end_date.date()),
            "rows_processed": snowflake_rows,
            "processing_time_s": processing_time,
            "market_feature_source_run_ids": (
                df_to_upload["source_run_id"]
                .dropna()
                .astype(str)
                .unique()
                .tolist()
            ),
            "tables_written": [
                SNOWFLAKE_HISTORY_TABLE,
                SNOWFLAKE_CLEAN_TABLE,
                "fx_data",
            ],
            "airflow": airflow_metadata,
        }
        write_run_summary(run_summary)

        return f"UPLOAD_SUCCESS_{snowflake_rows}_ROWS"

    except Exception as e:
        processing_time = round(time.time() - pipeline_start, 2)

        send_critical_alert(
            f"FX pipeline failed with unhandled exception",
            context={
                "run_id":   run_id,
                "mode":     mode,
                "duration": f"{processing_time}s",
                "error":    str(e),
            },
        )

        print(f"\n{'=' * 66}")
        print(f"  FX PIPELINE FAILED")
        print(f"  run_id   : {run_id}")
        print(f"  mode     : {mode}")
        print(f"  error    : {e}")
        print(f"  duration : {processing_time}s")
        print(f"{'=' * 66}\n")

        # Write failure summary to S3
        run_summary = {
            "pipeline_run_id": run_id,
            "pipeline_name": "fx_update_pipeline",
            "run_ts": run_ts.isoformat(),
            "status": "FAILED",
            "mode": mode,
            "processing_time_s": processing_time,
            "error": str(e),
            "airflow": airflow_metadata,
        }
        write_run_summary(run_summary)

        # Re-raise so Airflow marks the task as FAILED
        raise