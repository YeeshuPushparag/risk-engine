"""
collateral_pipeline.py
=======================
Production-grade collateral pipeline — consistency-first architecture.

This pipeline consumes the EQUITY, FX, COMMODITY, and BONDS Snowflake clean
tables, computes collateral exposures, margin metrics, and reuse logic, and
writes results to two Snowflake tables (detail + model) and a Postgres
serving layer.

Storage architecture
--------------------
Snowflake COLLATERAL_HISTORY  (append-only, audit trail)
    - Always INSERT; never DELETE or UPDATE
    - Includes run_mode column for lineage tracing
    - Duplicates across runs are EXPECTED and intentional

Snowflake COLLATERAL          (clean, deterministic latest state)
    - Replay / Backfill:  DELETE window -> INSERT fresh rows (transactional)
    - Incremental:        MERGE latest-state rows using
                          (trade_id, date)
    - Must NEVER contain conflicting rows for same (trade_id, date)

Postgres collateral_data      (serving layer — NOT source of truth)
    - Replay / Backfill:  DELETE window -> INSERT
    - Incremental:        INSERT
    - Always trim to last 2 calendar days after write

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

Determinism
-----------
All previously stochastic assignments (counterparty, collateral type, agreement
type, jurisdiction, reuse_flag) are replaced with deterministic SHA-256
hash-based functions keyed on stable business identifiers (trade_id, ticker,
asset_class, date). Replay runs are guaranteed to produce identical outputs
for identical inputs.

Source table load strategy
--------------------------
    Incremental   : WHERE "date" > <watermark from COLLATERAL>
    Replay/Backfill: WHERE "date" BETWEEN start_date AND end_date

All four upstream source tables (FX, EQUITY, COMMODITY, BONDS) share the
identical date window so cross-asset consistency is guaranteed.

DAG integration
---------------
master_dag.py calls run_collateral_pipeline(start_date_override, replay).
Signature matches derivatives_pipeline.py exactly.
"""

import os
import time
import hashlib
from io import BytesIO, StringIO
import numpy as np
import pandas as pd
from datetime import datetime
import boto3
import requests
from connections.snowflake_conn import get_snowflake_conn
from connections.postgre_conn import get_postgre_conn
from snowflake.connector.pandas_tools import write_pandas


# ============================================================
# CONSTANTS
# ============================================================
MACRO_BUCKET = "yeeshu-equity-bucket"
MACRO_KEY    = "historical-equity/macro_data.csv"

s3 = boto3.client("s3")

# Snowflake table names — single source of truth for table identifiers
SNOWFLAKE_CLEAN_TABLE   = "COLLATERAL"          # deterministic latest-state table
SNOWFLAKE_HISTORY_TABLE = "COLLATERAL_HISTORY"  # append-only audit/lineage table
SNOWFLAKE_MODEL_CLEAN_TABLE   = "COLLATERAL_MODEL"
SNOWFLAKE_MODEL_HISTORY_TABLE = "COLLATERAL_MODEL_HISTORY"

# Postgres table names
POSTGRES_DETAIL_TABLE = "collateral_data"
POSTGRES_MODEL_TABLE  = "collateral_model_data"

HAIRCUT = {
    "Cash":          0.00,
    "Treasury":      0.02,
    "CorporateBond": 0.10,
    "Equity":        0.15,
    "CommodityETF":  0.12,
}
INITIAL_MARGIN = {
    "FX":        0.05,
    "Commodity": 0.08,
    "Bond":      0.04,
    "Equity":    0.12,
}

# Collateral menu: (type, cumulative_probability) per asset class
# Using cumulative probabilities so deterministic hash-based selection
# preserves the same frequency distribution as the original np.random.choice.
COLLATERAL_MENU = {
    "FX":        [("Cash", 0.70), ("Treasury", 0.95), ("CorporateBond", 1.00)],
    "Commodity": [("Treasury", 0.60), ("Cash", 0.85), ("CommodityETF", 1.00)],
    "Bond":      [("Treasury", 0.80), ("Cash", 0.95), ("CorporateBond", 1.00)],
    "Equity":    [("Cash", 0.50), ("Treasury", 0.80), ("Equity", 1.00)],
}

# Agreement pool: (agreement_type, jurisdiction, cumulative_probability)
AGREEMENT_POOL = [
    ("CSA",   "US", 0.45),
    ("CSA",   "UK", 0.70),
    ("CSA",   "EU", 0.90),
    ("GMRA",  "UK", 0.95),
    ("GMSLA", "US", 1.00),
]

COUNTERPARTY_POOL = ["CP_A", "CP_B", "CP_C", "CP_D", "CP_E"]


# ============================================================
# PRODUCTION-GRADE ALERTING (CRITICAL ONLY)
# ============================================================
def send_critical_alert(message: str, context: dict = None):
    """
    Send CRITICAL alert to Slack for unrecoverable pipeline failures.

    Triggers for:
    - Snowflake write failure (HISTORY or CLEAN)
    - Postgres write failure (after all retries)
    - Source table load failure (after all retries)
    - Complete pipeline failures

    Does NOT trigger for:
    - Individual retry attempts (only on final exhaustion)
    - Macro data load failure (soft-fail, pipeline continues)
    """
    payload = {
        "level":    "CRITICAL",
        "pipeline": "collateral_pipeline",
        "message":  message,
        "context":  context or {},
        "timestamp": datetime.utcnow().isoformat(),
    }
    print(f"[CRITICAL] {message} | context={payload['context']}")
    webhook = os.getenv("SLACK_WEBHOOK_URL")
    if webhook:
        try:
            truncated = message[:500] + "..." if len(message) > 500 else message
            run_id    = context.get("run_id", "unknown") if context else "unknown"
            text = (
                f"*[CRITICAL]* collateral_pipeline\n"
                f"{truncated}\n"
                f"run_id: {run_id}"
            )
            requests.post(webhook, json={"text": text}, timeout=3)
        except Exception as e:
            print(f"[ALERT ERROR] Slack notification failed: {e}")


def send_success_alert(context: dict = None):
    """
    Send SUCCESS alert to Slack when pipeline completes successfully.
    Only fires when BOTH Snowflake AND Postgres succeed.
    """
    payload = {
        "level":    "SUCCESS",
        "pipeline": "collateral_pipeline",
        "context":  context or {},
        "timestamp": datetime.utcnow().isoformat(),
    }
    print(f"[SUCCESS] Pipeline completed | context={payload['context']}")
    webhook = os.getenv("SLACK_WEBHOOK_URL")
    if webhook:
        try:
            run_id       = context.get("run_id", "unknown") if context else "unknown"
            detail_rows  = context.get("snowflake_detail_rows", 0)
            duration     = context.get("duration_seconds", 0)
            text = (
                f"*[SUCCESS]* collateral_pipeline\n"
                f"✅ Pipeline ran successfully\n"
                f"run_id: {run_id}\n"
                f"detail rows: {detail_rows:,}\n"
                f"duration: {duration}s"
            )
            requests.post(webhook, json={"text": text}, timeout=3)
        except Exception as e:
            print(f"[ALERT ERROR] Slack notification failed: {e}")


# ============================================================
# RETRY WITH EXPONENTIAL BACKOFF
# ============================================================
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


# ============================================================
# DETERMINISTIC HELPERS — replaces all np.random calls
#
# All assignments previously made with np.random.choice / np.random.binomial
# are now derived from a stable SHA-256 hash of a business key.
# Same key -> same output across every run, making replay fully
# deterministic and reproducible.
# ============================================================
def _stable_hash(key: str) -> int:
    """Return a stable non-negative integer from any string key."""
    return int(hashlib.sha256(str(key).encode()).hexdigest(), 16)


def det_counterparty(key: str) -> str:
    """Assign counterparty deterministically from trade key."""
    return COUNTERPARTY_POOL[_stable_hash(key) % len(COUNTERPARTY_POOL)]


def det_collateral_type(key: str, asset_class: str) -> str:
    """
    Select collateral type deterministically using cumulative-probability
    lookup on COLLATERAL_MENU. Frequency distribution is preserved.
    """
    menu = COLLATERAL_MENU.get(asset_class, COLLATERAL_MENU["Equity"])
    r    = (_stable_hash(key + asset_class) % 10_000) / 10_000.0
    for col_type, cumulative in menu:
        if r < cumulative:
            return col_type
    return menu[-1][0]


def det_agreement(key: str) -> tuple:
    """
    Return (agreement_type, jurisdiction) deterministically.
    Uses cumulative-probability lookup on AGREEMENT_POOL.
    """
    r          = (_stable_hash(key + "agreement") % 10_000) / 10_000.0
    cumulative = 0.0
    for ag_type, jurisdiction, weight in AGREEMENT_POOL:
        cumulative += weight
        if r < cumulative:
            return ag_type, jurisdiction
    return AGREEMENT_POOL[-1][0], AGREEMENT_POOL[-1][1]


def det_reuse_flag(key: str) -> int:
    """
    Assign reuse_flag (0 or 1) deterministically.
    Preserves the original ~35% reuse probability.
    """
    return int((_stable_hash(key + "reuse") % 10_000) < 3_500)


# ============================================================
# BUSINESS LOGIC HELPERS
# ============================================================
def ensure_cols(df: pd.DataFrame, cols: list) -> pd.DataFrame:
    """Add missing columns as NaN so downstream logic never KeyErrors."""
    for c in cols:
        if c not in df.columns:
            df[c] = np.nan
    return df


def generate_trade_id(row) -> str:
    """
    Generate a deterministic trade identifier from stable business keys.
    Matches the same pattern used in derivatives_pipeline.py.
    """
    parts = [
        str(row.get("collateral_type_label", "")),
        str(row.get("ticker", "")),
        str(row.get("date", "")),
    ]
    if pd.notna(row.get("commodity_sym")):
        parts.append(str(row["commodity_sym"]).replace("=", "_"))
    if pd.notna(row.get("asset_manager")):
        parts.append(str(row["asset_manager"]).split()[0])
    return "_".join(parts)


# ============================================================
# METADATA HELPERS
# ============================================================
def drop_metadata_for_serving(df: pd.DataFrame) -> pd.DataFrame:
    """Strip all pipeline lineage columns before writing to Postgres."""
    drop_cols = [
        "pipeline_name", "pipeline_run_id", "data_source",
        "input_source", "transformation", "record_created_at",
    ]
    return df.drop(columns=[c for c in drop_cols if c in df.columns])


def add_pipeline_metadata(df: pd.DataFrame, run_id: str) -> pd.DataFrame:
    """Stamp every output row with pipeline lineage. run_mode is NOT stamped
    here — it is added only inside write_to_snowflake_history."""
    df = df.copy()
    df["pipeline_name"]     = "collateral_pipeline"
    df["pipeline_run_id"]   = run_id
    df["data_source"]       = "snowflake"
    df["input_source"]      = "fx + equity + commodity + bonds"
    df["transformation"]    = "collateral_v1"
    df["record_created_at"] = datetime.utcnow()
    return df


# ============================================================
# SOURCE TABLE LOADERS
# ============================================================
def get_incremental_watermark(run_id: str) -> str:
    """
    Return the last successfully processed date from the COLLATERAL
    clean table. Used exclusively for incremental mode to set the
    lower bound of the processing window.
    """
    def _query():
        with get_snowflake_conn() as ctx:
            with ctx.cursor() as cs:
                cs.execute(
                    f"SELECT COALESCE(MAX(\"date\"), '1900-01-01') "
                    f"FROM \"{SNOWFLAKE_CLEAN_TABLE}\""
                )
                return str(cs.fetchone()[0])

    return retry_with_backoff(
        _query,
        retries=2,
        critical_name="Snowflake COLLATERAL watermark query",
        run_id=run_id,
    )


def load_source_table(table: str, where_clause: str, run_id: str) -> pd.DataFrame:
    """
    Load rows from a Snowflake source table using the provided WHERE clause.

    Mode-aware callers pass:
      Incremental    : WHERE "date" > '<watermark>'::DATE
      Replay/Backfill: WHERE "date" BETWEEN '<start>'::DATE AND '<end>'::DATE
    """
    def _load():
        query = f'SELECT * FROM "{table}" WHERE {where_clause}'
        with get_snowflake_conn() as ctx:
            with ctx.cursor() as cs:
                cs.execute(query)
                return cs.fetch_pandas_all()

    return retry_with_backoff(
        _load,
        retries=2,
        critical_name=f"Snowflake source table load: {table}",
        run_id=run_id,
    )


# ============================================================
# S3 — MACRO DATA (soft fail, non-critical)
# ============================================================
def load_macro_data() -> pd.DataFrame:
    """Load macro CSV from S3. Pipeline continues without it on failure."""
    def _load():
        obj = s3.get_object(Bucket=MACRO_BUCKET, Key=MACRO_KEY)
        df  = pd.read_csv(BytesIO(obj["Body"].read()))
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        return df[df["date"].notna()]

    try:
        return retry_with_backoff(_load, retries=2)
    except Exception as e:
        print(f"  [WARNING] Macro data load failed: {e} — continuing without macro")
        return pd.DataFrame()


# ============================================================
# DETAIL BUILDER — deterministic, no np.random
# ============================================================
def build_detail(
    df:          pd.DataFrame,
    asset_class: str,
    sym_col:     str,
    exp_col:     str,
) -> pd.DataFrame:
    """
    Build detailed collateral exposure records for one asset class.

    All previously stochastic assignments (counterparty, collateral type,
    agreement type, jurisdiction, reuse_flag) are replaced with deterministic
    SHA-256 hash-based functions keyed on stable business identifiers.

    Business logic (exposure calculations, margin metrics, funding cost,
    liquidity score) is unchanged from the original pipeline.

    Args:
        df:          Source DataFrame for the asset class.
        asset_class: One of "FX", "Equity", "Commodity", "Bond".
        sym_col:     Column name for the ticker/symbol.
        exp_col:     Column name for the exposure amount.

    Returns:
        Enriched detail DataFrame. Empty DataFrame if input is empty.
    """
    if df.empty:
        return pd.DataFrame()

    required = ["date", sym_col, exp_col, "sector", "industry"]
    if asset_class == "Commodity":
        required.append("commodity")
    df = ensure_cols(df.copy(), required)

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df[df["date"].notna()].copy()

    if df.empty:
        return pd.DataFrame()

    out = pd.DataFrame()
    out["date"]       = df["date"]
    out["asset_class"] = asset_class
    out["ticker"]     = df[sym_col].astype(str)
    out["sector"]     = df.get("sector", pd.Series("Unknown", index=df.index)).fillna("Unknown")
    out["industry"]   = df.get("industry", pd.Series("Unknown", index=df.index)).fillna("Unknown")

    # Carry staging columns for trade_id generation; will be dropped later
    out["commodity_sym"]  = df["commodity"] if asset_class == "Commodity" else np.nan
    out["asset_manager"]  = df.get("asset_manager", pd.Series(np.nan, index=df.index))

    # Derivative type label used in trade_id (consistent with derivatives pipeline)
    collateral_type_label = {
        "FX":        "FXFwd",
        "Commodity": "Futures",
        "Bond":      "IRS",
        "Equity":    "EqFwd",
    }.get(asset_class, asset_class)
    out["collateral_type_label"] = collateral_type_label

    # ── Deterministic trade_id ──────────────────────────────────────────
    out["trade_id"] = out.apply(generate_trade_id, axis=1)

    # ── Deterministic counterparty ──────────────────────────────────────
    out["counterparty"] = out["trade_id"].apply(det_counterparty)

    # ── Deterministic agreement_type + jurisdiction ──────────────────────
    agreements           = out["trade_id"].apply(det_agreement)
    out["agreement_type"] = agreements.apply(lambda x: x[0])
    out["jurisdiction"]   = agreements.apply(lambda x: x[1])

    # ── Deterministic collateral_type ───────────────────────────────────
    out["collateral_type"] = out["trade_id"].apply(
        lambda t: det_collateral_type(t, asset_class)
    )

    # ── Exposure and collateral metrics (UNCHANGED business logic) ───────
    out["exposure_before_collateral"] = (
        pd.to_numeric(df[exp_col], errors="coerce").abs()
    )
    out["haircut"]             = out["collateral_type"].map(HAIRCUT)
    out["initial_margin_rate"] = INITIAL_MARGIN[asset_class]

    out["required_collateral"]  = (
        out["exposure_before_collateral"] * out["initial_margin_rate"]
    )
    out["collateral_value"]     = out["required_collateral"] * 0.9
    out["effective_collateral"] = out["collateral_value"] * (1 - out["haircut"])
    out["net_exposure"]         = (
        out["exposure_before_collateral"] - out["effective_collateral"]
    ).clip(lower=0)
    out["collateral_ratio"] = np.where(
        out["exposure_before_collateral"] > 0,
        out["effective_collateral"] / out["exposure_before_collateral"],
        0.0,
    )
    out["margin_call_flag"]   = (out["net_exposure"] > 0).astype(int)
    out["margin_call_amount"] = out["net_exposure"]

    # ── Deterministic reuse_flag ─────────────────────────────────────────
    out["reuse_flag"]  = out["trade_id"].apply(det_reuse_flag)
    out["reused_value"] = out["collateral_value"] * out["reuse_flag"] * 0.8

    # ── Funding cost + liquidity score (UNCHANGED business logic) ────────
    out["funding_cost"] = out["collateral_value"] * 0.01 * (
        out["haircut"] + out["initial_margin_rate"]
    )
    out["liquidity_score"] = 1 - (out["haircut"] + out["initial_margin_rate"])

    # ── Collateral ID (deterministic MD5 of trade_id) ────────────────────
    out["collateral_id"] = out["trade_id"].apply(
        lambda x: hashlib.md5(x.encode()).hexdigest()
    )

    # Drop internal staging columns not intended for output
    out = out.drop(
        columns=["commodity_sym", "asset_manager", "collateral_type_label"],
        errors="ignore",
    )

    return out


# ============================================================
# SNOWFLAKE — HISTORY TABLE (append-only, always INSERT)
# ============================================================
def write_to_snowflake_history(
    df:         pd.DataFrame,
    table_name: str,
    run_mode:   str,
    run_id:     str  = None,
    chunk_size: int  = 100_000,
) -> int:
    """
    Append rows to COLLATERAL_HISTORY. Never updates or deletes existing rows.

    Adds run_mode so every row is traceable to the pipeline run that
    produced it. Duplicates across runs are expected — this table is
    an immutable audit trail.

    Raises on failure (after retries). Pipeline does NOT continue if
    this write fails.
    """
    df_hist             = df.copy()
    df_hist["run_mode"] = run_mode   # lineage column — history table only

    def do_insert():
        with get_snowflake_conn() as ctx:
            success, nchunks, nrows, _ = write_pandas(
                ctx, df_hist, table_name,
                chunk_size=chunk_size, quote_identifiers=True,
            )
            if not success:
                raise RuntimeError(
                    f"write_pandas returned success=False for {table_name}"
                )
            print(f"  [HISTORY] Inserted {nrows:,} rows into {table_name}.")
            return nrows

    try:
        return retry_with_backoff(
            do_insert,
            retries=3,
            critical_name=f"Snowflake HISTORY insert ({table_name})",
            run_id=run_id,
        )
    except Exception as exc:
        send_critical_alert(
            f"Snowflake HISTORY insert failed for {SNOWFLAKE_HISTORY_TABLE}",
            context={"run_id": run_id, "rows": len(df), "error": str(exc)},
        )
        raise


# ============================================================
# SNOWFLAKE — CLEAN TABLE — DELETE + INSERT (replay / backfill)
# ============================================================
def _snowflake_clean_delete_insert(
    df:         pd.DataFrame,
    table_name: str,
    start_date,
    end_date,
    run_id:     str = None,
    chunk_size: int = 100_000,
) -> None:
    """
    Atomically replace a date window in the COLLATERAL
    clean table.

    Guarantees:
    - no partial replay state
    - no conflicting clean-table rows
    - deterministic replay/backfill behavior

    Business uniqueness:
        trade_id + date
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
        f"{table_name}_STAGE_"
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
                    LIKE "{table_name}"
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
                        DELETE FROM "{table_name}"
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
                        f"{table_name} "
                        f"for window "
                        f"[{start_str}, {end_str}]"
                    )

                    # =================================================
                    # STEP 5 — INSERT RECOMPUTED WINDOW
                    # =================================================

                    cs.execute(f"""
                        INSERT INTO "{table_name}"
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
                        f"{table_name}"
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
                "table": table_name,
                "mode": "delete_insert",
                "start": start_str,
                "end": end_str,
                "rows": len(df),
                "error": str(exc),
            },
        )

        raise

# ============================================================
# SNOWFLAKE — CLEAN TABLE — MERGE (incremental)
# ============================================================
def _snowflake_clean_merge(
    df:         pd.DataFrame,
    table_name: str,
    run_id:     str  = None,
    chunk_size: int  = 100_000,
) -> None:
    """
    Incremental UPSERT into COLLATERAL clean table.

    Business key:
        (trade_id, date)

    Guarantees:
    - deterministic latest-state rows
    - idempotent retries
    - replay overlap safety
    - no conflicting clean-table rows
    """

    key_columns = [
        "trade_id",
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
            "Duplicate collateral business keys detected "
            f"before MERGE. Sample:\n{sample}"
        )

    temp_table = (
        f"{table_name}_STAGE_"
        f"{int(time.time())}"
    )

    try:

        with get_snowflake_conn() as ctx:

            with ctx.cursor() as cs:

                # =====================================================
                # CREATE TEMP STAGE TABLE
                # =====================================================

                cs.execute(f"""
                    CREATE TEMPORARY TABLE {temp_table}
                    LIKE "{table_name}"
                """)

                # =====================================================
                # LOAD INTO STAGE TABLE
                # =====================================================

                success, _, nrows, _ = write_pandas(
                    conn=ctx,
                    df=df,
                    table_name=temp_table,
                    chunk_size=chunk_size,
                    quote_identifiers=True,
                    auto_create_table=False,
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
                    MERGE INTO "{table_name}" t
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

                print(
                    f"  [CLEAN] MERGE completed into "
                    f"{table_name}"
                )

    except Exception as exc:

        send_critical_alert(
            "Snowflake CLEAN MERGE failed",
            context={
                "run_id": run_id,
                "table": table_name,
                "mode": "merge",
                "rows": len(df),
                "error": str(exc),
            },
        )

        raise

# ============================================================
# SNOWFLAKE — UNIFIED WRITE DISPATCHER
# ============================================================
def write_to_snowflake_clean(
    df:         pd.DataFrame,
    table_name: str,
    mode:       str,
    start_date,
    end_date,
    run_id:     str  = None,
    chunk_size: int  = 100_000,
) -> None:
    """
    Route to the correct Snowflake CLEAN write strategy based on mode.

    - "replay" / "backfill": DELETE window then INSERT (atomic transaction).
    - "incremental":         MERGE latest-state rows using
                             (trade_id, date).

    Wraps the strategy call in retry_with_backoff. Raises on exhaustion.
    """
    if mode in ("replay", "backfill"):
        strategy_name = f"Snowflake CLEAN delete+insert ({mode})"

        def do_write():
            _snowflake_clean_delete_insert(df, table_name, start_date, end_date, run_id, chunk_size)

    else:  # incremental

        strategy_name = "Snowflake CLEAN MERGE (incremental)"

        def do_write():
            _snowflake_clean_merge(
                df,
                table_name,
                run_id,
                chunk_size,
            )

    retry_with_backoff(
        do_write,
        retries=3,
        critical_name=strategy_name,
        run_id=run_id,
    )


# ============================================================
# POSTGRES — SERVING LAYER (consistency-first: RAISES on failure)
# ============================================================
def _write_single_postgres_table(
    df:         pd.DataFrame,
    table_name: str,
    mode:       str,
    start_str:  str,
    end_str:    str,
    retries:    int = 3,
) -> None:
    """
    Write a DataFrame to a single Postgres table.

    Steps (inside a single transaction per attempt):
      1. Replay / Backfill only: DELETE date window.
      2. INSERT new rows via COPY FROM STDIN.
      3. Trim to last 2 calendar days.

    All steps commit together. Any failure -> implicit rollback.
    RAISES on final failure.
    """
    if df.empty:
        print(f"  [POSTGRES] Empty DataFrame for {table_name} — nothing to write.")
        return

    df_pg = drop_metadata_for_serving(df.copy())

    # Cast boolean column
    if "margin_call_flag" in df_pg.columns:
        df_pg["margin_call_flag"] = df_pg["margin_call_flag"].astype(bool)

    last_error = None

    for attempt in range(retries + 1):
        try:
            df_attempt = df_pg.copy()

            with get_postgre_conn() as pg_conn:
                with pg_conn.cursor() as pg_cur:

                    # Step 1: delete stale window (replay / backfill only)
                    if mode in ("replay", "backfill"):
                        pg_cur.execute(
                            f"DELETE FROM public.{table_name} "
                            f"WHERE date BETWEEN %s::DATE AND %s::DATE",
                            (start_str, end_str),
                        )
                        deleted = pg_cur.rowcount if pg_cur.rowcount is not None else 0
                        print(
                            f"  [POSTGRES] Deleted {deleted:,} rows from {table_name} "
                            f"for window [{start_str}, {end_str}]."
                        )

                    # Step 2: validate live schema
                    pg_cur.execute(f"""
                        SELECT column_name
                        FROM   information_schema.columns
                        WHERE  table_schema = 'public'
                          AND  table_name   = '{table_name}'
                        ORDER  BY ordinal_position
                    """)
                    pg_cols = [
                        r[0] for r in pg_cur.fetchall()
                        if r[0].lower() != "id"
                    ]
                    if not pg_cols:
                        raise RuntimeError(
                            f"Schema query returned 0 columns for public.{table_name}"
                        )
                    missing = set(pg_cols) - set(df_attempt.columns)
                    if missing:
                        raise ValueError(
                            f"DataFrame missing Postgres columns for {table_name}: {missing}"
                        )

                    df_ordered = df_attempt[pg_cols].copy()

                    # Step 3: bulk INSERT via COPY
                    copy_sql = (
                        f"COPY public.{table_name} "
                        f"({', '.join(pg_cols)}) FROM STDIN WITH CSV"
                    )
                    buf = StringIO()
                    df_ordered.to_csv(buf, index=False, header=False)
                    buf.seek(0)
                    with pg_cur.copy(copy_sql) as copy:
                        copy.write(buf.getvalue())

                    # Step 4: trim to last 2 calendar days
                    pg_cur.execute(f"""
                        DELETE FROM public.{table_name}
                        WHERE date <= (
                            SELECT MAX(date) FROM public.{table_name}
                        ) - INTERVAL '2 days'
                    """)
                    trimmed = pg_cur.rowcount if pg_cur.rowcount is not None else 0

                pg_conn.commit()

            print(
                f"  [POSTGRES] {table_name}: inserted {len(df_ordered):,} rows, "
                f"trimmed {trimmed:,} rows (keeping last 2 days)."
            )
            return  # success — exit retry loop

        except Exception as e:
            last_error = e
            if attempt < retries:
                wait_time = 2 ** attempt
                print(
                    f"  [POSTGRES] {table_name} retry {attempt + 1}/{retries} "
                    f"after {wait_time}s: {e}"
                )
                time.sleep(wait_time)
            else:
                print(
                    f"  [POSTGRES] {table_name} all {retries} retries exhausted: {e}"
                )

    # All retries exhausted -> raise to fail the pipeline
    send_critical_alert(
        f"Postgres write to {table_name} failed after {retries} retries",
        context={"error": str(last_error), "rows": len(df_pg)},
    )
    raise RuntimeError(
        f"Postgres write to public.{table_name} failed after {retries} retries: "
        f"{last_error}"
    )


def write_to_postgres(
    df_detail:  pd.DataFrame,
    df_model:   pd.DataFrame,
    mode:       str,
    start_date,
    end_date,
    retries:    int = 3,
) -> None:
    """
    Write both the detail and model DataFrames to their respective Postgres
    serving tables. Both writes must succeed — any failure raises.

    Args:
        df_detail:  Detail-level collateral DataFrame.
        df_model:   Aggregated model-level DataFrame.
        mode:       "incremental" | "backfill" | "replay".
        start_date: Window start (used for DELETE in replay/backfill).
        end_date:   Window end (used for DELETE in replay/backfill).
        retries:    Maximum retry attempts per table.

    Raises:
        RuntimeError: When all retry attempts are exhausted for either table.
    """
    start_str = pd.Timestamp(start_date).strftime("%Y-%m-%d")
    end_str   = pd.Timestamp(end_date).strftime("%Y-%m-%d")

    _write_single_postgres_table(
        df_detail, POSTGRES_DETAIL_TABLE, mode, start_str, end_str, retries
    )
    _write_single_postgres_table(
        df_model, POSTGRES_MODEL_TABLE, mode, start_str, end_str, retries
    )


# ============================================================
# MAIN PIPELINE ENTRY POINT
# ============================================================
def run_collateral_pipeline(
    start_date_override: str  = None,
    replay:              bool = False,
) -> str:
    """
    Production-grade collateral pipeline.

    Mode detection (derived from parameters ONLY — never from data columns):
    ┌─────────────────────────────────┬──────────────┐
    │ Condition                       │ mode         │
    ├─────────────────────────────────┼──────────────┤
    │ replay = True                   │ "replay"     │
    │ start_date_override provided    │ "backfill"   │
    │ default (neither)               │ "incremental"│
    └─────────────────────────────────┴──────────────┘

    Source table load strategy:
    ┌──────────────┬──────────────────────────────────────────────────────┐
    │ Mode         │ WHERE clause applied to FX / EQUITY / COMMODITY /    │
    │              │ BONDS source tables                                  │
    ├──────────────┼──────────────────────────────────────────────────────┤
    │ replay       │ "date" BETWEEN start_date AND end_date (today)       │
    │ backfill     │ "date" BETWEEN start_date AND end_date (today)       │
    │ incremental  │ "date" > watermark from COLLATERAL clean table       │
    └──────────────┴──────────────────────────────────────────────────────┘

    Snowflake writes (TWO TABLES):
    - COLLATERAL_HISTORY: always INSERT, includes run_mode column
    - COLLATERAL:         DELETE window + INSERT (replay/backfill)
                          MERGE latest-state rows using
                          (trade_id, date)

    Postgres writes (serving layer — TWO TABLES):
    - Replay/backfill:  DELETE window -> INSERT -> trim to last 2 days
    - Incremental:      INSERT -> trim to last 2 days

    Failure contract:
    - Pipeline fails if EITHER Snowflake write OR Postgres write fails.
    - Partial success is NOT allowed.
    - Macro data failure is soft-fail (pipeline continues without it).

    Args:
        start_date_override: "YYYY-MM-DD". If provided and replay=False
                             -> backfill mode.
        replay:              True -> replay mode. Loads source tables by
                             date range and recomputes deterministically.

    Returns:
        str: Status string for Airflow XCom.

    Raises:
        Any unhandled exception triggers a CRITICAL alert and re-raises
        so Airflow marks the task as FAILED.
    """
    pipeline_start = time.time()
    run_id         = datetime.utcnow().isoformat()

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
    end_date = today

    print(f"\n{'=' * 66}")
    print(f"  COLLATERAL PIPELINE START")
    print(f"  run_id              : {run_id}")
    print(f"  mode                : {mode}")
    print(f"  start_date_override : {start_date_override}")
    print(f"  replay              : {replay}")
    print(f"  end_date            : {end_date.date()}")
    print(f"{'=' * 66}\n")

    try:
        # ══════════════════════════════════════════════════════════════
        # STEP 1 — Determine processing window + build WHERE clause
        # ══════════════════════════════════════════════════════════════
        print("  [STEP 1] Determining processing window...")

        if mode in ("replay", "backfill"):
            if not start_date_override:
                raise ValueError(
                    f"mode='{mode}' requires start_date_override to be provided."
                )
            start_date   = pd.Timestamp(start_date_override).normalize()
            start_str    = start_date.strftime("%Y-%m-%d")
            end_str      = end_date.strftime("%Y-%m-%d")
            where_clause = (
                f'"date" BETWEEN \'{start_str}\'::DATE AND \'{end_str}\'::DATE'
            )
            print(
                f"  [{mode.upper()}] Processing window: "
                f"{start_str} -> {end_str}"
            )

        else:  # incremental
            watermark_str = get_incremental_watermark(run_id)
            last_date     = pd.Timestamp(watermark_str).normalize()

            print(f"  [INCREMENTAL] Last processed date: {last_date.date()}")
            print(f"  [INCREMENTAL] Today              : {today.date()}")

            if last_date.date() >= today.date():
                print("  [INCREMENTAL] Watermark is current — no new data to process.")
                return "NO_NEW_DATA"

            start_date   = last_date
            start_str    = last_date.strftime("%Y-%m-%d")
            end_str      = end_date.strftime("%Y-%m-%d")
            where_clause = f'"date" > \'{start_str}\'::DATE'
            print(f"  [INCREMENTAL] Loading rows with date > {start_str}")

        # ══════════════════════════════════════════════════════════════
        # STEP 2 — Load source tables from Snowflake
        # All four tables use the SAME WHERE clause so the date window
        # is consistent across all asset classes.
        # ══════════════════════════════════════════════════════════════
        print("\n  [STEP 2] Loading source tables from Snowflake...")

        fx_df  = load_source_table("FX",        where_clause, run_id)
        eq_df  = load_source_table("EQUITY",     where_clause, run_id)
        cm_df  = load_source_table("COMMODITY",  where_clause, run_id)
        bd_df  = load_source_table("BONDS",      where_clause, run_id)

        print(
            f"  FX: {len(fx_df):,}  Equity: {len(eq_df):,}  "
            f"Commodity: {len(cm_df):,}  Bonds: {len(bd_df):,}"
        )

        # ══════════════════════════════════════════════════════════════
        # STEP 3 — Build detail exposure records per asset class
        # Business logic and metric calculations are UNCHANGED.
        # All stochastic assignments replaced with deterministic hashes.
        # ══════════════════════════════════════════════════════════════
        print("\n  [STEP 3] Building detail exposure records...")

        fx  = build_detail(fx_df,  "FX",        "ticker", "exposure_amount")
        eq  = build_detail(eq_df,  "Equity",    "ticker", "mtm_value")
        cm  = build_detail(cm_df,  "Commodity", "ticker", "exposure_amount")
        bd  = build_detail(bd_df,  "Bond",      "ticker", "bond_price")

        df_detail = pd.concat([fx, eq, cm, bd], ignore_index=True)

        if df_detail.empty:
            print("  No new rows to process — pipeline complete.")
            return "NO_NEW_ROWS"

        print(f"  Detail rows: {len(df_detail):,}")

        # ══════════════════════════════════════════════════════════════
        # STEP 4 — Aggregate to model level (UNCHANGED business logic)
        # ══════════════════════════════════════════════════════════════
        print("\n  [STEP 4] Aggregating to model level...")

        df_model = df_detail.groupby(
            ["date", "asset_class", "ticker", "counterparty"],
            as_index=False,
        ).agg({
            "exposure_before_collateral": "sum",
            "required_collateral":        "sum",
            "collateral_value":           "sum",
            "effective_collateral":       "sum",
            "net_exposure":               "sum",
            "margin_call_amount":         "sum",
            "funding_cost":               "sum",
            "liquidity_score":            "mean",
        })

        df_model["collateral_ratio"] = (
            df_model["effective_collateral"] / df_model["exposure_before_collateral"]
        ).fillna(0)
        df_model["margin_call_flag"] = (df_model["net_exposure"] > 0).astype(int)

        print(f"  Model rows: {len(df_model):,}")

        # ══════════════════════════════════════════════════════════════
        # STEP 5 — Merge macro data (soft fail — pipeline continues)
        # ══════════════════════════════════════════════════════════════
        print("\n  [STEP 5] Merging macro data...")

        macro = load_macro_data()

        if not macro.empty:
            df_detail["month_year"] = df_detail["date"].dt.to_period("M").astype(str)
            macro["month_year"]     = macro["date"].dt.to_period("M").astype(str)

            df_detail = df_detail.merge(
                macro.drop(columns=["date"]),
                on="month_year",
                how="inner",
            ).drop(columns=["month_year"])

            if df_detail.empty:
                print(
                    "  [WARNING] No rows after macro merge — "
                    "continuing without macro."
                )
                for col in ["gdp", "unrate", "cpi", "fedfunds"]:
                    df_detail[col] = np.nan
        else:
            print("  [WARNING] Macro data not available — continuing without macro.")
            for col in ["gdp", "unrate", "cpi", "fedfunds"]:
                if col not in df_detail.columns:
                    df_detail[col] = np.nan

        print(f"  Detail rows after macro merge: {len(df_detail):,}")

        # Convert dates to date objects for Snowflake / Postgres
        df_detail["date"] = pd.to_datetime(df_detail["date"]).dt.date
        df_model["date"]  = pd.to_datetime(df_model["date"]).dt.date

        # ══════════════════════════════════════════════════════════════
        # STEP 6 — Attach pipeline metadata
        # run_mode is NOT stamped here — added only in write_to_snowflake_history
        # ══════════════════════════════════════════════════════════════
        print("\n  [STEP 6] Adding pipeline metadata...")

        df_detail = add_pipeline_metadata(df_detail, run_id)
        df_model  = add_pipeline_metadata(df_model,  run_id)

        snowflake_detail_rows = len(df_detail)
        print(f"  Final detail rows: {snowflake_detail_rows:,}")

        if df_detail.empty:
            print("  No valid rows after processing — pipeline complete.")
            return "NO_VALID_ROWS"

        # ══════════════════════════════════════════════════════════════
        # STEP 7 — Write to Snowflake HISTORY (always INSERT)
        # Failure here -> pipeline FAILS.
        # ══════════════════════════════════════════════════════════════
        print(
            f"\n  [STEP 7] Writing to Snowflake HISTORY "
            f"({SNOWFLAKE_HISTORY_TABLE})..."
        )
        
        write_to_snowflake_history(
            df_detail,
            table_name=SNOWFLAKE_HISTORY_TABLE,
            run_mode=mode,
            run_id=run_id,
        )

        write_to_snowflake_history(
            df_model,
            table_name=SNOWFLAKE_MODEL_HISTORY_TABLE,
            run_mode=mode,
            run_id=run_id,
        )

        # ══════════════════════════════════════════════════════════════
        # STEP 8 — Write to Snowflake CLEAN (deterministic latest state)
        # replay/backfill -> DELETE window + INSERT (transactional)
        # incremental     -> MERGE latest-state rows
        # Failure here -> pipeline FAILS.
        # ══════════════════════════════════════════════════════════════
        print(
            f"\n  [STEP 8] Writing to Snowflake CLEAN tables "
            f"(mode='{mode}')..."
        )
        write_to_snowflake_clean(
            df_detail,
            table_name=SNOWFLAKE_CLEAN_TABLE,
            mode=mode,
            start_date=start_date,
            end_date=end_date,
            run_id=run_id,
        )

        write_to_snowflake_clean(
            df_model,
            table_name=SNOWFLAKE_MODEL_CLEAN_TABLE,
            mode=mode,
            start_date=start_date,
            end_date=end_date,
            run_id=run_id,
        )

        # ══════════════════════════════════════════════════════════════
        # STEP 9 — Write to Postgres serving layer (both tables)
        # replay/backfill -> DELETE window + INSERT + trim to last 2 days
        # incremental     -> INSERT + trim to last 2 days
        # Failure here -> pipeline FAILS (consistency-first).
        # ══════════════════════════════════════════════════════════════
        print(
            f"\n  [STEP 9] Writing to Postgres serving layer "
            f"(mode='{mode}')..."
        )
        write_to_postgres(
            df_detail=df_detail,
            df_model=df_model,
            mode=mode,
            start_date=start_date,
            end_date=end_date,
        )

        # ══════════════════════════════════════════════════════════════
        # STEP 10 — Pipeline success
        # ══════════════════════════════════════════════════════════════
        processing_time = round(time.time() - pipeline_start, 2)

        send_success_alert({
            "run_id":               run_id,
            "mode":                 mode,
            "snowflake_detail_rows": snowflake_detail_rows,
            "duration_seconds":     processing_time,
        })

        print(f"\n{'=' * 66}")
        print(f"  COLLATERAL PIPELINE SUCCESS")
        print(f"  run_id           : {run_id}")
        print(f"  mode             : {mode}")
        print(f"  rows processed   : {snowflake_detail_rows:,}")
        print(f"  window           : {start_str} -> {end_str}")
        print(f"  duration         : {processing_time}s")
        print(f"  Snowflake HISTORY: OK")
        print(f"  Snowflake CLEAN  : OK")
        print(f"  Postgres         : OK")
        print(f"{'=' * 66}\n")

        return f"SUCCESS_{snowflake_detail_rows}"

    except Exception as e:
        processing_time = round(time.time() - pipeline_start, 2)
        send_critical_alert(
            "Collateral pipeline failed with unhandled exception",
            context={
                "run_id":   run_id,
                "mode":     mode,
                "duration": f"{processing_time}s",
                "error":    str(e),
            },
        )
        print(f"\n{'=' * 66}")
        print(f"  COLLATERAL PIPELINE FAILED")
        print(f"  run_id   : {run_id}")
        print(f"  mode     : {mode}")
        print(f"  error    : {e}")
        print(f"  duration : {processing_time}s")
        print(f"{'=' * 66}\n")
        raise