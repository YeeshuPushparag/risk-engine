"""
derivatives_pipeline.py
=======================
Production-grade derivatives pipeline — consistency-first architecture.

This pipeline consumes the EQUITY, FX, COMMODITY, and BONDS Snowflake clean
tables, computes derivative exposures, and writes results to two Snowflake
tables and a Postgres serving layer.

Storage architecture
--------------------
Snowflake DERIVATIVES_HISTORY  (append-only, audit trail)
    - Always INSERT; never DELETE or UPDATE
    - Includes run_mode column for lineage tracing
    - Duplicates across runs are EXPECTED and intentional

Snowflake DERIVATIVES          (clean, deterministic latest state)
    - Replay / Backfill:  DELETE window -> INSERT fresh rows (transactional)
    - Incremental:        INSERT only new rows (watermark prevents duplicates)
    - Must NEVER contain conflicting rows for same (trade_id, date)

Postgres derivative_data       (serving layer — NOT source of truth)
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

Source table load strategy
--------------------------
    Incremental  : WHERE "date" > <watermark from DERIVATIVES>
    Replay/Backfill : WHERE "date" BETWEEN start_date AND end_date

Determinism
-----------
All previously stochastic assignments (counterparty, tenor, agreement type,
jurisdiction) are replaced with deterministic SHA-256 hash-based functions
keyed on stable business identifiers (ticker, asset_class, date). Replay
runs are guaranteed to produce identical outputs for identical inputs.

DAG integration
---------------
master_dag.py calls run_derivatives_processing(start_date_override, replay).
Signature matches exactly.
"""

import os
import time
import hashlib
from io import BytesIO, StringIO
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
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

INITIAL_MARGIN = {
    "FX":        0.05,
    "Commodity": 0.08,
    "Bond":      0.04,
    "Equity":    0.10,
}
HAIRCUT = {
    "Cash":          0.00,
    "Treasury":      0.02,
    "CorporateBond": 0.10,
    "Equity":        0.15,
}
COLLATERAL_TYPE_MAP = {
    "FX":        "Cash",
    "Commodity": "Treasury",
    "Bond":      "Treasury",
    "Equity":    "Equity",
}

# Agreement pool: (agreement_type, jurisdiction, cumulative_probability)
AGREEMENT_POOL = [
    ("CSA",   "US", 0.45),
    ("CSA",   "UK", 0.25),
    ("CSA",   "EU", 0.20),
    ("GMRA",  "UK", 0.05),
    ("GMSLA", "US", 0.05),
]

TENOR_OPTIONS     = [0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0]
COUNTERPARTY_POOL = ["CP_A", "CP_B", "CP_C", "CP_D", "CP_E"]

# Snowflake table names — single source of truth
SNOWFLAKE_CLEAN_TABLE   = "DERIVATIVES"
SNOWFLAKE_HISTORY_TABLE = "DERIVATIVES_HISTORY"

# Postgres table name
POSTGRES_TABLE = "derivative_data"


# ============================================================
# PRODUCTION-GRADE ALERTING (CRITICAL ONLY)
# ============================================================
def send_critical_alert(message: str, context: dict = None):
    """
    Send CRITICAL alert for unrecoverable pipeline failures only.

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
        "level":     "CRITICAL",
        "pipeline":  "derivatives_pipeline",
        "message":   message,
        "context":   context or {},
        "timestamp": datetime.utcnow().isoformat(),
    }
    print(f"[CRITICAL] {message} | context={payload['context']}")
    webhook = os.getenv("SLACK_WEBHOOK_URL")
    if webhook:
        try:
            truncated = message[:500] + "..." if len(message) > 500 else message
            run_id    = context.get("run_id", "unknown") if context else "unknown"
            text = (
                f"*[CRITICAL]* derivatives_pipeline\n"
                f"{truncated}\n"
                f"run_id: {run_id}"
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
# All assignments previously made with np.random.choice / np.random.seed
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


def det_tenor(key: str) -> float:
    """Assign tenor (years) deterministically from trade key."""
    return TENOR_OPTIONS[_stable_hash(key) % len(TENOR_OPTIONS)]


def det_agreement(key: str) -> tuple:
    """
    Return (agreement_type, jurisdiction) deterministically.

    Uses cumulative-probability lookup on AGREEMENT_POOL so the
    frequency distribution is preserved across replay runs.
    """
    r = (_stable_hash(key) % 10_000) / 10_000.0
    cumulative = 0.0
    for ag_type, jurisdiction, weight in AGREEMENT_POOL:
        cumulative += weight
        if r < cumulative:
            return ag_type, jurisdiction
    return AGREEMENT_POOL[-1][0], AGREEMENT_POOL[-1][1]


# ============================================================
# BUSINESS LOGIC HELPERS
# ============================================================
def ensure_cols(df: pd.DataFrame, cols: list) -> pd.DataFrame:
    """Add missing columns as NaN so downstream logic never KeyErrors."""
    for c in cols:
        if c not in df.columns:
            df[c] = np.nan
    return df


def parse_dates_safe(series: pd.Series) -> pd.Series:
    """Parse a date series with fallback to dayfirst format."""
    d1 = pd.to_datetime(series, errors="coerce")
    if d1.notna().sum() >= len(series) * 0.8:
        return d1
    return pd.to_datetime(series, errors="coerce", dayfirst=True)


def generate_trade_id(row) -> str:
    """
    Generate a deterministic trade identifier from stable business keys.

    Key fields: derivative_type + ticker + date + optional commodity/manager.
    This replaces any stochastic component so every replay produces the
    same trade_id for the same position.
    """
    parts = [
        str(row.get("derivative_type", "")),
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
        "input_source", "transformation", "record_created_at", "run_mode", "agreement_type", "jurisdiction"
    ]
    return df.drop(columns=[c for c in drop_cols if c in df.columns])


def add_pipeline_metadata(df: pd.DataFrame, run_id: str) -> pd.DataFrame:
    df = df.copy()
    df["pipeline_name"]    = "derivatives_pipeline"
    df["pipeline_run_id"]  = run_id
    df["data_source"]      = "snowflake"
    df["input_source"]     = "fx + commodity + bonds + equity"
    df["transformation"]   = "derivatives_v1"
    df["record_created_at"] = datetime.utcnow()
    return df


# ============================================================
# SOURCE TABLE LOADERS
# ============================================================
def get_incremental_watermark(run_id: str) -> str:
    """
    Return the last successfully processed date from the DERIVATIVES
    clean table. Used exclusively for incremental mode to set the
    lower bound of the processing window.
    """
    def _query():
        with get_snowflake_conn() as ctx:
            with ctx.cursor() as cs:
                cs.execute(
                    f'SELECT COALESCE(MAX("date"), \'1900-01-01\') '
                    f'FROM "{SNOWFLAKE_CLEAN_TABLE}"'
                )
                return str(cs.fetchone()[0])

    return retry_with_backoff(
        _query,
        retries=2,
        critical_name="Snowflake DERIVATIVES watermark query",
        run_id=run_id,
    )


def load_source_table(table: str, where_clause: str, run_id: str) -> pd.DataFrame:
    """
    Load rows from a Snowflake source table using the provided WHERE clause.

    Mode-aware callers pass:
      Incremental   : WHERE "date" > '<watermark>'::DATE
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
# SNOWFLAKE — HISTORY TABLE (append-only, always INSERT)
# ============================================================
def write_to_snowflake_history(
    df: pd.DataFrame,
    run_mode: str,
    run_id: str = None,
    chunk_size: int = 20_000,
):
    """
    Append rows to DERIVATIVES_HISTORY. Never updates or deletes.

    Adds run_mode so every row is traceable to the pipeline run that
    produced it. Duplicates across runs are expected — this table is
    an immutable audit trail.

    Raises on failure (after retries). Pipeline does NOT continue if
    this write fails.
    """
    df_hist             = df.copy()
    df_hist["run_mode"] = run_mode

    def do_insert():
        with get_snowflake_conn() as ctx:
            success, nchunks, nrows, _ = write_pandas(
                ctx, df_hist, SNOWFLAKE_HISTORY_TABLE,
                chunk_size=chunk_size, quote_identifiers=True,
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
            f"Snowflake HISTORY insert failed for {SNOWFLAKE_HISTORY_TABLE}",
            context={"run_id": run_id, "rows": len(df), "error": str(exc)},
        )
        raise


# ============================================================
# SNOWFLAKE — CLEAN TABLE — DELETE + INSERT (replay / backfill)
# ============================================================
def _snowflake_clean_delete_insert(
    df: pd.DataFrame,
    start_date,
    end_date,
    run_id: str = None,
    chunk_size: int = 20_000,
):
    """
    Atomically replace a date window in the DERIVATIVES (clean) table.

    Protocol:
      1. Load new rows into a session-scoped temp staging table.
      2. BEGIN TRANSACTION.
      3. DELETE existing rows for the window [start_date, end_date].
      4. INSERT all rows from the staging table.
      5. COMMIT on success; ROLLBACK on any failure.

    Guarantees the DERIVATIVES table never holds conflicting rows for the
    same (trade_id, date) after a replay or backfill.
    """
    start_str  = pd.Timestamp(start_date).strftime("%Y-%m-%d")
    end_str    = pd.Timestamp(end_date).strftime("%Y-%m-%d")
    temp_table = f"{SNOWFLAKE_CLEAN_TABLE}_STAGE_{int(time.time())}"

    try:
        with get_snowflake_conn() as ctx:
            with ctx.cursor() as cs:
                # Step 1 — staging table (auto-dropped when session closes)
                cs.execute(
                    f'CREATE TEMPORARY TABLE {temp_table} LIKE "{SNOWFLAKE_CLEAN_TABLE}"'
                )
                write_pandas(
                    ctx, df, temp_table,
                    chunk_size=chunk_size, quote_identifiers=True,
                )

                # Step 2 — open explicit transaction
                cs.execute("BEGIN TRANSACTION")
                try:
                    # Step 3 — DELETE existing window rows
                    cs.execute(f"""
                        DELETE FROM "{SNOWFLAKE_CLEAN_TABLE}"
                        WHERE "date" BETWEEN '{start_str}'::DATE AND '{end_str}'::DATE
                    """)
                    deleted = cs.rowcount if cs.rowcount is not None else 0
                    print(
                        f"  [CLEAN] Deleted {deleted:,} rows from {SNOWFLAKE_CLEAN_TABLE} "
                        f"for window [{start_str}, {end_str}]"
                    )

                    # Step 4 — INSERT fresh rows from staging
                    cs.execute(
                        f'INSERT INTO "{SNOWFLAKE_CLEAN_TABLE}" SELECT * FROM {temp_table}'
                    )
                    cs.execute("SELECT ROW_COUNT()")
                    inserted = cs.fetchone()[0]
                    print(
                        f"  [CLEAN] Inserted {inserted:,} rows into {SNOWFLAKE_CLEAN_TABLE}"
                    )

                    # Step 5 — commit atomically
                    cs.execute("COMMIT")

                except Exception:
                    cs.execute("ROLLBACK")
                    raise

    except Exception as exc:
        send_critical_alert(
            f"Snowflake CLEAN delete+insert failed — window [{start_str}, {end_str}] "
            f"may be partially written. Immediate investigation required.",
            context={
                "run_id": run_id,
                "table":  SNOWFLAKE_CLEAN_TABLE,
                "mode":   "delete_insert",
                "start":  start_str,
                "end":    end_str,
                "rows":   len(df),
                "error":  str(exc),
            },
        )
        raise


# ============================================================
# SNOWFLAKE — CLEAN TABLE — INSERT (incremental, watermark-safe)
# ============================================================
def _snowflake_clean_insert(
    df: pd.DataFrame,
    run_id: str = None,
    chunk_size: int = 20_000,
):
    """
    Insert new rows into DERIVATIVES (clean) for incremental runs.

    The watermark-based date filter applied upstream guarantees these rows
    are strictly new — no duplicates on (trade_id, date) will be produced.
    """
    try:
        with get_snowflake_conn() as ctx:
            success, nchunks, nrows, _ = write_pandas(
                ctx, df, SNOWFLAKE_CLEAN_TABLE,
                chunk_size=chunk_size, quote_identifiers=True,
            )
            if not success:
                raise RuntimeError(
                    f"write_pandas returned success=False for {SNOWFLAKE_CLEAN_TABLE}"
                )
            print(
                f"  [CLEAN] Inserted {nrows:,} rows into {SNOWFLAKE_CLEAN_TABLE} "
                f"(incremental)"
            )
    except Exception as exc:
        send_critical_alert(
            f"Snowflake CLEAN insert failed",
            context={
                "run_id": run_id,
                "table":  SNOWFLAKE_CLEAN_TABLE,
                "mode":   "insert",
                "rows":   len(df),
                "error":  str(exc),
            },
        )
        raise


# ============================================================
# SNOWFLAKE — UNIFIED WRITE DISPATCHER
# ============================================================
def write_to_snowflake_clean(
    df: pd.DataFrame,
    mode: str,
    start_date,
    end_date,
    run_id: str = None,
    chunk_size: int = 20_000,
):
    """
    Route to the correct Snowflake CLEAN write strategy based on mode.

    - "replay" / "backfill": DELETE window then INSERT (atomic transaction).
    - "incremental":         INSERT only (watermark ensures no duplicates).

    Wraps the strategy call in retry_with_backoff. Raises on exhaustion.
    """
    if mode in ("replay", "backfill"):
        strategy_name = f"Snowflake CLEAN delete+insert ({mode})"

        def do_write():
            _snowflake_clean_delete_insert(df, start_date, end_date, run_id, chunk_size)

    else:  # incremental
        strategy_name = "Snowflake CLEAN INSERT (incremental)"

        def do_write():
            _snowflake_clean_insert(df, run_id, chunk_size)

    retry_with_backoff(
        do_write,
        retries=3,
        critical_name=strategy_name,
        run_id=run_id,
    )


# ============================================================
# POSTGRES — SERVING LAYER (consistency-first: RAISES on failure)
# ============================================================
def write_to_postgres(
    df: pd.DataFrame,
    mode: str,
    start_date,
    end_date,
    retries: int = 3,
):
    """
    Write enriched derivative rows to the Postgres serving layer.

    Steps (inside a single transaction per attempt):
      1. Replay / Backfill only:
            DELETE FROM derivative_data WHERE date BETWEEN start AND end
      2. INSERT new rows via COPY (most efficient Postgres bulk load).
      3. Trim to last 2 calendar days:
            DELETE WHERE date < MAX(date) - INTERVAL '2 days'

    All three steps commit together. Any step failure -> implicit rollback.

    RAISES on final failure — pipeline does NOT continue if Postgres fails.
    This enforces the consistency-first contract.
    """
    if df.empty:
        print("  [POSTGRES] Empty DataFrame — nothing to write.")
        return

    start_str = pd.Timestamp(start_date).strftime("%Y-%m-%d")
    end_str   = pd.Timestamp(end_date).strftime("%Y-%m-%d")

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
                            f"DELETE FROM public.{POSTGRES_TABLE} "
                            f"WHERE date BETWEEN %s::DATE AND %s::DATE",
                            (start_str, end_str),
                        )
                        deleted = pg_cur.rowcount if pg_cur.rowcount is not None else 0
                        print(
                            f"  [POSTGRES] Deleted {deleted:,} rows "
                            f"for window [{start_str}, {end_str}]"
                        )

                    # Step 2: validate live schema
                    pg_cur.execute(f"""
                        SELECT column_name
                        FROM   information_schema.columns
                        WHERE  table_schema = 'public'
                          AND  table_name   = '{POSTGRES_TABLE}'
                        ORDER  BY ordinal_position
                    """)
                    pg_cols = [
                        r[0] for r in pg_cur.fetchall()
                        if r[0].lower() != "id"
                    ]
                    if not pg_cols:
                        raise RuntimeError(
                            f"Schema query returned 0 columns for {POSTGRES_TABLE}"
                        )
                    missing = set(pg_cols) - set(df_attempt.columns)
                    if missing:
                        raise ValueError(
                            f"DataFrame missing Postgres columns: {missing}"
                        )

                    df_ordered = df_attempt[pg_cols].copy()

                    # Step 3: bulk INSERT via COPY
                    copy_sql = (
                        f"COPY public.{POSTGRES_TABLE} "
                        f"({', '.join(pg_cols)}) FROM STDIN WITH CSV"
                    )
                    buf = StringIO()
                    df_ordered.to_csv(buf, index=False, header=False)
                    buf.seek(0)
                    with pg_cur.copy(copy_sql) as copy:
                        copy.write(buf.getvalue())

                    # Step 4: trim to last 2 calendar days
                    pg_cur.execute(f"""
                        DELETE FROM public.{POSTGRES_TABLE}
                        WHERE date < (
                            SELECT MAX(date) FROM public.{POSTGRES_TABLE}
                        ) - INTERVAL '2 days'
                    """)
                    trimmed = pg_cur.rowcount if pg_cur.rowcount is not None else 0

                pg_conn.commit()

            print(
                f"  [POSTGRES] Inserted {len(df_ordered):,} rows, "
                f"trimmed {trimmed:,} rows (keeping last 2 days)"
            )
            return  # success — exit retry loop

        except Exception as e:
            last_error = e
            if attempt < retries:
                wait_time = 2 ** attempt
                print(
                    f"  [POSTGRES] Retry {attempt + 1}/{retries} "
                    f"after {wait_time}s: {e}"
                )
                time.sleep(wait_time)
            else:
                print(f"  [POSTGRES] All {retries} retries exhausted: {e}")

    # All retries exhausted -> raise to fail the pipeline
    send_critical_alert(
        f"Postgres write to {POSTGRES_TABLE} failed after {retries} retries",
        context={"error": str(last_error), "rows": len(df_pg)},
    )
    raise RuntimeError(
        f"Postgres write to {POSTGRES_TABLE} failed after {retries} retries: {last_error}"
    )


# ============================================================
# MAIN PIPELINE ENTRY POINT
# ============================================================
def run_derivatives_processing(
    start_date_override: str  = None,
    replay:              bool = False,
):
    """
    Production-grade derivatives pipeline.

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
    │ Mode         │ WHERE clause applied to FX / COMMODITY / BONDS /     │
    │              │ EQUITY source tables                                 │
    ├──────────────┼──────────────────────────────────────────────────────┤
    │ replay       │ "date" BETWEEN start_date AND end_date (today)       │
    │ backfill     │ "date" BETWEEN start_date AND end_date (today)       │
    │ incremental  │ "date" > watermark from DERIVATIVES clean table      │
    └──────────────┴──────────────────────────────────────────────────────┘

    Snowflake writes (TWO TABLES):
    - DERIVATIVES_HISTORY: always INSERT, includes run_mode column
    - DERIVATIVES:         DELETE window + INSERT (replay/backfill)
                           INSERT only (incremental, watermark-safe)

    Postgres writes (serving layer):
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

    print(f"\n{'='*66}")
    print(f"  DERIVATIVES PIPELINE START")
    print(f"  run_id              : {run_id}")
    print(f"  mode                : {mode}")
    print(f"  start_date_override : {start_date_override}")
    print(f"  replay              : {replay}")
    print(f"  end_date            : {end_date.date()}")
    print(f"{'='*66}\n")

    try:
        # ══════════════════════════════════════════════════════════════
        # STEP 1 — Determine processing window + build WHERE clause
        # ══════════════════════════════════════════════════════════════
        if mode in ("replay", "backfill"):
            if not start_date_override:
                raise ValueError(
                    f"mode='{mode}' requires start_date_override to be provided."
                )
            start_date = pd.Timestamp(start_date_override).normalize()
            start_str  = start_date.strftime("%Y-%m-%d")
            end_str    = end_date.strftime("%Y-%m-%d")
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
            print(
                f"  [INCREMENTAL] Loading rows with date > {start_str}"
            )

        # ══════════════════════════════════════════════════════════════
        # STEP 2 — Load source tables from Snowflake
        # Each table uses the same WHERE clause so the date window is
        # consistent across all asset classes.
        # ══════════════════════════════════════════════════════════════
        print("\n  [STEP 2] Loading source tables from Snowflake...")

        fx  = load_source_table("FX",        where_clause, run_id)
        cmd = load_source_table("COMMODITY",  where_clause, run_id)
        bnd = load_source_table("BONDS",      where_clause, run_id)
        eq  = load_source_table("EQUITY",     where_clause, run_id)

        print(
            f"  FX: {len(fx):,}  Commodity: {len(cmd):,}  "
            f"Bonds: {len(bnd):,}  Equity: {len(eq):,}"
        )

        # ══════════════════════════════════════════════════════════════
        # STEP 3 — Build derivative exposure frames (business logic
        # unchanged from original implementation)
        # ══════════════════════════════════════════════════════════════
        print("\n  [STEP 3] Building derivative exposure frames...")
        frames = []

        # ── FX ──────────────────────────────────────────────────────
        if not fx.empty:
            fx["date"] = parse_dates_safe(fx["date"])
            fx = fx[fx["date"].notna()].copy()
            fx = ensure_cols(
                fx, ["ticker", "sector", "industry", "fx_rate", "exposure_amount"]
            )
            frames.append(pd.DataFrame({
                "date":                fx["date"],
                "asset_class":         "FX",
                "derivative_type":     "FXFwd",
                "ticker":              fx["ticker"],
                "sector":              fx["sector"],
                "industry":            fx["industry"],
                "underlying_price":    pd.to_numeric(fx["fx_rate"], errors="coerce"),
                "notional":            pd.to_numeric(fx["exposure_amount"], errors="coerce").abs(),
                "delta":               1.0,
                "gamma":               0.0,
                "vega":                0.0,
                "initial_margin_rate": INITIAL_MARGIN["FX"],
                "collateral_type":     COLLATERAL_TYPE_MAP["FX"],
                "haircut":             HAIRCUT["Cash"],
                "asset_manager":       None,
                "commodity_sym":       None,
            }))

        # ── Commodity ───────────────────────────────────────────────
        if not cmd.empty:
            cmd["date"] = parse_dates_safe(cmd["date"])
            cmd = cmd[cmd["date"].notna()].copy()
            cmd = ensure_cols(
                cmd, ["ticker", "sector", "industry", "close",
                       "exposure_amount", "asset_manager", "commodity"]
            )
            frames.append(pd.DataFrame({
                "date":                cmd["date"],
                "asset_class":         "Commodity",
                "derivative_type":     "Futures",
                "ticker":              cmd["ticker"],
                "sector":              cmd["sector"],
                "industry":            cmd["industry"],
                "underlying_price":    pd.to_numeric(cmd["close"], errors="coerce"),
                "notional":            pd.to_numeric(cmd["exposure_amount"], errors="coerce").abs(),
                "delta":               1.0,
                "gamma":               0.0,
                "vega":                0.0,
                "initial_margin_rate": INITIAL_MARGIN["Commodity"],
                "collateral_type":     COLLATERAL_TYPE_MAP["Commodity"],
                "haircut":             HAIRCUT["Treasury"],
                "asset_manager":       cmd["asset_manager"],
                "commodity_sym":       cmd["commodity"],
            }))

        # ── Bonds ────────────────────────────────────────────────────
        if not bnd.empty:
            bnd["date"] = parse_dates_safe(bnd["date"])
            bnd = bnd[bnd["date"].notna()].copy()
            bnd = ensure_cols(
                bnd, ["ticker", "sector", "industry",
                       "yield_to_maturity", "maturity_years"]
            )
            notional = (
                pd.to_numeric(bnd["maturity_years"], errors="coerce").abs() * 1_000_000
            ).fillna(1_000_000)
            frames.append(pd.DataFrame({
                "date":                bnd["date"],
                "asset_class":         "Bond",
                "derivative_type":     "IRS",
                "ticker":              bnd["ticker"],
                "sector":              bnd["sector"],
                "industry":            bnd["industry"],
                "underlying_price":    pd.to_numeric(bnd["yield_to_maturity"], errors="coerce"),
                "notional":            notional,
                "delta":               np.nan,
                "gamma":               np.nan,
                "vega":                np.nan,
                "initial_margin_rate": INITIAL_MARGIN["Bond"],
                "collateral_type":     COLLATERAL_TYPE_MAP["Bond"],
                "haircut":             HAIRCUT["Treasury"],
                "asset_manager":       None,
                "commodity_sym":       None,
            }))

        # ── Equity ───────────────────────────────────────────────────
        if not eq.empty:
            eq["date"] = parse_dates_safe(eq["date"])
            eq = eq[eq["date"].notna()].copy()
            eq = ensure_cols(
                eq, ["ticker", "sector", "industry",
                      "close", "mtm_value", "asset_manager"]
            )
            frames.append(pd.DataFrame({
                "date":                eq["date"],
                "asset_class":         "Equity",
                "derivative_type":     "EqFwd",
                "ticker":              eq["ticker"],
                "sector":              eq["sector"],
                "industry":            eq["industry"],
                "underlying_price":    pd.to_numeric(eq["close"], errors="coerce"),
                "notional":            pd.to_numeric(eq["mtm_value"], errors="coerce").abs(),
                "delta":               1.0,
                "gamma":               0.0,
                "vega":                0.0,
                "initial_margin_rate": INITIAL_MARGIN["Equity"],
                "collateral_type":     COLLATERAL_TYPE_MAP["Equity"],
                "haircut":             HAIRCUT["Equity"],
                "asset_manager":       eq["asset_manager"],
                "commodity_sym":       None,
            }))

        if not frames:
            print("  No new rows to process — pipeline complete.")
            return "NO_NEW_ROWS"

        df = pd.concat(frames, ignore_index=True)
        print(f"  Combined rows: {len(df):,}")

        # ══════════════════════════════════════════════════════════════
        # STEP 4 — Deterministic identity assignments
        #
        # All assignments use SHA-256 hash of the trade_id so that
        # replay runs produce exactly the same rows as the original run.
        # ══════════════════════════════════════════════════════════════
        print("\n  [STEP 4] Computing deterministic trade_id, counterparty, agreement...")

        df["trade_id"] = df.apply(generate_trade_id, axis=1)

        df["counterparty"] = df["trade_id"].apply(det_counterparty)

        agreements            = df["trade_id"].apply(det_agreement)
        df["agreement_type"]  = agreements.apply(lambda x: x[0])
        df["jurisdiction"]    = agreements.apply(lambda x: x[1])

        df["tenor_years"] = df["trade_id"].apply(det_tenor)

        # ══════════════════════════════════════════════════════════════
        # STEP 5 — Collateral metrics (unchanged business logic)
        # ══════════════════════════════════════════════════════════════
        print("\n  [STEP 5] Computing collateral metrics...")

        df["exposure_before_collateral"] = df["notional"].abs()
        df["required_collateral"]  = (
            df["exposure_before_collateral"] * df["initial_margin_rate"]
        )
        df["collateral_value"]     = df["required_collateral"] * 0.9
        df["effective_collateral"] = df["collateral_value"] * (1 - df["haircut"])
        df["net_exposure"]         = (
            df["exposure_before_collateral"] - df["effective_collateral"]
        ).clip(lower=0)
        df["collateral_ratio"] = np.where(
            df["exposure_before_collateral"] > 0,
            df["effective_collateral"] / df["exposure_before_collateral"],
            0,
        )
        df["margin_call_flag"]   = (df["net_exposure"] > 0).astype(int)
        df["margin_call_amount"] = df["net_exposure"]

        # ══════════════════════════════════════════════════════════════
        # STEP 6 — Greeks (unchanged business logic)
        # ══════════════════════════════════════════════════════════════
        print("\n  [STEP 6] Handling Greeks...")

        mask_all = df[["delta", "gamma", "vega"]].isna().all(axis=1)
        df.loc[mask_all, ["delta", "gamma", "vega"]] = [1.0, 0.0, 0.0]
        for c in ["delta", "gamma", "vega"]:
            df[c] = df[c].fillna(df[c].median(skipna=True)).clip(lower=0)
        df["delta_equivalent_exposure"] = df["delta"] * df["notional"]

        # ══════════════════════════════════════════════════════════════
        # STEP 7 — Maturity dates (deterministic, keyed on trade_id)
        # Replaces the former np.random.choice over TENOR_OPTIONS.
        # ══════════════════════════════════════════════════════════════
        print("\n  [STEP 7] Computing maturity dates...")

        df["date"] = pd.to_datetime(df["date"])
        df["maturity_date"] = df.apply(
            lambda r: r["date"] + timedelta(days=round(r["tenor_years"] * 365)),
            axis=1,
        )

        # ══════════════════════════════════════════════════════════════
        # STEP 8 — PnL proxy (unchanged business logic)
        # ══════════════════════════════════════════════════════════════
        print("\n  [STEP 8] Computing PnL...")

        vol_max = df.groupby("asset_class")["underlying_price"].transform("std")
        vol_max_scalar = vol_max.max()
        df["pnl"] = df["delta_equivalent_exposure"] * 0.001 * (
            vol_max / (vol_max_scalar if vol_max_scalar > 0 else 1)
        )

        # Drop internal staging columns
        df = df.drop(columns=["commodity_sym", "asset_manager"], errors="ignore")

        # ══════════════════════════════════════════════════════════════
        # STEP 9 — Macro merge (soft fail — pipeline continues without)
        # ══════════════════════════════════════════════════════════════
        print("\n  [STEP 9] Merging macro data...")

        macro = load_macro_data()
        if not macro.empty:
            df["month_year"]    = df["date"].dt.to_period("M").astype(str)
            macro["month_year"] = macro["date"].dt.to_period("M").astype(str)
            merged = df.merge(
                macro.drop(columns=["date"]), on="month_year", how="inner"
            ).drop(columns=["month_year"])

            if merged.empty:
                print("  [WARNING] No rows after macro merge — continuing without macro")
                merged = df.drop(columns=["month_year"], errors="ignore").copy()
                for col in ["gdp", "unrate", "cpi", "fedfunds"]:
                    if col not in merged.columns:
                        merged[col] = np.nan
        else:
            merged = df.copy()
            for col in ["gdp", "unrate", "cpi", "fedfunds"]:
                if col not in merged.columns:
                    merged[col] = np.nan

        print(f"  Rows after macro merge: {len(merged):,}")

        # Convert dates to date objects (required by Snowflake / Postgres)
        merged["date"]         = pd.to_datetime(merged["date"]).dt.date
        merged["maturity_date"] = pd.to_datetime(merged["maturity_date"]).dt.date

        # ══════════════════════════════════════════════════════════════
        # STEP 10 — Pipeline metadata
        # ══════════════════════════════════════════════════════════════
        print("\n  [STEP 10] Adding pipeline metadata...")

        merged = add_pipeline_metadata(merged, run_id)
        snowflake_rows = len(merged)
        print(f"  Final rows: {snowflake_rows:,}")

        if merged.empty:
            print("  No valid rows after processing — pipeline complete.")
            return "NO_VALID_ROWS"

        # ══════════════════════════════════════════════════════════════
        # STEP 11 — Write to Snowflake HISTORY (always INSERT)
        # Failure here -> pipeline FAILS.
        # ══════════════════════════════════════════════════════════════
        print(
            f"\n  [STEP 11] Writing to Snowflake HISTORY "
            f"({SNOWFLAKE_HISTORY_TABLE})..."
        )
        write_to_snowflake_history(merged, run_mode=mode, run_id=run_id)

        # ══════════════════════════════════════════════════════════════
        # STEP 12 — Write to Snowflake CLEAN (deterministic latest state)
        # replay/backfill -> DELETE window + INSERT (transactional)
        # incremental     -> INSERT only (watermark-safe)
        # Failure here -> pipeline FAILS.
        # ══════════════════════════════════════════════════════════════
        print(
            f"\n  [STEP 12] Writing to Snowflake CLEAN "
            f"({SNOWFLAKE_CLEAN_TABLE}) mode='{mode}'..."
        )
        write_to_snowflake_clean(
            merged,
            mode=mode,
            start_date=start_date,
            end_date=end_date,
            run_id=run_id,
        )

        # ══════════════════════════════════════════════════════════════
        # STEP 13 — Write to Postgres serving layer
        # Failure here -> pipeline FAILS (consistency-first).
        # ══════════════════════════════════════════════════════════════
        print(
            f"\n  [STEP 13] Writing to Postgres "
            f"({POSTGRES_TABLE}) mode='{mode}'..."
        )
        write_to_postgres(
            merged,
            mode=mode,
            start_date=start_date,
            end_date=end_date,
        )

        # ══════════════════════════════════════════════════════════════
        # STEP 14 — Pipeline success
        # ══════════════════════════════════════════════════════════════
        processing_time = round(time.time() - pipeline_start, 2)

        print(f"\n{'='*66}")
        print(f"  DERIVATIVES PIPELINE SUCCESS")
        print(f"  run_id           : {run_id}")
        print(f"  mode             : {mode}")
        print(f"  rows processed   : {snowflake_rows:,}")
        print(f"  window           : {start_str} -> {end_str}")
        print(f"  duration         : {processing_time}s")
        print(f"  Snowflake HISTORY: OK")
        print(f"  Snowflake CLEAN  : OK")
        print(f"  Postgres         : OK")
        print(f"{'='*66}\n")

        return f"SUCCESS_{snowflake_rows}"

    except Exception as e:
        processing_time = round(time.time() - pipeline_start, 2)
        send_critical_alert(
            "Derivatives pipeline failed with unhandled exception",
            context={
                "run_id":   run_id,
                "mode":     mode,
                "duration": f"{processing_time}s",
                "error":    str(e),
            },
        )
        print(f"\n{'='*66}")
        print(f"  DERIVATIVES PIPELINE FAILED")
        print(f"  run_id   : {run_id}")
        print(f"  mode     : {mode}")
        print(f"  error    : {e}")
        print(f"  duration : {processing_time}s")
        print(f"{'='*66}\n")
        raise