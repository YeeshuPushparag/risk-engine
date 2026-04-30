import os
import time
import hashlib
from io import BytesIO, StringIO
import psycopg
import numpy as np
import pandas as pd
from dateutil import parser
from datetime import datetime, timedelta
import boto3
import requests
from snowflake.connector.pandas_tools import write_pandas

# 🔁 PROVIDED CONNECTIONS (ALREADY DEFINED ELSEWHERE)
from connections.snowflake_conn import get_snowflake_conn
from connections.postgre_conn import get_postgre_conn


# ============================================================
# PRODUCTION-GRADE ALERTING (CRITICAL + SUCCESS)
# ============================================================
def send_critical_alert(message: str, context: dict = None):
    """
    Send CRITICAL alert to Slack for unrecoverable pipeline failures.
    
    Triggers only for:
    - Snowflake table load failures (after retries)
    - Snowflake MERGE failures (system of record)
    - Complete pipeline failures
    """
    payload = {
        "level": "CRITICAL",
        "pipeline": "collateral_pipeline",
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
            text = f"*[CRITICAL]* collateral_pipeline\n" \
                   f"{truncated_msg}\n" \
                   f"run_id: {run_id}"
            requests.post(webhook, json={"text": text}, timeout=3)
        except Exception as e:
            print(f"[ALERT ERROR] Slack notification failed: {e}")


def send_success_alert(context: dict = None):
    """
    Send SUCCESS alert to Slack when pipeline completes successfully.
    This is a COMPLETORY alert - DAG ran successfully.
    """
    payload = {
        "level": "SUCCESS",
        "pipeline": "collateral_pipeline",
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
            detail_rows = context.get('snowflake_detail_rows', 0)
            model_rows = context.get('snowflake_model_rows', 0)
            duration = context.get('duration_seconds', 0)
            
            text = f"*[SUCCESS]* collateral_pipeline\n" \
                   f"✅ DAG ran successfully\n" \
                   f"run_id: {run_id}\n" \
                   f"detail rows: {detail_rows:,}\n" \
                   f"model rows: {model_rows:,}\n" \
                   f"duration: {duration}s"
            requests.post(webhook, json={"text": text}, timeout=3)
        except Exception as e:
            print(f"[ALERT ERROR] Slack notification failed: {e}")


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
# SNOWFLAKE IDEMPOTENT WRITE (MERGE - NO DUPLICATES)
# ============================================================
def write_to_snowflake_idempotent(df, table_name, key_columns, run_id=None, chunk_size=100000):
    """
    Write to Snowflake with MERGE logic to prevent duplicates.
    Sends CRITICAL alert on failure.
    """
    if df.empty:
        return 0
    
    temp_table = f"{table_name}_TEMP_{int(time.time())}"
    
    try:
        with get_snowflake_conn() as ctx:
            with ctx.cursor() as cs:
                # Step 1: Create temp table with same structure
                cs.execute(f"""
                    CREATE TEMPORARY TABLE {temp_table} 
                    LIKE {table_name}
                """)
                
                # Step 2: Write to temp table
                write_pandas(ctx, df, temp_table, chunk_size=chunk_size, quote_identifiers=True)
                
                # Step 3: Get column list (exclude ID column if exists)
                cs.execute(f"""
                    SELECT COLUMN_NAME 
                    FROM INFORMATION_SCHEMA.COLUMNS 
                    WHERE TABLE_NAME = '{table_name}'
                    AND COLUMN_NAME != 'ID'
                """)
                columns = [row[0] for row in cs.fetchall()]
                col_list = ', '.join([f'"{c}"' for c in columns])
                
                # Step 4: Build MERGE statement
                merge_condition = ' AND '.join([f'target."{col}" = source."{col}"' for col in key_columns])
                update_set = ', '.join([f'target."{col}" = source."{col}"' for col in columns])
                insert_cols = ', '.join([f'source."{col}"' for col in columns])
                
                merge_sql = f"""
                    MERGE INTO {table_name} AS target
                    USING {temp_table} AS source
                    ON {merge_condition}
                    WHEN MATCHED THEN UPDATE SET {update_set}
                    WHEN NOT MATCHED THEN INSERT ({col_list}) VALUES ({insert_cols})
                """
                
                # Step 5: Execute MERGE
                cs.execute(merge_sql)
                
                # Step 6: Get affected rows count
                cs.execute("SELECT ROW_COUNT()")
                affected_rows = cs.fetchone()[0]
                
                print(f"  Snowflake MERGE {table_name}: {affected_rows} rows affected")
                return affected_rows
                
    except Exception as exc:
        send_critical_alert(
            f"Snowflake MERGE failed for table {table_name}",
            context={"run_id": run_id, "table": table_name, "rows": len(df), "error": str(exc)}
        )
        raise


# ============================================================
# SNOWFLAKE TABLE LOAD WITH RETRY (CRITICAL)
# ============================================================
def load_latest_with_retry(table, run_id=None):
    """Load latest data from Snowflake table with retry. Sends CRITICAL alert if fails."""
    def _load():
        query = f'''
            SELECT *
            FROM "{table}"
            WHERE "date" > (SELECT COALESCE(MAX("date"), '1900-01-01') FROM "COLLATERAL")
        '''
        with get_snowflake_conn() as ctx:
            with ctx.cursor() as cs:
                cs.execute(query)
                return cs.fetch_pandas_all()
    
    return retry_with_backoff(_load, retries=2,
                               critical_name=f"Snowflake table load: {table}",
                               run_id=run_id)


# ============================================================
# POSTGRES WRITE WITH RETRY (NO ALERTS - NON-CRITICAL)
# ============================================================
def write_to_postgres_with_retry(df, table_name, retries=3):
    """
    Write to Postgres with retry logic.
    Returns success flag and error message if any.
    NO ALERTS - Postgres is serving layer, Snowflake is source of truth.
    """
    if df.empty:
        return True, "Empty DataFrame - nothing to write"
    
    BATCH_SIZE = 100_000
    last_error = None
    
    for attempt in range(retries + 1):
        try:
            with get_postgre_conn() as pg_conn:
                with pg_conn.cursor() as cur:
                    # Fetch schema order
                    cur.execute(f"""
                        SELECT column_name
                        FROM information_schema.columns
                        WHERE table_schema='public'
                        AND table_name='{table_name}'
                        ORDER BY ordinal_position
                    """)
                    
                    cols = [r[0] for r in cur.fetchall() if r[0] != "id"]
                    
                    if not cols:
                        raise RuntimeError(f"No columns found for {table_name}")
                    
                    # Validate schema
                    missing = set(cols) - set(df.columns)
                    if missing:
                        raise ValueError(f"[POSTGRES ERROR] Missing columns for {table_name}: {missing}")
                    
                    # Enforce schema order
                    df_pg = df[cols].copy()
                    
                    quoted_cols = [f'"{c}"' for c in cols]
                    copy_sql = f"COPY public.{table_name} ({','.join(quoted_cols)}) FROM STDIN WITH CSV"
                    
                    # Batch COPY
                    for start in range(0, len(df_pg), BATCH_SIZE):
                        chunk = df_pg.iloc[start:start + BATCH_SIZE]
                        buf = StringIO()
                        chunk.to_csv(buf, index=False, header=False)
                        buf.seek(0)
                        
                        with cur.copy(copy_sql) as copy:
                            copy.write(buf.getvalue())
                    
                pg_conn.commit()
            
            print(f"[POSTGRES {table_name}] Uploaded {len(df)} rows successfully")
            return True, None
            
        except Exception as e:
            last_error = e
            if attempt < retries:
                wait_time = 2 ** attempt
                print(f"  Postgres retry {attempt + 1}/{retries} after {wait_time}s: {e}")
                time.sleep(wait_time)
            else:
                print(f"  Postgres {table_name} failed after {retries} retries: {e}")
    
    return False, str(last_error)


# ============================================================
# S3 LOAD WITH RETRY (SOFT FAIL - NO ALERTS)
# ============================================================
def load_csv_from_s3_with_retry(bucket, key, retries=3):
    """Load CSV from S3 with retry logic. NO ALERT - macro is non-critical."""
    def _load():
        obj = s3.get_object(Bucket=bucket, Key=key)
        df = pd.read_csv(BytesIO(obj["Body"].read()))
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        return df[df["date"].notna()]
    
    return retry_with_backoff(_load, retries=retries)


# ============================================================
# CLIENTS
# ============================================================
MACRO_BUCKET = "pushparag-equity-bucket"
MACRO_KEY = "historical-equity/macro_data.csv"

s3 = boto3.client("s3")


# ============================================================
# RISK CONFIGS
# ============================================================
HAIRCUT = {
    "Cash": 0.00, "Treasury": 0.02, "CorporateBond": 0.10,
    "Equity": 0.15, "CommodityETF": 0.12
}
INITIAL_MARGIN = {
    "FX": 0.05, "Commodity": 0.08, "Bond": 0.04, "Equity": 0.12
}
COLLATERAL_MENU = {
    "FX": [("Cash", 0.70), ("Treasury", 0.25), ("CorporateBond", 0.05)],
    "Commodity": [("Treasury", 0.60), ("Cash", 0.25), ("CommodityETF", 0.15)],
    "Bond": [("Treasury", 0.80), ("Cash", 0.15), ("CorporateBond", 0.05)],
    "Equity": [("Cash", 0.50), ("Treasury", 0.30), ("Equity", 0.20)],
}
AGREEMENT_CHOICES = [
    ("CSA", "US", 0.45), ("CSA", "UK", 0.25),
    ("CSA", "EU", 0.20), ("GMRA", "UK", 0.05),
    ("GMSLA", "US", 0.05),
]


# ============================================================
# UTIL FUNCTIONS
# ============================================================
def load_macro_with_retry():
    """Load macro data from S3 with retry. NO ALERT - non-critical."""
    try:
        return load_csv_from_s3_with_retry(MACRO_BUCKET, MACRO_KEY, retries=2)
    except Exception as e:
        print(f"  WARNING: Macro data load failed: {e} - continuing without macro")
        return pd.DataFrame()


def fast_counterparty(series):
    """Generate deterministic counterparty names based on ticker."""
    arr = np.array(["CP_A", "CP_B", "CP_C", "CP_D", "CP_E"])
    idx = series.astype(str).apply(
        lambda x: int(hashlib.sha256(x.encode()).hexdigest(), 16) % 5
    )
    return arr[idx]


def ensure_cols(df, cols):
    """Ensure DataFrame has required columns."""
    for c in cols:
        if c not in df.columns:
            df[c] = np.nan
    return df


def generate_trade_id(row):
    """Generate unique trade ID for collateral tracking."""
    parts = [row["collateral_type"], row["ticker"], row["date"].strftime("%Y%m%d")]

    if pd.notna(row.get("commodity_sym")):
        parts.append(str(row["commodity_sym"]).replace("=", "_"))

    if pd.notna(row.get("asset_manager")):
        parts.append(str(row["asset_manager"]).split()[0])

    return "_".join(parts)


def build_detail(df, asset_class, sym, exp):
    """Build detailed collateral exposure records."""
    if df.empty:
        return pd.DataFrame()

    required_cols = ["date", sym, exp, "sector", "industry", "asset_manager"]
    if asset_class == "Commodity":
        required_cols.append("commodity")

    df = ensure_cols(df, required_cols)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df[df["date"].notna()]

    out = pd.DataFrame()
    out["date"] = df["date"]
    out["asset_class"] = asset_class
    out["ticker"] = df[sym].astype(str)
    out["sector"] = df.get("sector", "Unknown")
    out["industry"] = df.get("industry", "Unknown")
    out["counterparty"] = fast_counterparty(df[sym])
    out["exposure_before_collateral"] = df[exp].abs().astype(float)

    out["asset_manager"] = df["asset_manager"].fillna(np.nan)
    out["commodity_sym"] = df["commodity"] if asset_class == "Commodity" else np.nan

    collateral_map = {
        "FX": "FXFwd",
        "Commodity": "Futures",
        "Bond": "IRS",
        "Equity": "EqFwd"
    }
    out["collateral_type"] = collateral_map[asset_class]
    out["trade_id"] = out.apply(generate_trade_id, axis=1)

    ac_pairs = [(a, j) for (a, j, p) in AGREEMENT_CHOICES]
    probs = [p for (*_, p) in AGREEMENT_CHOICES]
    idx = np.random.choice(len(ac_pairs), len(out), p=np.array(probs) / sum(probs))
    a, j = zip(*[ac_pairs[i] for i in idx])
    out["agreement_type"], out["jurisdiction"] = a, j

    collateral_types, collateral_probs = zip(*COLLATERAL_MENU[asset_class])
    out["collateral_type"] = np.random.choice(
        collateral_types, len(out),
        p=np.array(collateral_probs) / sum(collateral_probs)
    )

    out["haircut"] = out["collateral_type"].map(HAIRCUT)
    out["initial_margin_rate"] = INITIAL_MARGIN[asset_class]

    out["required_collateral"] = out["exposure_before_collateral"] * out["initial_margin_rate"]
    out["collateral_value"] = out["required_collateral"] * 0.9
    out["effective_collateral"] = out["collateral_value"] * (1 - out["haircut"])
    out["net_exposure"] = (out["exposure_before_collateral"] - out["effective_collateral"]).clip(0)
    out["collateral_ratio"] = (
        out["effective_collateral"] / out["exposure_before_collateral"]
    ).fillna(0)

    out["margin_call_flag"] = (out["net_exposure"] > 0).astype(int)
    out["margin_call_amount"] = out["net_exposure"]
    out["reuse_flag"] = np.random.binomial(1, 0.35, len(out))
    out["reused_value"] = out["collateral_value"] * out["reuse_flag"] * 0.8
    out["funding_cost"] = out["collateral_value"] * 0.01 * (
        out["haircut"] + out["initial_margin_rate"]
    )
    out["liquidity_score"] = 1 - (out["haircut"] + out["initial_margin_rate"])

    out["collateral_id"] = out["trade_id"].apply(
        lambda x: hashlib.md5(x.encode()).hexdigest()
    )

    return out.drop(columns=["commodity_sym", "asset_manager", "collateral_type"], errors="ignore")


def drop_metadata_for_serving(df):
    """Remove metadata columns for serving layer."""
    drop_cols = [
        "pipeline_name",
        "pipeline_run_id",
        "data_source",
        "input_source",
        "transformation",
        "record_created_at",
    ]
    return df.drop(columns=[c for c in drop_cols if c in df.columns])


def add_pipeline_metadata(df, run_id):
    """Add pipeline metadata to DataFrame."""
    df["pipeline_name"] = "collateral_pipeline"
    df["pipeline_run_id"] = run_id
    df["data_source"] = "snowflake"
    df["input_source"] = "fx + equity + commodity + bonds"
    df["transformation"] = "collateral_v1"
    df["record_created_at"] = datetime.utcnow()
    return df


# ============================================================
# MAIN AIRFLOW ENTRYPOINT (WITH CRITICAL + SUCCESS ALERTS)
# ============================================================
def run_collateral_pipeline():
    """
    Complete production-grade collateral pipeline with:
    1. Retry on all external calls (Snowflake, S3, Postgres)
    2. Idempotent Snowflake writes (MERGE, not append) - NO DUPLICATES
    3. Partial success handling (Snowflake success doesn't require Postgres success)
    4. Slack alerts for CRITICAL failures AND SUCCESS completion
    5. Run-level tracking with run_id
    """
    pipeline_start = time.time()
    run_id = datetime.utcnow().isoformat()
    np.random.seed(42)
    
    snowflake_detail_rows = 0
    snowflake_model_rows = 0
    postgres_detail_success = False
    postgres_model_success = False
    
    print(f"\n{'='*66}")
    print(f"  COLLATERAL PIPELINE START - run_id={run_id}")
    print(f"{'='*66}\n")
    
    try:
        # ============================================================
        # STEP 1: Load all source tables from Snowflake (CRITICAL)
        # ============================================================
        print("[STEP 1] Loading source tables from Snowflake...")
        
        fx_df = load_latest_with_retry("FX", run_id=run_id)
        print(f"  FX loaded: {len(fx_df)} rows")
        
        eq_df = load_latest_with_retry("EQUITY", run_id=run_id)
        print(f"  EQUITY loaded: {len(eq_df)} rows")
        
        cm_df = load_latest_with_retry("COMMODITY", run_id=run_id)
        print(f"  COMMODITY loaded: {len(cm_df)} rows")
        
        bd_df = load_latest_with_retry("BONDS", run_id=run_id)
        print(f"  BONDS loaded: {len(bd_df)} rows")
        
        # ============================================================
        # STEP 2: Build detail exposures for each asset class
        # ============================================================
        print("[STEP 2] Building detail exposures...")
        
        fx = build_detail(fx_df, "FX", "ticker", "exposure_amount")
        eq = build_detail(eq_df, "Equity", "ticker", "mtm_value")
        cm = build_detail(cm_df, "Commodity", "ticker", "exposure_amount")
        bd = build_detail(bd_df, "Bond", "ticker", "bond_price")
        
        df_detail = pd.concat([fx, eq, cm, bd], ignore_index=True)
        
        if df_detail.empty:
            print("  No new rows to process")
            return "NO_NEW_ROWS"
        
        print(f"  Detail rows: {len(df_detail)}")
        
        # ============================================================
        # STEP 3: Aggregate to model level
        # ============================================================
        print("[STEP 3] Aggregating to model level...")
        
        df_model = df_detail.groupby(
            ["date", "asset_class", "ticker", "counterparty"], as_index=False
        ).agg({
            "exposure_before_collateral": "sum",
            "required_collateral": "sum",
            "collateral_value": "sum",
            "effective_collateral": "sum",
            "net_exposure": "sum",
            "margin_call_amount": "sum",
            "funding_cost": "sum",
            "liquidity_score": "mean",
        })
        
        df_model["collateral_ratio"] = (
            df_model["effective_collateral"] / df_model["exposure_before_collateral"]
        ).fillna(0)
        df_model["margin_call_flag"] = (df_model["net_exposure"] > 0).astype(int)
        
        print(f"  Model rows: {len(df_model)}")
        
        # ============================================================
        # STEP 4: Merge macro data (SOFT FAIL - NO ALERT)
        # ============================================================
        print("[STEP 4] Merging macro data...")
        
        macro = load_macro_with_retry()
        
        if not macro.empty:
            df_detail["month_year"] = df_detail["date"].dt.to_period("M").astype(str)
            macro["month_year"] = macro["date"].dt.to_period("M").astype(str)
            
            df_detail = df_detail.merge(
                macro.drop(columns=["date"]),
                on="month_year",
                how="inner"
            ).drop(columns=["month_year"])
        else:
            print("  WARNING: Macro data not available - continuing without macro")
            for col in ["gdp", "unrate", "cpi", "fedfunds"]:
                if col not in df_detail.columns:
                    df_detail[col] = np.nan
        
        # Convert dates to date objects for Snowflake
        df_detail["date"] = pd.to_datetime(df_detail["date"]).dt.date
        df_model["date"] = pd.to_datetime(df_model["date"]).dt.date
        
        print(f"  After macro merge - detail: {len(df_detail)} rows")
        
        # ============================================================
        # STEP 5: Add pipeline metadata
        # ============================================================
        print("[STEP 5] Adding pipeline metadata...")
        
        df_detail = add_pipeline_metadata(df_detail, run_id)
        df_model = add_pipeline_metadata(df_model, run_id)
        
        snowflake_detail_rows = len(df_detail)
        snowflake_model_rows = len(df_model)
        
        # ============================================================
        # STEP 6: Write to Snowflake (IDEMPOTENT - MERGE) - CRITICAL
        # ============================================================
        print("[STEP 6] Writing to Snowflake (idempotent MERGE)...")
        
        def write_collateral_detail():
            return write_to_snowflake_idempotent(
                df_detail,
                "COLLATERAL",
                key_columns=["trade_id", "date", "ticker"],
                run_id=run_id,
                chunk_size=100000
            )
        
        def write_collateral_model():
            return write_to_snowflake_idempotent(
                df_model,
                "COLLATERAL_MODEL",
                key_columns=["date", "asset_class", "ticker", "counterparty"],
                run_id=run_id,
                chunk_size=100000
            )
        
        detail_affected = retry_with_backoff(write_collateral_detail, retries=3,
                                              critical_name="Snowflake MERGE write to COLLATERAL table",
                                              run_id=run_id)
        model_affected = retry_with_backoff(write_collateral_model, retries=3,
                                             critical_name="Snowflake MERGE write to COLLATERAL_MODEL table",
                                             run_id=run_id)
        
        print(f"  COLLATERAL detail: {detail_affected} rows affected")
        print(f"  COLLATERAL_MODEL: {model_affected} rows affected")
        
        # ============================================================
        # STEP 7: Write to Postgres (NO ALERTS - non-critical)
        # ============================================================
        print("[STEP 7] Writing to Postgres...")
        
        # Postgres Detail
        df_pg_detail = drop_metadata_for_serving(df_detail.copy())
        postgres_detail_ok, postgres_detail_error = write_to_postgres_with_retry(
            df_pg_detail, "collateral_data", retries=3
        )
        postgres_detail_success = postgres_detail_ok
        
        if not postgres_detail_ok:
            print(f"  WARNING: Postgres collateral_data write failed: {postgres_detail_error}")
        
        # Postgres Model
        df_pg_model = drop_metadata_for_serving(df_model.copy())
        postgres_model_ok, postgres_model_error = write_to_postgres_with_retry(
            df_pg_model, "collateral_model_data", retries=3
        )
        postgres_model_success = postgres_model_ok
        
        if not postgres_model_ok:
            print(f"  WARNING: Postgres collateral_model_data write failed: {postgres_model_error}")
        
        # ============================================================
        # STEP 8: Success - Send COMPLETORY Slack Alert
        # ============================================================
        processing_time = round(time.time() - pipeline_start, 2)
        
        # Send SUCCESS alert ONLY if everything (including Postgres) succeeded
        if postgres_detail_success and postgres_model_success:
            send_success_alert({
                "run_id": run_id,
                "snowflake_detail_rows": snowflake_detail_rows,
                "snowflake_model_rows": snowflake_model_rows,
                "postgres_detail_success": postgres_detail_success,
                "postgres_model_success": postgres_model_success,
                "duration_seconds": processing_time
            })
        else:
            print("  SUCCESS (Snowflake) but Postgres failed -> skipping success alert")
        
        print(f"\n{'='*66}")
        print(f"  COLLATERAL PIPELINE SUCCESS - run_id={run_id}")
        print(f"  Snowflake detail rows: {snowflake_detail_rows}")
        print(f"  Snowflake model rows: {snowflake_model_rows}")
        print(f"  Postgres detail: {'SUCCESS' if postgres_detail_success else 'FAILED'}")
        print(f"  Postgres model: {'SUCCESS' if postgres_model_success else 'FAILED'}")
        print(f"  Duration: {processing_time}s")
        print(f"{'='*66}\n")
        
        return "SUCCESS"
    
    except Exception as e:
        # Send CRITICAL alert for any unhandled exception
        send_critical_alert(
            f"Pipeline failed with unhandled exception",
            context={"run_id": run_id, "error": str(e)}
        )
        
        processing_time = round(time.time() - pipeline_start, 2)
        print(f"\n{'='*66}")
        print(f"  COLLATERAL PIPELINE FAILED - run_id={run_id}")
        print(f"  Error: {str(e)}")
        print(f"  Duration: {processing_time}s")
        print(f"{'='*66}\n")
        
        # Re-raise for Airflow to catch
        raise