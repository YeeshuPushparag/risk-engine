import os
import time
import hashlib
from io import BytesIO, StringIO
import numpy as np
import pandas as pd
from dateutil import parser
import boto3
import requests
from datetime import datetime, timedelta
from connections.snowflake_conn import get_snowflake_conn
from connections.postgre_conn import get_postgre_conn
from snowflake.connector.pandas_tools import write_pandas


# ============================================================
# PRODUCTION-GRADE ALERTING (CRITICAL ONLY)
# ============================================================
def send_critical_alert(message: str, context: dict = None):
    """
    Send CRITICAL alert to Slack only for unrecoverable pipeline failures.
    
    Triggers only for:
    - Snowflake MERGE failures (system of record)
    - Snowflake table load failures (after retries)
    - Complete pipeline failures
    
    Does NOT send for:
    - Retry attempts (only final failure)
    - Postgres failures (non-critical)
    - Missing macro data (continues anyway)
    """
    payload = {
        "level": "CRITICAL",
        "pipeline": "derivatives_pipeline",
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
            text = f"*[CRITICAL]* derivatives_pipeline\n" \
                   f"{truncated_msg}\n" \
                   f"run_id: {run_id}"
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
def load_table_with_retry(table, run_id=None, retries=2):
    """
    Load table from Snowflake with retry.
    Sends CRITICAL alert if fails after retries.
    """
    def _load():
        q = f'''
            SELECT *
            FROM "{table}"
            WHERE "date" > (SELECT COALESCE(MAX("date"), '1900-01-01') FROM "DERIVATIVES")
        '''
        with get_snowflake_conn() as ctx:
            with ctx.cursor() as cs:
                cs.execute(q)
                return cs.fetch_pandas_all()
    
    return retry_with_backoff(_load, retries=retries,
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
        macro = pd.read_csv(BytesIO(obj["Body"].read()))
        macro["date"] = pd.to_datetime(macro["date"], errors="coerce")
        return macro[macro["date"].notna()]
    
    return retry_with_backoff(_load, retries=retries)


# ============================================================
# CONSTANTS
# ============================================================
MACRO_BUCKET = "pushparag-equity-bucket"
MACRO_KEY = "historical-equity/macro_data.csv"

s3 = boto3.client("s3")

INITIAL_MARGIN = {"FX": 0.05, "Commodity": 0.08, "Bond": 0.04, "Equity": 0.10}
HAIRCUT = {"Cash": 0.00, "Treasury": 0.02, "CorporateBond": 0.10, "Equity": 0.15}
COLLATERAL_TYPE = {"FX": "Cash", "Commodity": "Treasury", "Bond": "Treasury", "Equity": "Equity"}


# ============================================================
# UTILS
# ============================================================
def generate_trade_id(row):
    """Generate unique trade ID for derivatives tracking."""
    parts = [row["derivative_type"], row["ticker"], row["date"].strftime("%Y%m%d")]

    if pd.notna(row.get("commodity_sym")):
        parts.append(str(row["commodity_sym"]).replace("=", "_"))

    if pd.notna(row.get("asset_manager")):
        parts.append(str(row["asset_manager"]).split()[0])

    return "_".join(parts)


def parse_dates_safe(series):
    """Parse dates safely with fallback."""
    d1 = pd.to_datetime(series, errors="coerce")
    if d1.notna().sum() >= len(series) * 0.8:
        return d1
    return pd.to_datetime(series, errors="coerce", dayfirst=True)


def fast_counterparty(series):
    """Generate deterministic counterparty names based on ticker."""
    pool = np.array(["CP_A", "CP_B", "CP_C", "CP_D", "CP_E"])
    return series.astype(str).apply(
        lambda s: pool[int(hashlib.sha256(s.encode()).hexdigest(), 16) % len(pool)]
    )


def ensure_cols(df, cols):
    """Ensure DataFrame has required columns."""
    for c in cols:
        if c not in df.columns:
            df[c] = np.nan
    return df


def load_macro_with_retry():
    """Load macro data from S3 with retry. NO ALERT - non-critical."""
    try:
        return load_csv_from_s3_with_retry(MACRO_BUCKET, MACRO_KEY, retries=2)
    except Exception as e:
        print(f"  WARNING: Macro data load failed: {e} - continuing without macro")
        return pd.DataFrame()


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
    df["pipeline_name"] = "derivatives_pipeline"
    df["pipeline_run_id"] = run_id
    df["data_source"] = "snowflake"
    df["input_source"] = "fx + commodity + bonds + equity"
    df["transformation"] = "derivatives_v1"
    df["record_created_at"] = datetime.utcnow()
    return df


def upload_derivatives_to_postgres(merged: pd.DataFrame) -> tuple[bool, str]:
    """
    Upload derivatives data to PostgreSQL (serving layer).
    NO ALERTS - Postgres is non-critical serving layer.
    
    Returns:
        (success_flag, message)
    """
    if merged.empty:
        return True, "Empty DataFrame - nothing to write"

    try:
        # STEP 1: REMOVE METADATA
        df_pg = drop_metadata_for_serving(merged.copy())

        # STEP 2: TYPE HANDLING
        if "margin_call_flag" in df_pg.columns:
            df_pg["margin_call_flag"] = df_pg["margin_call_flag"].astype(bool)

        for col in df_pg.columns:
            if col not in [
                "trade_id", "counterparty", "asset_class", "derivative_type",
                "ticker", "sector", "industry", "collateral_type",
                "date", "maturity_date", "margin_call_flag"
            ]:
                df_pg[col] = pd.to_numeric(df_pg[col], errors="coerce")

        # Use the generic retry function
        return write_to_postgres_with_retry(df_pg, "derivative_data", retries=3)

    except Exception as e:
        error_msg = f"Derivatives Postgres upload failed: {e}"
        print(f"[POSTGRES][WARN] {error_msg} (Snowflake preserved)")
        return False, error_msg


# ============================================================
# MAIN AIRFLOW TASK (WITH CRITICAL ALERTS ONLY)
# ============================================================
def run_derivatives_processing():
    """
    Complete production-grade derivatives pipeline with:
    1. Retry on all external calls (Snowflake, S3, Postgres)
    2. Idempotent Snowflake writes (MERGE, not append) - NO DUPLICATES
    3. Partial success handling (Snowflake success doesn't require Postgres success)
    4. Slack alerts ONLY for critical failures (Snowflake, pipeline crash)
    5. Run-level tracking with run_id
    6. Fixed query bug (COALESCE for empty DERIVATIVES table)
    """
    pipeline_start = time.time()
    run_id = datetime.utcnow().isoformat()
    np.random.seed(42)
    
    snowflake_rows = 0
    postgres_success = False
    
    print(f"\n{'='*66}")
    print(f"  DERIVATIVES PIPELINE START - run_id={run_id}")
    print(f"{'='*66}\n")
    
    frames = []
    
    try:
        # ============================================================
        # STEP 1: Load FX data (CRITICAL)
        # ============================================================
        print("[STEP 1] Loading FX data from Snowflake...")
        
        fx = load_table_with_retry("FX", run_id=run_id, retries=2)
        print(f"  FX rows: {len(fx)}")
        
        fx["date"] = parse_dates_safe(fx["date"])
        fx = fx[fx["date"].notna()]
        fx = ensure_cols(fx, ["ticker", "sector", "industry", "fx_rate", "exposure_amount"])
        
        frames.append(pd.DataFrame({
            "date": fx["date"],
            "asset_class": "FX",
            "derivative_type": "FXFwd",
            "ticker": fx["ticker"],
            "sector": fx["sector"],
            "industry": fx["industry"],
            "underlying_price": fx["fx_rate"],
            "notional": fx["exposure_amount"].abs(),
            "delta": 1.0,
            "gamma": 0.0,
            "vega": 0.0,
            "initial_margin_rate": INITIAL_MARGIN["FX"],
            "collateral_type": COLLATERAL_TYPE["FX"],
            "haircut": HAIRCUT["Cash"],
            "asset_manager": None,
            "commodity_sym": None,
        }))
        
        # ============================================================
        # STEP 2: Load Commodity data (CRITICAL)
        # ============================================================
        print("[STEP 2] Loading Commodity data from Snowflake...")
        
        cmd = load_table_with_retry("COMMODITY", run_id=run_id, retries=2)
        print(f"  Commodity rows: {len(cmd)}")
        
        cmd["date"] = parse_dates_safe(cmd["date"])
        cmd = cmd[cmd["date"].notna()]
        cmd = ensure_cols(cmd, ["ticker", "sector", "industry", "close", "exposure_amount", "asset_manager", "commodity"])
        
        frames.append(pd.DataFrame({
            "date": cmd["date"],
            "asset_class": "Commodity",
            "derivative_type": "Futures",
            "ticker": cmd["ticker"],
            "sector": cmd["sector"],
            "industry": cmd["industry"],
            "underlying_price": cmd["close"],
            "notional": cmd["exposure_amount"].abs(),
            "delta": 1.0,
            "gamma": 0.0,
            "vega": 0.0,
            "initial_margin_rate": INITIAL_MARGIN["Commodity"],
            "collateral_type": COLLATERAL_TYPE["Commodity"],
            "haircut": HAIRCUT["Treasury"],
            "asset_manager": cmd["asset_manager"],
            "commodity_sym": cmd["commodity"],
        }))
        
        # ============================================================
        # STEP 3: Load Bonds data (CRITICAL)
        # ============================================================
        print("[STEP 3] Loading Bonds data from Snowflake...")
        
        bnd = load_table_with_retry("BONDS", run_id=run_id, retries=2)
        print(f"  Bonds rows: {len(bnd)}")
        
        bnd["date"] = parse_dates_safe(bnd["date"])
        bnd = bnd[bnd["date"].notna()]
        bnd = ensure_cols(bnd, ["ticker", "sector", "industry", "yield_to_maturity", "maturity_years"])
        
        frames.append(pd.DataFrame({
            "date": bnd["date"],
            "asset_class": "Bond",
            "derivative_type": "IRS",
            "ticker": bnd["ticker"],
            "sector": bnd["sector"],
            "industry": bnd["industry"],
            "underlying_price": bnd["yield_to_maturity"],
            "notional": (bnd["maturity_years"].abs() * 1_000_000).fillna(1_000_000),
            "delta": np.nan,
            "gamma": np.nan,
            "vega": np.nan,
            "initial_margin_rate": INITIAL_MARGIN["Bond"],
            "collateral_type": COLLATERAL_TYPE["Bond"],
            "haircut": HAIRCUT["Treasury"],
            "asset_manager": None,
            "commodity_sym": None,
        }))
        
        # ============================================================
        # STEP 4: Load Equity data (CRITICAL)
        # ============================================================
        print("[STEP 4] Loading Equity data from Snowflake...")
        
        eq = load_table_with_retry("EQUITY", run_id=run_id, retries=2)
        print(f"  Equity rows: {len(eq)}")
        
        eq["date"] = parse_dates_safe(eq["date"])
        eq = eq[eq["date"].notna()]
        eq = ensure_cols(eq, ["ticker", "sector", "industry", "close", "mtm_value", "asset_manager"])
        
        frames.append(pd.DataFrame({
            "date": eq["date"],
            "asset_class": "Equity",
            "derivative_type": "EqFwd",
            "ticker": eq["ticker"],
            "sector": eq["sector"],
            "industry": eq["industry"],
            "underlying_price": eq["close"],
            "notional": eq["mtm_value"].abs(),
            "delta": 1.0,
            "gamma": 0.0,
            "vega": 0.0,
            "initial_margin_rate": INITIAL_MARGIN["Equity"],
            "collateral_type": COLLATERAL_TYPE["Equity"],
            "haircut": HAIRCUT["Equity"],
            "asset_manager": eq["asset_manager"],
            "commodity_sym": None,
        }))
        
        # ============================================================
        # STEP 5: Combine all frames
        # ============================================================
        print("[STEP 5] Combining all data...")
        
        non_empty_frames = [f for f in frames if not f.empty]
        
        if not non_empty_frames:
            print("  No new rows to process")
            return "NO_NEW_ROWS"
        
        df = pd.concat(non_empty_frames, ignore_index=True)
        print(f"  Combined rows: {len(df)}")
        
        # ============================================================
        # STEP 6: Add counterparty and trade_id
        # ============================================================
        print("[STEP 6] Adding counterparty and trade_id...")
        
        df["counterparty"] = fast_counterparty(df["ticker"])
        df["trade_id"] = df.apply(generate_trade_id, axis=1)
        
        # ============================================================
        # STEP 7: Calculate collateral metrics
        # ============================================================
        print("[STEP 7] Calculating collateral metrics...")
        
        df["exposure_before_collateral"] = df["notional"].abs()
        df["required_collateral"] = df["exposure_before_collateral"] * df["initial_margin_rate"]
        df["collateral_value"] = df["required_collateral"] * 0.9
        df["effective_collateral"] = df["collateral_value"] * (1 - df["haircut"])
        df["net_exposure"] = (df["exposure_before_collateral"] - df["effective_collateral"]).clip(lower=0)
        df["collateral_ratio"] = np.where(
            df["exposure_before_collateral"] > 0,
            df["effective_collateral"] / df["exposure_before_collateral"],
            0,
        )
        df["margin_call_flag"] = (df["net_exposure"] > 0).astype(int)
        df["margin_call_amount"] = df["net_exposure"]
        
        # ============================================================
        # STEP 8: Handle Greeks (delta, gamma, vega)
        # ============================================================
        print("[STEP 8] Handling Greeks...")
        
        mask_all = df[["delta", "gamma", "vega"]].isna().all(axis=1)
        df.loc[mask_all, ["delta", "gamma", "vega"]] = [1.0, 0.0, 0.0]
        
        for c in ["delta", "gamma", "vega"]:
            df[c] = df[c].fillna(df[c].median(skipna=True)).clip(lower=0)
        
        df["delta_equivalent_exposure"] = df["delta"] * df["notional"]
        
        # ============================================================
        # STEP 9: Add maturity dates and tenor
        # ============================================================
        print("[STEP 9] Adding maturity dates...")
        
        rand_years = np.random.choice(np.arange(0.5, 5.1, 0.5), size=len(df))
        df["date"] = pd.to_datetime(df["date"])
        df["maturity_date"] = df["date"] + pd.to_timedelta((rand_years * 365).round(), unit="D")
        df["tenor_years"] = rand_years
        
        # ============================================================
        # STEP 10: Calculate PnL
        # ============================================================
        print("[STEP 10] Calculating PnL...")
        
        vol = df.groupby("asset_class")["underlying_price"].transform("std").fillna(0)
        df["pnl"] = df["delta_equivalent_exposure"] * 0.001 * (vol / (vol.max() or 1))
        
        # Drop temporary columns
        df = df.drop(columns=["commodity_sym", "asset_manager"], errors="ignore")
        
        # ============================================================
        # STEP 11: Merge macro data (SOFT FAIL - NO ALERT)
        # ============================================================
        print("[STEP 11] Merging macro data...")
        
        macro = load_macro_with_retry()
        
        if not macro.empty:
            df["month_year"] = df["date"].dt.to_period("M").astype(str)
            macro["month_year"] = macro["date"].dt.to_period("M").astype(str)
            
            merged = df.merge(
                macro.drop(columns=["date"]),
                on="month_year",
                how="inner",
            ).drop(columns=["month_year"])
            
            if merged.empty:
                print("  WARNING: No rows after macro merge - continuing without macro")
                merged = df.copy()
                for col in ["gdp", "unrate", "cpi", "fedfunds"]:
                    if col not in merged.columns:
                        merged[col] = np.nan
        else:
            print("  WARNING: Macro data not available - continuing without macro")
            merged = df.copy()
            for col in ["gdp", "unrate", "cpi", "fedfunds"]:
                if col not in merged.columns:
                    merged[col] = np.nan
        
        print(f"  After macro merge: {len(merged)} rows")
        
        # Convert dates to date objects for Snowflake
        merged["date"] = pd.to_datetime(merged["date"]).dt.date
        merged["maturity_date"] = pd.to_datetime(merged["maturity_date"]).dt.date
        
        # ============================================================
        # STEP 12: Add pipeline metadata
        # ============================================================
        print("[STEP 12] Adding pipeline metadata...")
        
        merged = add_pipeline_metadata(merged, run_id)
        snowflake_rows = len(merged)
        
        # ============================================================
        # STEP 13: Write to Snowflake (IDEMPOTENT - MERGE) - CRITICAL
        # ============================================================
        print("[STEP 13] Writing to Snowflake (idempotent MERGE)...")
        
        def write_derivatives():
            return write_to_snowflake_idempotent(
                merged,
                "DERIVATIVES",
                key_columns=["trade_id", "date", "ticker", "derivative_type"],
                run_id=run_id,
                chunk_size=100000
            )
        
        affected = retry_with_backoff(write_derivatives, retries=3,
                                       critical_name="Snowflake MERGE write to DERIVATIVES table",
                                       run_id=run_id)
        print(f"  Snowflake MERGE DERIVATIVES: {affected} rows affected")
        
        # ============================================================
        # STEP 14: Write to Postgres (NO ALERTS - non-critical)
        # ============================================================
        print("[STEP 14] Writing to Postgres...")
        
        postgres_ok, postgres_error = upload_derivatives_to_postgres(merged)
        postgres_success = postgres_ok
        
        if not postgres_ok:
            print(f"  WARNING: Postgres derivative_data write failed: {postgres_error}")
        
        # ============================================================
        # STEP 15: Success
        # ============================================================
        processing_time = round(time.time() - pipeline_start, 2)
        print(f"\n{'='*66}")
        print(f"  DERIVATIVES PIPELINE SUCCESS - run_id={run_id}")
        print(f"  Snowflake rows: {snowflake_rows}")
        print(f"  Postgres: {'SUCCESS' if postgres_success else 'FAILED'}")
        print(f"  Duration: {processing_time}s")
        print(f"{'='*66}\n")
        
        return f"SUCCESS_{snowflake_rows}"
    
    except Exception as e:
        # Send CRITICAL alert for any unhandled exception
        send_critical_alert(
            f"Pipeline failed with unhandled exception",
            context={"run_id": run_id, "error": str(e)}
        )
        
        processing_time = round(time.time() - pipeline_start, 2)
        print(f"\n{'='*66}")
        print(f"  DERIVATIVES PIPELINE FAILED - run_id={run_id}")
        print(f"  Error: {str(e)}")
        print(f"  Duration: {processing_time}s")
        print(f"{'='*66}\n")
        
        # Re-raise for Airflow to catch
        raise