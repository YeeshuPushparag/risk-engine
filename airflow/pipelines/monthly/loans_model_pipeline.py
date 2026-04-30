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
from snowflake.connector.pandas_tools import write_pandas
from connections.snowflake_conn import get_snowflake_conn
from connections.postgre_conn import get_postgre_conn
from datetime import datetime, timedelta

S3_BUCKET = "pushparag-loan-bucket"
OUTPUT_TABLE = "LOANS"
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
    - Snowflake MERGE failures (system of record)
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
            postgres_status = context.get('postgres_status', 'UNKNOWN')
            duration = context.get('duration_seconds', 0)
            
            text = f"*[SUCCESS]* loans_model_pipeline\n" \
                   f"✅ FINAL LOAN PIPELINE completed successfully\n" \
                   f"run_id: {run_id}\n" \
                   f"Snowflake rows: {snowflake_rows:,}\n" \
                   f"Postgres: {postgres_status}\n" \
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
    df["pipeline_name"] = "loans_model_pipeline"
    df["pipeline_run_id"] = run_id
    df["data_source"] = "s3 + snowflake"
    df["input_source"] = "loan_enriched + macro"
    df["transformation"] = "loan_model_v1"
    df["record_created_at"] = datetime.utcnow()
    return df


# ============================================================
# MAIN PIPELINE (WITH CRITICAL + SUCCESS ALERTS)
# ============================================================
def run_loans_model_pipeline():
    """
    Complete production-grade loans model pipeline with:
    1. Retry on all external calls (S3, Snowflake, Postgres)
    2. Idempotent Snowflake writes (MERGE, not append) - NO DUPLICATES
    3. Safe model handling (WARNING, not HARD FAIL)
    4. Slack alerts for CRITICAL failures AND SUCCESS completion
    5. Run-level tracking with run_id
    6. Fixed query bug (COALESCE for empty LOANS table)
    """
    pipeline_start = time.time()
    run_id = datetime.utcnow().isoformat()
    np.random.seed(42)
    
    snowflake_rows = 0
    postgres_success = False
    macro_loaded = True
    
    print(f"\n{'='*66}")
    print(f"  LOANS PIPELINE START - run_id={run_id}")
    print(f"{'='*66}\n")
    
    try:
        # ============================================================
        # STEP 1: Load loan enriched data from S3 (CRITICAL)
        # ============================================================
        print("[STEP 1] Loading loan enriched data from S3...")
        
        loans = load_parquet_from_s3_with_retry(
            S3_BUCKET,
            "loan_enriched_fx_bonds_commod_derivatives_collateral.parquet",
            run_id=run_id,
            retries=3
        )
        print(f"  Loaded {len(loans)} rows")
        
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
        # STEP 4: Filter by last processed date (CRITICAL)
        # ============================================================
        print("[STEP 4] Filtering by last processed date...")
        
        def get_last_loan_date():
            with get_snowflake_conn() as ctx:
                cur = ctx.cursor()
                cur.execute(f'SELECT COALESCE(MAX("date"), \'1900-01-01\') FROM "{OUTPUT_TABLE}"')
                return cur.fetchone()[0]
        
        last_date = retry_with_backoff(get_last_loan_date, retries=2,
                                        critical_name="Snowflake LOANS watermark query",
                                        run_id=run_id)
        last_month = pd.Period(last_date, freq="M") if last_date else None
        
        if last_month:
            loans = loans[loans["date"].dt.to_period("M") > last_month]
        
        if loans.empty:
            print("  No new rows to process")
            processing_time = round(time.time() - pipeline_start, 2)
            print(f"\n{'='*66}")
            print(f"  LOANS PIPELINE COMPLETE - No new data")
            print(f"  Duration: {processing_time}s")
            print(f"{'='*66}\n")
            return "NO_NEW_DATA"
        
        print(f"  Rows after date filtering: {len(loans)}")
        
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
        # STEP 11: Add pipeline metadata
        # ============================================================
        print("[STEP 11] Adding pipeline metadata...")
        loans = add_pipeline_metadata(loans, run_id)
        snowflake_rows = len(loans)
        
        # ============================================================
        # STEP 12: Write to Snowflake (IDEMPOTENT - MERGE) - CRITICAL
        # ============================================================
        print("[STEP 12] Writing to Snowflake (idempotent MERGE)...")
        
        loans_sf = enforce_snowflake_types(loans)
        
        if not loans_sf.empty:
            def write_loans():
                return write_to_snowflake_idempotent(
                    loans_sf,
                    OUTPUT_TABLE,
                    key_columns=["loan_id", "date"],
                    run_id=run_id,
                    chunk_size=100000
                )
            
            affected = retry_with_backoff(write_loans, retries=3,
                                           critical_name="Snowflake MERGE write to LOANS table",
                                           run_id=run_id)
            print(f"  Snowflake MERGE LOANS: {affected} rows affected")
        
        # ============================================================
        # STEP 13: Write to Postgres (NO ALERTS - non-critical)
        # ============================================================
        print("[STEP 13] Writing to Postgres...")
        
        # Prepare for Postgres
        df_pg = drop_metadata_for_serving(loans.copy())
        df_pg["month"] = pd.to_datetime(df_pg["date"]).dt.to_period("M").dt.to_timestamp().dt.date
        
        # Drop Snowflake-only column
        if "date" in df_pg.columns:
            df_pg = df_pg.drop(columns=["date"])
        
        # Type fixes for Postgres
        if "stage" in df_pg.columns:
            df_pg["stage"] = df_pg["stage"].astype(float).round().astype(int)
        
        for c in ["loan_age_months", "time_to_maturity_months"]:
            if c in df_pg.columns:
                df_pg[c] = df_pg[c].fillna(0).astype(float).round().astype(int)
        
        postgres_ok, postgres_error = write_to_postgres_with_retry(df_pg, "loan_data", retries=3)
        postgres_success = postgres_ok
        
        if not postgres_ok:
            print(f"  WARNING: Postgres loan_data write failed: {postgres_error}")
        
        # ============================================================
        # STEP 14: Success - Send COMPLETORY Slack Alert
        # ============================================================
        processing_time = round(time.time() - pipeline_start, 2)
        pg_rows = len(df_pg) if 'df_pg' in locals() else 0
        
        # Send SUCCESS alert to Slack (FINAL pipeline completed)
        if postgres_success:
            send_success_alert({
                "run_id": run_id,
                "snowflake_rows": snowflake_rows,
                "postgres_status": "SUCCESS",
                "postgres_rows": pg_rows,
                "macro_loaded": macro_loaded,
                "duration_seconds": processing_time
            })
        else:
            print("SUCCESS (Snowflake) but Postgres failed -> skipping success alert")
        
        print(f"\n{'='*66}")
        print(f"  LOANS PIPELINE SUCCESS - run_id={run_id}")
        print(f"  Snowflake rows: {snowflake_rows}")
        print(f"  Postgres: {'SUCCESS' if postgres_success else 'FAILED'} ({pg_rows} rows)")
        print(f"  Macro loaded: {macro_loaded}")
        print(f"  Duration: {processing_time}s")
        print(f"{'='*66}\n")
        
        return f"SUCCESS_SNOWFLAKE_{snowflake_rows}_ROWS_PG_{pg_rows}_ROWS"
    
    except Exception as e:
        # Send CRITICAL alert for any unhandled exception
        send_critical_alert(
            f"Pipeline failed with unhandled exception",
            context={"run_id": run_id, "error": str(e)}
        )
        
        processing_time = round(time.time() - pipeline_start, 2)
        print(f"\n{'='*66}")
        print(f"  LOANS PIPELINE FAILED - run_id={run_id}")
        print(f"  Error: {str(e)}")
        print(f"  Duration: {processing_time}s")
        print(f"{'='*66}\n")
        
        # Re-raise for Airflow to catch
        raise