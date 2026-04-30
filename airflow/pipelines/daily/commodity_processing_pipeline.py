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

# ============================================================
# RETRY DECORATOR (PRODUCTION-GRADE)
# ============================================================
def retry_with_backoff(func, retries=3, backoff_factor=2, exceptions=(Exception,)):
    """
    Retry a function with exponential backoff.
    
    Args:
        func: Function to retry
        retries: Number of retry attempts
        backoff_factor: Multiplier for backoff (2, 4, 8 seconds)
        exceptions: Tuple of exceptions to catch and retry
    
    Returns:
        Function result
    
    Raises:
        Last exception if all retries fail
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
    
    raise last_exception


# ============================================================
# SNOWFLAKE IDEMPOTENT WRITE (MERGE - NO DUPLICATES)
# ============================================================
def write_to_snowflake_idempotent(df, table_name, key_columns, chunk_size=20000):
    """
    Write to Snowflake with MERGE logic to prevent duplicates.
    
    Args:
        df: DataFrame to write
        table_name: Target table name
        key_columns: List of columns that form unique key (e.g., ["ticker", "date"])
        chunk_size: Rows per chunk for write_pandas
    
    Returns:
        Tuple (inserted_count, updated_count)
    """
    temp_table = f"{table_name}_TEMP_{int(time.time())}"
    
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
            
            print(f"  Snowflake MERGE completed: {affected_rows} rows affected")
            
            return affected_rows


# ============================================================
# S3 LOAD WITH RETRY
# ============================================================
def load_csv_from_s3_with_retry(bucket, key, parse_dates=None, dayfirst=True, retries=3):
    """Load CSV from S3 with retry logic."""
    def _load():
        obj = s3.get_object(Bucket=bucket, Key=key)
        return pd.read_csv(BytesIO(obj["Body"].read()), parse_dates=parse_dates, dayfirst=dayfirst)
    
    return retry_with_backoff(_load, retries=retries)


def load_parquet_from_s3_with_retry(bucket, key, retries=3):
    """Load parquet from S3 with retry logic."""
    def _load():
        obj = s3.get_object(Bucket=bucket, Key=key)
        return pd.read_parquet(BytesIO(obj["Body"].read()))
    
    return retry_with_backoff(_load, retries=retries)


def load_model_from_s3_with_retry(bucket, key, retries=3):
    """Load XGBoost model from S3 with retry logic."""
    def _load():
        model_obj = s3.get_object(Bucket=bucket, Key=key)
        booster = xgb.Booster()
        booster.load_model(bytearray(model_obj["Body"].read()))
        return booster
    
    return retry_with_backoff(_load, retries=retries)


def load_features_from_s3_with_retry(bucket, key, retries=3):
    """Load feature list from S3 with retry logic."""
    def _load():
        feat_obj = s3.get_object(Bucket=bucket, Key=key)
        return joblib.load(BytesIO(feat_obj["Body"].read()))
    
    return retry_with_backoff(_load, retries=retries)


# ============================================================
# POSTGRES WRITE WITH RETRY AND PARTIAL SUCCESS HANDLING
# ============================================================
def write_to_postgres_with_retry(df, table_name, retries=3):
    """
    Write to Postgres with retry logic.
    Returns success flag and error message if any.
    """
    if df.empty:
        return True, "Empty DataFrame - nothing to write"
    
    last_error = None
    
    for attempt in range(retries + 1):
        try:
            # Type casting for integer columns
            if "volume" in df.columns:
                df["volume"] = (
                    df["volume"]
                    .fillna(0)
                    .astype(float)
                    .round()
                    .astype(int)
                )
            
            BATCH_SIZE = 100000
            
            with get_postgre_conn() as pg_conn:
                with pg_conn.cursor() as pg_cur:
                    # Fetch schema order
                    pg_cur.execute(f"""
                        SELECT column_name
                        FROM information_schema.columns
                        WHERE table_schema='public'
                        AND table_name='{table_name}'
                        ORDER BY ordinal_position
                    """)
                    
                    pg_cols_order = [
                        r[0] for r in pg_cur.fetchall()
                        if r[0] != "id"
                    ]
                    
                    if not pg_cols_order:
                        raise RuntimeError(f"No columns found for public.{table_name}")
                    
                    # Validate schema
                    missing = set(pg_cols_order) - set(df.columns)
                    if missing:
                        raise ValueError(f"[POSTGRES ERROR] Missing columns: {missing}")
                    
                    # Enforce schema order
                    df_pg = df[pg_cols_order].copy()
                    
                    quoted_cols = [f'"{c}"' for c in pg_cols_order]
                    copy_sql = f"COPY public.{table_name} ({','.join(quoted_cols)}) FROM STDIN WITH CSV"
                    
                    # Batch COPY
                    for start in range(0, len(df_pg), BATCH_SIZE):
                        chunk = df_pg.iloc[start:start+BATCH_SIZE]
                        buf = StringIO()
                        chunk.to_csv(buf, index=False, header=False)
                        buf.seek(0)
                        
                        with pg_cur.copy(copy_sql) as copy:
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
# CONSTANTS
# ============================================================
S3_BUCKET = "pushparag-commodity-bucket"
s3 = boto3.client("s3")

# === S3 PATH PREFIXES ===
BASE_PREFIX = "historical-commodity/"
ROLLING_PREFIX = BASE_PREFIX + "rolling/"
MODEL_PREFIX = "models/"

# === FILE PATHS ===
INPUT_COMMOD = ROLLING_PREFIX + "commodities_30d.parquet"
SYM = BASE_PREFIX + "unique_tickers_sector.csv"
MACRO = BASE_PREFIX + "macro_data.csv"

MODEL_FILE = MODEL_PREFIX + "commodities_model_vol21_xgb.json"
FEATURE_FILE = MODEL_PREFIX + "commodities_features_vol21.pkl"


# ============================================================
# HELPER FUNCTIONS
# ============================================================
def rename_source_lineage(df):
    rename_map = {
        "pipeline_name": "source_pipeline",
        "pipeline_run_id": "source_run_id",
        "data_source": "source_data_source",
        "input_source": "source_input_source",
        "transformation": "source_transformation",
        "record_created_at": "source_created_at",
        "feature_version": "source_feature_version",
        "schema_version": "source_schema_version",
        "schema_hash": "source_schema_hash",
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
        "partial_run",
    ]

    existing = [c for c in drop_cols if c in df.columns]
    return df.drop(columns=existing)


def drop_metadata_for_serving(df):
    drop_cols = [
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

    return df.drop(columns=[c for c in drop_cols if c in df.columns])


# ============================================================
# MAIN COMMODITY PROCESSING PIPELINE (COMPLETE FIXED VERSION)
# ============================================================
def process_commodities():
    """
    Complete production-grade commodity processing pipeline with:
    1. Retry on all external calls (S3, Snowflake, Postgres)
    2. Idempotent Snowflake writes (MERGE, not append) - NO DUPLICATES
    3. Safe model handling (WARNING, not HARD FAIL)
    4. Partial success handling (Snowflake success doesn't require Postgres success)
    5. Fixed variable bugs and added defensive programming
    
    Returns a status string for Airflow/XCom.
    """
    pipeline_start = time.time()
    run_id = datetime.utcnow().isoformat()
    snowflake_rows = 0
    postgres_success = False
    
    print(f"\n{'='*66}")
    print(f"  COMMODITY PIPELINE START - run_id={run_id}")
    print(f"{'='*66}\n")
    
    try:
        # ============================================================
        # STEP 1: Load company and macro data (with retry)
        # ============================================================
        print("[STEP 1] Loading company and macro data from S3...")
        
        companies = load_csv_from_s3_with_retry(S3_BUCKET, SYM, dayfirst=True, retries=3)
        macro = load_csv_from_s3_with_retry(S3_BUCKET, MACRO, parse_dates=["date"], dayfirst=True, retries=3)
        
        print(f"  Companies loaded: {len(companies)}")
        print(f"  Macro rows loaded: {len(macro)}")
        
        # ============================================================
        # STEP 2: Get last date from Snowflake COMMODITY table (with retry)
        # ============================================================
        print("[STEP 2] Getting last date from Snowflake COMMODITY table...")
        
        def get_last_commodity_date():
            with get_snowflake_conn() as ctx:
                with ctx.cursor() as cs:
                    cs.execute('SELECT MAX("date") FROM "COMMODITY"')
                    return cs.fetchone()[0]
        
        max_date_commo = retry_with_backoff(get_last_commodity_date, retries=2)
        
        if max_date_commo is not None:
            max_date_commo = pd.Timestamp(max_date_commo).date()
        else:
            max_date_commo = pd.Timestamp("1970-01-01").date()
        
        print(f"  Last date in Commodity table: {max_date_commo}")
        
        # ============================================================
        # STEP 3: Get MTM data from Snowflake EQUITY table (with retry)
        # ============================================================
        print("[STEP 3] Fetching MTM data from Snowflake EQUITY table...")
        
        def get_mtm_data():
            with get_snowflake_conn() as ctx:
                with ctx.cursor() as cs:
                    cs.execute(f"""
                        SELECT "ticker","date","mtm_value","asset_manager"
                        FROM EQUITY
                        WHERE "date" > '{max_date_commo}'
                    """)
                    mtm_rows = cs.fetchall()
                    mtm_cols = ["ticker", "date", "mtm_value", "asset_manager"]
                    mtm_df = pd.DataFrame(mtm_rows, columns=mtm_cols)
                    mtm_df["date"] = pd.to_datetime(mtm_df["date"])
                    return mtm_df
        
        mtm = retry_with_backoff(get_mtm_data, retries=2)
        
        if mtm.empty:
            print("  No new MTM rows to process")
            return "NO_NEW_MTM_ROWS"
        
        print(f"  MTM rows: {len(mtm)}")
        
        # ============================================================
        # STEP 4: Load commodity parquet from S3 (with retry)
        # ============================================================
        print("[STEP 4] Loading commodity data from S3...")
        
        commod_base = load_parquet_from_s3_with_retry(S3_BUCKET, INPUT_COMMOD, retries=3)
        print(f"  Commodity rows: {len(commod_base)}")
        
        # Apply lineage transformations
        commod_base = rename_source_lineage(commod_base)
        commod_base = drop_old_pipeline_metrics(commod_base)
        commod_base["date"] = pd.to_datetime(commod_base["date"])
        
        # Filter to new dates only
        commod_base = commod_base[commod_base["date"].dt.date > max_date_commo]
        
        if commod_base.empty:
            print("  No new commodity rows to process")
            return "NO_NEW_COMMOD_ROWS"
        
        # Rename column for consistency
        commod_base.rename(columns={"commodity_symbol": "commodity"}, inplace=True)
        print(f"  New commodity rows: {len(commod_base)}")
        
        # ============================================================
        # STEP 5: Sector to commodity mapping
        # ============================================================
        print("[STEP 5] Building sector-commodity exposure mapping...")
        
        sector_to_commodities = {
            "Energy": {"CL=F": 0.7, "NG=F": 0.3},
            "Basic Materials": {"GC=F": 0.3, "SI=F": 0.3, "ZC=F": 0.4},
            "Industrials": {"CL=F": 0.4, "GC=F": 0.6},
            "Consumer Defensive": {"ZC=F": 0.6, "GC=F": 0.4},
            "Utilities": {"NG=F": 0.8, "CL=F": 0.2},
            "Technology": {"GC=F": 0.7, "SI=F": 0.3},
            "Healthcare": {"SI=F": 0.4, "GC=F": 0.6},
            "Financial Services": {"GC=F": 0.8, "CL=F": 0.2},
            "Real Estate": {"GC=F": 0.5, "ZC=F": 0.5},
            "Communication Services": {"GC=F": 0.5, "CL=F": 0.5},
            "Consumer Cyclical": {"CL=F": 0.6, "GC=F": 0.4},
        }
        
        exposure_rows = []
        
        for _, r in companies.iterrows():
            mapping = sector_to_commodities.get(r["sector"])
            if not mapping:
                continue
            
            for comm, weight in mapping.items():
                exposure_rows.append({
                    "ticker": r["ticker"],
                    "sector": r["sector"],
                    "industry": r["industry"],
                    "commodity": comm,
                    "sensitivity": weight,
                })
        
        exp = pd.DataFrame(exposure_rows)
        print(f"  Exposure mapping rows: {len(exp)}")
        
        # ============================================================
        # STEP 6: Merge commodity data and MTM
        # ============================================================
        print("[STEP 6] Merging commodity and MTM data...")
        
        # Merge commodity data
        seg = exp.merge(commod_base, on="commodity", how="left", validate="m:m")
        
        # Merge MTM
        seg = seg.merge(mtm, on=["ticker", "date"], how="inner")
        
        # Convert and clean numeric columns
        seg["mtm_value"] = pd.to_numeric(seg["mtm_value"], errors="coerce").fillna(0)
        seg["vol_20d"] = pd.to_numeric(seg["vol_20d"], errors="coerce")
        seg["daily_return"] = pd.to_numeric(seg["daily_return"], errors="coerce").fillna(0.0)
        
        # Calculate hedge ratio using percentile rank
        seg["hedge_ratio"] = (0.2 + 0.6 * seg["vol_20d"].rank(pct=True)).clip(0, 1)
        
        # Calculate exposure and PnL
        seg["exposure_amount"] = seg["sensitivity"] * seg["mtm_value"]
        seg["commodity_pnl"] = (
            seg["exposure_amount"]
            * seg["daily_return"]
            * (1 - seg["hedge_ratio"])
        )
        
        # ============================================================
        # STEP 7: Merge macro data
        # ============================================================
        print("[STEP 7] Merging macro data...")
        
        seg["date"] = pd.to_datetime(seg["date"])
        macro["date"] = pd.to_datetime(macro["date"])
        
        seg["mm_yy"] = seg["date"].dt.strftime("%m-%y")
        macro["mm_yy"] = macro["date"].dt.strftime("%m-%y")
        
        macro_for_merge = macro.drop(columns=["date"])
        seg = seg.merge(macro_for_merge, on="mm_yy", how="left").drop(columns=["mm_yy"])
        
        # ============================================================
        # STEP 8: Select final columns
        # ============================================================
        print("[STEP 8] Selecting final columns...")
        
        cols = [
            "ticker", "asset_manager", "sector", "industry", "commodity", "date",
            "open", "high", "low", "close", "volume", "daily_return", "log_return",
            "vol_20d", "sensitivity", "hedge_ratio", "mtm_value", "exposure_amount",
            "commodity_pnl", "VaR_95", "VaR_99", "gdp", "unrate", "cpi", "fedfunds",
            "source_pipeline", "source_run_id", "source_data_source",
            "source_input_source", "source_transformation", "source_created_at",
            "source_feature_version", "source_schema_version", "source_schema_hash",
            "pipeline_name", "pipeline_run_id", "data_source", "input_source",
            "transformation", "record_created_at"
        ]
        
        available_cols = [c for c in cols if c in seg.columns]
        final_new = seg[available_cols].sort_values(
            ["commodity", "date", "ticker"]
        ).reset_index(drop=True)
        
        if final_new.empty:
            print("  No output rows generated")
            return "NO_OUTPUT_ROWS"
        
        print(f"  Final rows: {len(final_new)}")
        
        # ============================================================
        # STEP 9: Load XGBoost model and predict volatility (SAFE)
        # ============================================================
        print("[STEP 9] Loading XGBoost model and predicting volatility...")
        
        # Load model and features with retry
        booster = load_model_from_s3_with_retry(S3_BUCKET, MODEL_FILE, retries=2)
        feature_cols = load_features_from_s3_with_retry(S3_BUCKET, FEATURE_FILE, retries=2)
        
        # Check missing features - log warnings, don't crash
        missing = [f for f in feature_cols if f not in final_new.columns]
        if missing:
            print(f"  WARNING: Missing features for Commodity model: {missing[:5]}")
            for col in missing:
                final_new[col] = 0
        
        # Fill nulls with 0 for prediction
        available_features = [f for f in feature_cols if f in final_new.columns]
        null_cols = [c for c in available_features if final_new[c].isna().any()]
        if null_cols:
            print(f"  WARNING: Null values in features: {null_cols[:5]} (filling with 0)")
            for col in null_cols:
                final_new[col] = final_new[col].fillna(0)
        
        # Prepare features in correct order
        X_all = final_new[available_features].copy()
        X_all = X_all.fillna(0)
        
        try:
            dmat = xgb.DMatrix(X_all)
            final_new["pred_vol21"] = booster.predict(dmat)
            print("  Model prediction completed")
        except Exception as e:
            print(f"  WARNING: Model prediction failed: {e}")
            final_new["pred_vol21"] = final_new["vol_20d"].fillna(0.01)
        
        # Convert date to date object for Snowflake
        final_new["date"] = final_new["date"].dt.date
        
        # ============================================================
        # STEP 10: Add pipeline metadata
        # ============================================================
        print("[STEP 10] Adding pipeline metadata...")
        
        final_new["pipeline_name"] = "commodity_processing_pipeline"
        final_new["pipeline_run_id"] = run_id
        final_new["data_source"] = "s3 + snowflake"
        final_new["input_source"] = "commodity_features + mtm + macro"
        final_new["transformation"] = "commodity_exposure_v1"
        final_new["record_created_at"] = datetime.utcnow()
        
        snowflake_rows = len(final_new)
        
        # ============================================================
        # STEP 11: Write to Snowflake (IDEMPOTENT - MERGE)
        # ============================================================
        print("[STEP 11] Writing to Snowflake (idempotent MERGE)...")
        
        def do_snowflake_write():
            return write_to_snowflake_idempotent(
                final_new,
                "COMMODITY",
                key_columns=["ticker", "commodity", "date"],
                chunk_size=20000
            )
        
        affected = retry_with_backoff(do_snowflake_write, retries=3)
        print(f"  Snowflake MERGE complete: {affected} rows affected")
        
        # ============================================================
        # STEP 12: Write to Postgres (WITH RETRY, non-critical)
        # ============================================================
        print("[STEP 12] Writing to Postgres...")
        
        df_pg = drop_metadata_for_serving(final_new.copy())
        postgres_ok, postgres_error = write_to_postgres_with_retry(df_pg, "commodity_data", retries=3)
        postgres_success = postgres_ok
        
        if not postgres_ok:
            print(f"  WARNING: Postgres commodity write failed: {postgres_error}")
            # Don't fail the pipeline - Snowflake is the source of truth
        
        # ============================================================
        # STEP 13: Success
        # ============================================================
        processing_time = round(time.time() - pipeline_start, 2)
        print(f"\n{'='*66}")
        print(f"  COMMODITY PIPELINE SUCCESS - run_id={run_id}")
        print(f"  Snowflake rows: {snowflake_rows}")
        print(f"  Postgres: {'SUCCESS' if postgres_success else 'FAILED'}")
        print(f"  Duration: {processing_time}s")
        print(f"{'='*66}\n")
        
        return f"UPLOAD_SUCCESS_{snowflake_rows}_ROWS"
    
    except Exception as e:
        processing_time = round(time.time() - pipeline_start, 2)
        print(f"\n{'='*66}")
        print(f"  COMMODITY PIPELINE FAILED - run_id={run_id}")
        print(f"  Error: {str(e)}")
        print(f"  Duration: {processing_time}s")
        print(f"{'='*66}\n")
        raise