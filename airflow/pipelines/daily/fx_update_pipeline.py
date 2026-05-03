import os
import time
import pandas as pd
import numpy as np
import datetime
import joblib
import xgboost as xgb
import boto3
import requests
from io import BytesIO, StringIO
from connections.postgre_conn import get_postgre_conn
from connections.snowflake_conn import get_snowflake_conn
from snowflake.connector.pandas_tools import write_pandas
from datetime import datetime, timedelta

# ============================================================
# PRODUCTION-GRADE ALERTING (CRITICAL ONLY)
# ============================================================
def send_critical_alert(message: str, context: dict = None):
    """
    Send CRITICAL alert to Slack only for unrecoverable pipeline failures.
    
    Triggers only for:
    - Snowflake MERGE failures (system of record)
    - S3 data loading failures (after retries)
    - Complete pipeline failures
    
    Does NOT send for:
    - Retry attempts (only final failure)
    - Postgres failures (non-critical)
    - Model failures (soft fail)
    """
    payload = {
        "level": "CRITICAL",
        "pipeline": "fx_update_pipeline",
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
            text = f"*[CRITICAL]* fx_update_pipeline\n" \
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
def write_to_snowflake_idempotent(df, table_name, key_columns, run_id=None, chunk_size=20000):
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
                
                print(f"  Snowflake MERGE completed: {affected_rows} rows affected")
                return affected_rows
                
    except Exception as exc:
        send_critical_alert(
            f"Snowflake MERGE failed for table {table_name}",
            context={"run_id": run_id, "table": table_name, "rows": len(df), "error": str(exc)}
        )
        raise


# ============================================================
# S3 LOAD WITH RETRY (CRITICAL IF FAILS)
# ============================================================
def load_parquet_from_s3_with_retry(bucket, key, run_id=None, retries=3):
    """Load parquet from S3 with retry logic. CRITICAL if fails."""
    def _load():
        obj = s3.get_object(Bucket=bucket, Key=key)
        return pd.read_parquet(BytesIO(obj["Body"].read()))
    
    return retry_with_backoff(_load, retries=retries, 
                               critical_name=f"S3 parquet load: {key}",
                               run_id=run_id)


def load_model_from_s3_with_retry(bucket, key, run_id=None, retries=3):
    """Load XGBoost model from S3 with retry logic. NOT CRITICAL - soft fail."""
    def _load():
        model_obj = s3.get_object(Bucket=bucket, Key=key)
        booster = xgb.Booster()
        booster.load_model(bytearray(model_obj["Body"].read()))
        return booster
    
    # No critical alert for model loading - soft fail is acceptable
    return retry_with_backoff(_load, retries=retries)


def load_features_from_s3_with_retry(bucket, key, run_id=None, retries=3):
    """Load feature list from S3 with retry logic. NOT CRITICAL - soft fail."""
    def _load():
        feat_obj = s3.get_object(Bucket=bucket, Key=key)
        return joblib.load(BytesIO(feat_obj["Body"].read()))
    
    # No critical alert for feature loading - soft fail is acceptable
    return retry_with_backoff(_load, retries=retries)


# ============================================================
# CONSTANTS
# ============================================================
S3_BUCKET = "yeeshu-fx-bucket"
s3 = boto3.client("s3")

TRADING_DAYS = 252
BASE_HEDGE = 0.10
W_VOL = 0.75
W_CARRY = 0.15
Z_95, Z_99 = 1.65, 2.33
TRADING_DAYS_PER_Q = 63


# ============================================================
# FX ENRICHMENT (SAME LOGIC AS ORIGINAL)
# ============================================================
def fx_enrichment(df):
    df = df.sort_values(["ticker", "date"]).reset_index(drop=True)

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

    df["carry_daily"] = (df["interest_diff"] / 100) / TRADING_DAYS
    df["return_carry_adj"] = df["fx_return"] + df["carry_daily"]

    def minmax_grp(s):
        vmin, vmax = s.min(), s.max()
        return (s - vmin) / (vmax - vmin if vmax > vmin else 1.0)

    df["vol_scaled"] = df.groupby("currency_pair")["fx_volatility"].transform(minmax_grp)

    abs_int = abs(df["interest_diff"])
    imin, imax = abs_int.min(), abs_int.max()
    df["int_scaled"] = 1 - (abs_int - imin) / (imax - imin if imax > imin else 1.0)

    df["hedge_ratio_raw"] = BASE_HEDGE + W_VOL * df["vol_scaled"] + W_CARRY * df["int_scaled"]

    med_vol = df["vol_scaled"].median()
    sigmoid = 1 / (1 + np.exp(-6 * (df["vol_scaled"] - med_vol)))
    df["hedge_ratio"] = np.clip(0.5 * sigmoid + 0.5 * df["hedge_ratio_raw"], 0, 1)

    df["exposure_amount"] = df["position_size"] * (1 - df["hedge_ratio"])

    df["VaR_95"] = df["position_size"] * df["fx_volatility"] * Z_95
    df["VaR_99"] = df["position_size"] * df["fx_volatility"] * Z_99
    df["value_at_risk"] = df["VaR_99"]

    df["volume"] = 0.25 * df["position_size"]
    df["fx_pnl"] = df["exposure_amount"] * df["fx_return"]
    df["carry_pnl"] = df["exposure_amount"] * df["carry_daily"]
    df["total_pnl"] = df["fx_pnl"] + df["carry_pnl"]
    df["expected_pnl"] = df["total_pnl"]
    df["sharpe_like_ratio"] = df["total_pnl"] / (
        df["position_size"] * df["fx_volatility"].replace(0, np.nan)
    )
    df["is_warmup"] = df[["fx_volatility_20d", "fx_volatility_30d"]].isna().any(axis=1)

    return df[~df["is_warmup"]].copy()


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
# POSTGRES WRITE WITH RETRY (NO ALERTS - NON-CRITICAL)
# ============================================================
def write_to_postgres_with_retry(df, retries=3):
    """
    Write to Postgres with retry logic.
    Returns success flag and error message if any.
    NO ALERTS - Postgres is serving layer, Snowflake is source of truth.
    """
    if df.empty:
        return True, "Empty DataFrame - nothing to write"
    
    last_error = None
    
    for attempt in range(retries + 1):
        try:
            # Type casting for integer columns
            integer_cols = ["volume", "position_size", "exposure_amount"]

            for col in integer_cols:
                if col in df.columns:
                    df[col] = (
                        df[col]
                        .fillna(0)
                        .astype(float)
                        .round()
                        .astype(int)
                    )

            with get_postgre_conn() as pg_conn:
                with pg_conn.cursor() as pg_cur:
                    # Fetch schema order
                    pg_cur.execute("""
                        SELECT column_name
                        FROM information_schema.columns
                        WHERE table_schema='public'
                        AND table_name='fx_data'
                        ORDER BY ordinal_position
                    """)

                    pg_cols_order = [
                        row[0] for row in pg_cur.fetchall()
                        if row[0] != "id"
                    ]

                    if not pg_cols_order:
                        raise RuntimeError("No columns found for public.fx_data")

                    # Validate schema
                    missing = set(pg_cols_order) - set(df.columns)
                    if missing:
                        raise ValueError(f"[FX POSTGRES ERROR] Missing columns: {missing}")

                    # Enforce schema order
                    df_pg = df[pg_cols_order].copy()

                    # COPY to Postgres
                    buf = StringIO()
                    df_pg.to_csv(buf, index=False, header=False)
                    buf.seek(0)

                    columns_quoted = [f'"{col}"' for col in pg_cols_order]
                    copy_sql = f"COPY public.fx_data ({','.join(columns_quoted)}) FROM STDIN WITH CSV"

                    with pg_cur.copy(copy_sql) as copy:
                        copy.write(buf.getvalue())

                pg_conn.commit()

            print(f"[POSTGRES FX] Uploaded {len(df)} rows successfully")
            return True, None
            
        except Exception as e:
            last_error = e
            if attempt < retries:
                wait_time = 2 ** attempt
                print(f"  Postgres retry {attempt + 1}/{retries} after {wait_time}s: {e}")
                time.sleep(wait_time)
            else:
                print(f"  Postgres FX failed after {retries} retries: {e}")
    
    return False, str(last_error)


# ============================================================
# FX UPDATE PIPELINE WITH S3 + SNOWFLAKE (CRITICAL ALERTS ONLY)
# ============================================================
def update_fx_snowflake():
    """
    Updates the FX Snowflake table using enriched FX exposure data from S3
    and an XGBoost model for 21d volatility.
    
    Slack alerts ONLY for:
    - Snowflake watermark query failure
    - S3 FX data load failure
    - Snowflake MERGE failure
    - Unhandled pipeline exception
    
    Returns a status string for Airflow/XCom.
    """
    pipeline_start = time.time()
    run_id = datetime.utcnow().isoformat()
    snowflake_rows = 0
    postgres_success = False
    
    print(f"\n{'='*66}")
    print(f"  FX PIPELINE START - run_id={run_id}")
    print(f"{'='*66}\n")
    
    try:
        # ============================================================
        # STEP 1: Get last date from Snowflake FX table (CRITICAL)
        # ============================================================
        today = datetime.date.today()
        
        def get_last_fx_date():
            with get_snowflake_conn() as ctx:
                with ctx.cursor() as cs:
                    cs.execute('SELECT MAX("date") FROM "FX"')
                    return cs.fetchone()[0]
        
        last_date_sf = retry_with_backoff(get_last_fx_date, retries=2,
                                           critical_name="Snowflake FX watermark query",
                                           run_id=run_id)
        
        if last_date_sf is not None:
            last_date_sf = pd.Timestamp(last_date_sf).date()
        else:
            last_date_sf = pd.Timestamp("1970-01-01").date()
        
        print(f"  Last date in Snowflake FX: {last_date_sf}")
        print(f"  Today: {today}")
        
        if last_date_sf >= today:
            print("  No new data needed - pipeline complete")
            return "NO_UPDATE_NEEDED"
        
        # ============================================================
        # STEP 2: Load FX file from S3 (CRITICAL)
        # ============================================================
        print("  Loading FX exposure data from S3...")
        fx = load_parquet_from_s3_with_retry(
            S3_BUCKET, 
            "historical-fx/rolling/fx_exposure_30d.parquet",
            run_id=run_id,
            retries=3
        )
        
        print(f"  Loaded FX rows: {len(fx)}")
        fx["date"] = pd.to_datetime(fx["date"]).dt.date
        
        # ============================================================
        # STEP 3: Filter for enrichment (FIXED VARIABLE NAME BUG)
        # ============================================================
        start_date = last_date_sf + timedelta(days=1)
        print(f"  Processing data from: {start_date}")
        
        fx_for_enrichment = (
            fx
            .groupby("ticker", group_keys=False)
            .apply(lambda x: x[x["date"] < start_date].tail(41) if len(x[x["date"] < start_date]) > 0 else x.tail(41))
            .reset_index(drop=True)
        )
        
        if fx_for_enrichment.empty:
            print("  No data available for enrichment")
            return "NO_DATA_FOR_ENRICHMENT"
        
        # ============================================================
        # STEP 4: FX enrichment
        # ============================================================
        print("  Running FX enrichment...")
        df_enriched = fx_enrichment(fx_for_enrichment)
        
        # Filter only new rows
        filtered_enriched_fx = df_enriched[df_enriched["date"] > last_date_sf]
        filtered_enriched_fx = rename_source_lineage(filtered_enriched_fx)
        filtered_enriched_fx = drop_old_pipeline_metrics(filtered_enriched_fx)
        
        if filtered_enriched_fx.empty:
            print("  No new FX rows to process")
            return "NO_NEW_ROWS"
        
        print(f"  New FX rows to process: {len(filtered_enriched_fx)}")
        
        # ============================================================
        # STEP 5: Add pipeline metadata
        # ============================================================
        filtered_enriched_fx["pipeline_name"] = "fx_update_pipeline"
        filtered_enriched_fx["pipeline_run_id"] = run_id
        filtered_enriched_fx["data_source"] = "s3_fx_features"
        filtered_enriched_fx["input_source"] = "fx_features + mtm + macro"
        filtered_enriched_fx["transformation"] = "fx_enrichment_v1"
        filtered_enriched_fx["record_created_at"] = datetime.utcnow()
        
        # ============================================================
        # STEP 6: Load XGBoost model & features (SOFT FAIL - NO ALERTS)
        # ============================================================
        print("  Loading FX volatility model...")
        
        try:
            booster = load_model_from_s3_with_retry(S3_BUCKET, "models/fx_model_vol21_xgb.json", retries=2)
            feature_cols = load_features_from_s3_with_retry(S3_BUCKET, "models/fx_features_vol21.pkl", retries=2)
            model_loaded = True
        except Exception as e:
            print(f"  WARNING: Could not load FX model: {e} - continuing without predictions")
            model_loaded = False
            booster = None
            feature_cols = []
        
        # ============================================================
        # STEP 7: Safe model prediction (WARNING, not HARD FAIL)
        # ============================================================
        if model_loaded and feature_cols:
            # Check missing features - log warnings, don't crash
            missing = [f for f in feature_cols if f not in filtered_enriched_fx.columns]
            if missing:
                print(f"  WARNING: Missing features for FX model: {missing[:5]}")
                for col in missing:
                    filtered_enriched_fx[col] = 0
            
            # Fill nulls with 0 for prediction
            null_cols = [c for c in feature_cols if c in filtered_enriched_fx.columns and filtered_enriched_fx[c].isna().any()]
            if null_cols:
                print(f"  WARNING: Null values in features: {null_cols[:5]} (filling with 0)")
                for col in null_cols:
                    filtered_enriched_fx[col] = filtered_enriched_fx[col].fillna(0)
            
            # Prepare features in correct order
            available_features = [f for f in feature_cols if f in filtered_enriched_fx.columns]
            X = filtered_enriched_fx[available_features].copy()
            X = X.fillna(0)
            
            print("  Running FX volatility prediction...")
            
            try:
                filtered_enriched_fx["predicted_volatility_21d"] = booster.predict(xgb.DMatrix(X))
                print("  FX model prediction completed")
            except Exception as e:
                print(f"  WARNING: FX model prediction failed: {e}")
                filtered_enriched_fx["predicted_volatility_21d"] = filtered_enriched_fx["fx_volatility_20d"].fillna(0.01)
        else:
            print("  WARNING: Model not available - using historical volatility")
            filtered_enriched_fx["predicted_volatility_21d"] = filtered_enriched_fx["fx_volatility_20d"].fillna(0.01)
        
        # ============================================================
        # STEP 8: Prepare final columns for Snowflake
        # ============================================================
        final_cols = [
            "ticker","sector","industry","currency_pair","foreign_revenue_ratio","date",
            "fx_rate","fx_return","fx_volatility_20d","fx_volatility_30d","fx_volatility",
            "interest_diff","carry_daily","return_carry_adj",
            "position_size","hedge_ratio","exposure_amount",
            "fx_pnl","carry_pnl","total_pnl","expected_pnl",
            "VaR_95","VaR_99","value_at_risk",
            "volume","sharpe_like_ratio","is_warmup",
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
            "record_created_at"
        ]
        
        # Keep only columns that exist
        df_to_upload = filtered_enriched_fx[[c for c in final_cols if c in filtered_enriched_fx.columns]]
        df_to_upload = df_to_upload.sort_values(["ticker", "date"])
        snowflake_rows = len(df_to_upload)
        print(f"  Preparing {snowflake_rows} rows for Snowflake...")
        
        # ============================================================
        # STEP 9: Write to Snowflake (IDEMPOTENT - MERGE) - CRITICAL
        # ============================================================
        print("  Writing to Snowflake (idempotent MERGE)...")
        
        def do_snowflake_write():
            return write_to_snowflake_idempotent(
                df_to_upload,
                "FX",
                key_columns=["ticker", "date"],
                run_id=run_id,
                chunk_size=20000
            )
        
        affected = retry_with_backoff(do_snowflake_write, retries=3,
                                       critical_name="Snowflake MERGE write to FX table",
                                       run_id=run_id)
        print(f"  Snowflake MERGE complete: {affected} rows affected")
        
        # ============================================================
        # STEP 10: Write to Postgres (NO ALERTS - non-critical)
        # ============================================================
        print("  Writing to Postgres...")
        
        # Prepare Postgres data (remove metadata)
        df_pg = drop_metadata_for_serving(df_to_upload.copy())
        postgres_ok, postgres_error = write_to_postgres_with_retry(df_pg, retries=3)
        postgres_success = postgres_ok
        
        if not postgres_ok:
            print(f"  WARNING: Postgres FX write failed: {postgres_error}")
            # Don't fail the pipeline - Snowflake is the source of truth
        
        # ============================================================
        # STEP 11: Success
        # ============================================================
        processing_time = round(time.time() - pipeline_start, 2)
        print(f"\n{'='*66}")
        print(f"  FX PIPELINE SUCCESS - run_id={run_id}")
        print(f"  Snowflake rows: {snowflake_rows}")
        print(f"  Postgres: {'SUCCESS' if postgres_success else 'FAILED'}")
        print(f"  Duration: {processing_time}s")
        print(f"{'='*66}\n")
        
        return f"UPLOAD_SUCCESS_{snowflake_rows}_ROWS"
    
    except Exception as e:
        # Send CRITICAL alert for any unhandled exception
        send_critical_alert(
            f"Pipeline failed with unhandled exception",
            context={"run_id": run_id, "error": str(e)}
        )
        
        processing_time = round(time.time() - pipeline_start, 2)
        print(f"\n{'='*66}")
        print(f"  FX PIPELINE FAILED - run_id={run_id}")
        print(f"  Error: {str(e)}")
        print(f"  Duration: {processing_time}s")
        print(f"{'='*66}\n")
        
        # Re-raise for Airflow to catch
        raise