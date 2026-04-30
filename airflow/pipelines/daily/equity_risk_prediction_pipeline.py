import os
import time
import pandas as pd
import numpy as np
from scipy.stats import norm
from datetime import datetime
import xgboost as xgb
import joblib
import boto3
from io import BytesIO, StringIO
from connections.snowflake_conn import get_snowflake_conn
from connections.postgre_conn import get_postgre_conn
from snowflake.connector.pandas_tools import write_pandas
import requests
import json

# =========================
# CONSTANTS — UNCHANGED
# =========================
Z_95 = norm.ppf(0.05)
Z_99 = norm.ppf(0.01)
ALPHA_95 = 0.95
ALPHA_99 = 0.99

MIN_VOLATILITY = 0.01
MIN_VaR_95 = 0.005
MIN_VaR_99 = 0.01
MIN_CVaR_95 = 0.007
MIN_CVaR_99 = 0.015

S3_BUCKET = "pushparag-equity-bucket"
s3 = boto3.client("s3")

# =========================
# PRODUCTION-GRADE ALERTING (CRITICAL ONLY)
# =========================
def send_critical_alert(message: str, context: dict = None):
    """
    Send CRITICAL alert to Slack only for unrecoverable pipeline failures.
    
    Triggers only for:
    - Snowflake MERGE failures (system of record)
    - Complete pipeline failures
    - Data validation failures that prevent writes
    
    Does NOT send for:
    - Retry attempts (only final failure)
    - Postgres failures (non-critical)
    - Model failures (soft fail)
    - Missing macro data (continues anyway)
    """
    payload = {
        "level": "CRITICAL",
        "pipeline": "equity_risk_prediction_pipeline",
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
            text = f"*[CRITICAL]* equity_risk_prediction_pipeline\n" \
                   f"{truncated_msg}\n" \
                   f"run_id: {run_id}"
            requests.post(webhook, json={"text": text}, timeout=3)
        except Exception as e:
            print(f"[ALERT ERROR] Slack notification failed: {e}")

# =========================
# RETRY DECORATOR (PRODUCTION-GRADE)
# =========================
def retry_with_backoff(func, retries=3, backoff_factor=2, exceptions=(Exception,), 
                        critical_name=None, run_id=None):
    """
    Retry a function with exponential backoff.
    Only sends CRITICAL alert if all retries are exhausted.
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


# =========================
# SNOWFLAKE IDEMPOTENT WRITE (MERGE)
# =========================
def write_to_snowflake_idempotent(df, table_name, key_columns, run_id=None, chunk_size=20000):
    """
    Write to Snowflake with MERGE logic to prevent duplicates.
    Sends CRITICAL alert on failure.
    """
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
                
                # Step 3: Get column list (exclude auto-generated columns if any)
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
                
                # Step 6: Get counts
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


# =========================
# FINANCIAL ENRICHMENT
# =========================
def financial_enrichment(df):
    td = df["totalDebt"]
    ta = df["totalAssets"]

    df["interest_coverage_proxy"] = df["ebitda"] / (0.05 * td)
    df.loc[(td <= 0) | td.isna(), "interest_coverage_proxy"] = np.nan

    df["maturity_proxy"] = df["longTermDebt"] / td
    df.loc[(td <= 0) | td.isna(), "maturity_proxy"] = np.nan

    valid = (ta > 0) & (td > 0)

    wc_ta = (ta - td) / ta
    re_ta = df["netIncome"] / ta
    ebit_ta = (df["ebitda"] - 0.05 * td) / ta
    mve_tl = df["marketCap"] / td
    s_ta = df["revenue"] / ta

    df["Altman_Z"] = (
        1.2 * wc_ta +
        1.4 * re_ta +
        3.3 * ebit_ta +
        0.6 * mve_tl +
        1.0 * s_ta
    ).where(valid, np.nan)

    df["free_cash_flow"] = df["ebitda"] - 0.10 * df["revenue"]
    df["cash_and_equivalents"] = df["totalAssets"] - df["totalDebt"] - 0.5 * df["revenue"]

    bins = [-np.inf, 0.5, 0.8, 1.23, 1.8, 3.0, 4.2, 5.85, 7.0, 8.15, np.inf]
    labels = ["D", "C", "CC", "CCC", "B", "BB", "BBB", "A", "AA", "AAA"]
    df["credit_rating"] = pd.cut(df["Altman_Z"], bins=bins, labels=labels)

    df["mtm_value"] = df["close"] * df["Shares"]

    return df


# =========================
# RISK ENRICHMENT
# =========================
def risk_enrichment(df):
    numeric_cols = [
        "total_value","total_percent","Shares","volume","mtm_value",
        "daily_return","vol_5d","vol_20d","volatility_21d","downside_risk",
        "sharpe_ratio","sortino_ratio","beta",
        "avg_volume_10d",
        "gdp","unrate","cpi","fedfunds",
        "totalDebt","shortTermDebt","longTermDebt","totalAssets",
        "ebitda","revenue","netIncome","marketCap","dividendYield",
        "Altman_Z","free_cash_flow","cash_and_equivalents",
    ]

    for c in numeric_cols:
        if c not in df.columns:
            df[c] = np.nan
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df["daily_pnl"] = df["mtm_value"] * df["daily_return"]

    total_mtm = df.groupby(["asset_manager", "date"])["mtm_value"].transform("sum")
    df["portfolio_weight"] = df["mtm_value"] / total_mtm
    df["portfolio_weight"] = df["portfolio_weight"].fillna(0)

    df["sector_exposure"] = df.groupby(["asset_manager","date","sector"])["portfolio_weight"].transform("sum")
    df["industry_exposure"] = df.groupby(["asset_manager","date","industry"])["portfolio_weight"].transform("sum")

    df["beta"] = df["beta"].fillna(0)
    df["beta_weighted"] = df["portfolio_weight"] * df["beta"]
    df["sector_beta_weighted"] = df.groupby(["asset_manager","date","sector"])["beta_weighted"].transform("sum")

    df["daily_sigma"] = (
        df["volatility_21d"]
        .fillna(df["vol_20d"])
        .fillna(df["vol_5d"])
        .fillna(MIN_VOLATILITY)
    ).clip(lower=MIN_VOLATILITY)

    df["daily_mu"] = df["daily_return"].fillna(0)

    df["daily_VaR_95"] = (-(df["daily_mu"] + Z_95 * df["daily_sigma"])).clip(lower=MIN_VaR_95)
    df["daily_VaR_99"] = (-(df["daily_mu"] + Z_99 * df["daily_sigma"])).clip(lower=MIN_VaR_99)

    z95 = norm.ppf(1 - ALPHA_95)
    z99 = norm.ppf(1 - ALPHA_99)

    df["daily_CVaR_95"] = (
        -df["daily_mu"] + df["daily_sigma"] * (norm.pdf(z95) / ALPHA_95)
    ).clip(lower=MIN_CVaR_95)

    df["daily_CVaR_99"] = (
        -df["daily_mu"] + df["daily_sigma"] * (norm.pdf(z99) / ALPHA_99)
    ).clip(lower=MIN_CVaR_99)

    g = df.groupby(["asset_manager","date"])
    df["weighted_mu"] = df["portfolio_weight"] * df["daily_mu"]
    port_mu = g["weighted_mu"].transform("sum")
    df["daily_portfolio_mu"] = port_mu

    df["w_sigma2"] = (df["portfolio_weight"] * df["daily_sigma"]) ** 2
    port_vol = g["w_sigma2"].transform("sum").apply(np.sqrt).clip(lower=MIN_VOLATILITY)

    df["daily_portfolio_ex_ante_volatility"] = port_vol

    df["daily_portfolio_VaR_95"] = (-(port_mu + Z_95 * port_vol)).clip(lower=MIN_VaR_95)
    df["daily_portfolio_VaR_99"] = (-(port_mu + Z_99 * port_vol)).clip(lower=MIN_VaR_99)

    df["daily_portfolio_CVaR_95"] = (
        -port_mu + port_vol * (norm.pdf(z95) / ALPHA_95)
    ).clip(lower=MIN_CVaR_95)

    df["daily_portfolio_CVaR_99"] = (
        -port_mu + port_vol * (norm.pdf(z99) / ALPHA_99)
    ).clip(lower=MIN_CVaR_99)

    df["top_5_exposure"] = g["portfolio_weight"].transform(lambda s: s.nlargest(5).sum())

    df["sector_sum"] = df.groupby(["asset_manager","date","sector"])["portfolio_weight"].transform("sum")
    df["sector_sum_sq"] = df["sector_sum"] ** 2
    df["HHI_sector"] = g["sector_sum_sq"].transform("sum")

    df["diversification_score"] = 1 / df["HHI_sector"].replace(0, np.nan)

    # Liquidity
    df["avg_volume_10d"] = df["avg_volume_10d"].fillna(0)
    df["Amihud_illiquidity"] = df["daily_return"].abs() / df["avg_volume_10d"].replace(0, np.nan)

    df["turnover_ratio"] = df["volume"] / df["Shares"].replace(0, np.nan)

    # Liquidity risk score (minmax)
    df["liquidity_risk_score"] = df.groupby(["asset_manager","date"])["Amihud_illiquidity"] \
        .transform(lambda x: (x - x.min()) / (x.max() - x.min()) if x.max() != x.min() else 0)
    df["Amihud_illiquidity"] = df["Amihud_illiquidity"].fillna(0)
    df["liquidity_risk_score"] = df["liquidity_risk_score"].fillna(0)
    df["turnover_ratio"] = df["turnover_ratio"].fillna(0)
    df["sector_exposure_pct"] = df["sector_exposure"]

    # Portfolio daily pnl
    df["portfolio_daily_pnl"] = g["daily_pnl"].transform("sum")

    return df


# =========================
# RUN ALL MODELS — WITH SAFE HANDLING (NO HARD FAIL)
# =========================
def run_all_equity_predictions(df):
    model_map = {
        "ret_1d": ("models/equity_model_1d_xgb.json", "models/equity_features_1d.pkl", "pred_ret_1d"),
        "ret_5d": ("models/equity_model_5d_xgb.json", "models/equity_features_5d.pkl", "pred_ret_5d"),
        "ret_21d": ("models/equity_model_21d_xgb.json", "models/equity_features_21d.pkl", "pred_ret_21d"),
        "vol_21d": ("models/equity_model_vol21_xgb.json", "models/equity_features_vol21.pkl", "pred_vol_21d"),
        "down_21d": ("models/equity_model_down21_xgb.json", "models/equity_features_down21.pkl", "pred_downside_21d"),
        "var_21d": ("models/equity_model_var21_xgb.json", "models/equity_features_var21.pkl", "pred_var_21d"),
        "factor_21d": ("models/equity_model_factor21_xgb.json", "models/equity_features_factor21.pkl", "pred_factor_21d"),
        "sector_rotation": ("models/equity_model_sector_rotation_xgb.json", "models/equity_features_sector_rotation.pkl", "pred_sector_rotation"),
        "macro_regime": ("models/equity_model_macro_regime_xgb.json", "models/equity_features_macro_regime.pkl", "pred_macro_regime"),
        "port_ret_1d": ("models/equity_model_portfolio1d_xgb.json", "models/equity_features_portfolio1d.pkl", "pred_port_ret_1d"),
        "port_ret_5d": ("models/equity_model_portfolio5d_xgb.json", "models/equity_features_portfolio5d.pkl", "pred_port_ret_5d"),
        "port_ret_21d": ("models/equity_model_portfolio21d_xgb.json", "models/equity_features_portfolio21d.pkl", "pred_port_ret_21d"),
        "port_var_1d": ("models/equity_model_portfolio_var_1d_xgb.json", "models/equity_features_portfolio_var_1d.pkl", "pred_port_var_1d"),
        "port_var_5d": ("models/equity_model_portfolio_var_5d_xgb.json", "models/equity_features_portfolio_var_5d.pkl", "pred_port_var_5d"),
        "port_var_21d": ("models/equity_model_portfolio_var_21d_xgb.json", "models/equity_features_portfolio_var_21d.pkl", "pred_port_var_21d")
    }

    # Clean obvious bad values
    df = df.replace([np.inf, -np.inf], np.nan)
    
    # Track failed models for logging
    failed_models = []

    for name, (model_s3_path, feat_path, out_col) in model_map.items():
        print(f"\n[MODEL] Running {name}")

        # =========================
        # 1. LOAD FEATURE LIST (with retry - NOT CRITICAL)
        # =========================
        def load_features():
            feat_obj = s3.get_object(Bucket=S3_BUCKET, Key=feat_path)
            return joblib.load(BytesIO(feat_obj["Body"].read()))
        
        try:
            features = retry_with_backoff(load_features, retries=2)
        except Exception as e:
            print(f"[WARNING] Feature load failed for {name}: {e}")
            df[out_col] = np.nan
            failed_models.append(name)
            continue

        # =========================
        # 2. FEATURE VALIDATION (LOG, DON'T FAIL)
        # =========================
        missing = [f for f in features if f not in df.columns]
        if missing:
            print(f"[WARNING] Model {name} missing features: {missing[:5]}... (using NaN)")
            for col in missing:
                df[col] = np.nan

        # Check NULLs and fill with safe defaults
        null_cols = [c for c in features if c in df.columns and df[c].isna().any()]
        if null_cols:
            print(f"[WARNING] Model {name} has nulls in: {null_cols[:5]}... (filling with 0)")
            for col in null_cols:
                df[col] = df[col].fillna(0)

        # =========================
        # 3. PREPARE INPUT (SAFE)
        # =========================
        available_features = [f for f in features if f in df.columns]
        X = df[available_features].copy()
        X = X.fillna(0)

        # =========================
        # 4. LOAD MODEL (with retry - NOT CRITICAL)
        # =========================
        def load_model():
            model_obj = s3.get_object(Bucket=S3_BUCKET, Key=model_s3_path)
            booster = xgb.Booster()
            booster.load_model(bytearray(model_obj["Body"].read()))
            return booster
        
        try:
            booster = retry_with_backoff(load_model, retries=2)
        except Exception as e:
            print(f"[WARNING] Model load failed for {name}: {e}")
            df[out_col] = np.nan
            failed_models.append(name)
            continue

        # =========================
        # 5. PREDICT (SAFE)
        # =========================
        try:
            dtest = xgb.DMatrix(X)
            raw_pred = booster.predict(dtest)

            if name == "macro_regime":
                df[out_col] = raw_pred.argmax(axis=1)
            else:
                df[out_col] = raw_pred

            print(f"[SUCCESS] {name} completed")

        except Exception as e:
            print(f"[WARNING] Prediction failed for {name}: {e}")
            df[out_col] = np.nan
            failed_models.append(name)
            continue

    if failed_models:
        print(f"\n[WARNING] {len(failed_models)} models failed: {failed_models}")
    
    return df


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
        "ticker_universe_size",
        "input_rows",
        "output_rows",
        "processing_time_s",
        "replay_mode",
        "partial_run",
    ]

    existing = [c for c in drop_cols if c in df.columns]
    return df.drop(columns=existing)


def add_current_pipeline_metadata(df):
    now = datetime.utcnow()

    df["pipeline_name"] = "equity_risk_prediction_pipeline"
    df["pipeline_run_id"] = now.isoformat()
    df["data_source"] = "s3_market_features"
    df["input_source"] = "market_features + mtm + financial_enrichment + risk_enrichment"
    df["transformation"] = "equity_risk_v1"
    df["record_created_at"] = now

    return df


def drop_all_metadata_for_serving(df):
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

    existing = [c for c in drop_cols if c in df.columns]
    return df.drop(columns=existing)


# =========================
# POSTGRES WRITE WITH RETRY (NO ALERTS - NON-CRITICAL)
# =========================
def write_to_postgres_with_retry(df, retries=3):
    """
    Write to Postgres with retry logic.
    Returns success flag and error message if any.
    NO ALERTS - Postgres is serving layer, Snowflake is source of truth.
    """
    if df.empty:
        return True, "Empty DataFrame - nothing to write"
    
    POSTGRES_COLUMN_MAP = {
        "class": "class_name",
        "CUSIP": "cusip",
        "Shares": "shares",
        "Discretion": "discretion",
        "Sole": "sole",
        "Shared": "shared",
        "None": "none_ownership",
        "totalAssets": "total_assets",
        "totalDebt": "total_debt",
        "shortTermDebt": "short_term_debt",
        "longTermDebt": "long_term_debt",
        "netIncome": "net_income",
        "marketCap": "market_cap",
        "dividendYield": "dividend_yield",
        "Altman_Z": "altman_z",
        "daily_VaR_95": "daily_var_95",
        "daily_VaR_99": "daily_var_99",
        "daily_CVaR_95": "daily_cvar_95",
        "daily_CVaR_99": "daily_cvar_99",
        "daily_portfolio_VaR_95": "daily_portfolio_var_95",
        "daily_portfolio_VaR_99": "daily_portfolio_var_99",
        "daily_portfolio_CVaR_95": "daily_portfolio_cvar_95",
        "daily_portfolio_CVaR_99": "daily_portfolio_cvar_99",
        "HHI_sector": "hhi_sector",
        "Amihud_illiquidity": "amihud_illiquidity",
    }

    # Prepare data
    df_pg = drop_all_metadata_for_serving(df.copy())
    df_pg = df_pg.rename(columns=POSTGRES_COLUMN_MAP)
    
    last_error = None
    
    for attempt in range(retries + 1):
        try:
            with get_postgre_conn() as pg_conn:
                with pg_conn.cursor() as pg_cur:
                    # Fetch DB schema order
                    pg_cur.execute("""
                        SELECT column_name 
                        FROM information_schema.columns
                        WHERE table_schema='public'
                        AND table_name='equity_data'
                        ORDER BY ordinal_position
                    """)

                    pg_cols_order = [
                        row[0] for row in pg_cur.fetchall()
                        if row[0].lower() != "id"
                    ]

                    if not pg_cols_order:
                        raise RuntimeError("No columns found in Postgres table")

                    # Validate schema
                    missing = set(pg_cols_order) - set(df_pg.columns)
                    if missing:
                        raise ValueError(f"Missing columns: {missing}")

                    # Type casting for integer columns
                    integer_cols = [
                        "volume", "other_manager", "sole", "shared",
                        "none_ownership", "pred_macro_regime"
                    ]

                    for col in integer_cols:
                        if col in df_pg.columns:
                            df_pg[col] = (
                                df_pg[col]
                                .fillna(0)
                                .astype(float)
                                .round()
                                .astype(int)
                            )

                    # Enforce exact schema order
                    df_pg = df_pg[pg_cols_order]

                    # COPY to Postgres
                    BATCH_SIZE = 100_000
                    copy_sql = f"COPY public.equity_data ({','.join(pg_cols_order)}) FROM STDIN WITH CSV"

                    for start in range(0, len(df_pg), BATCH_SIZE):
                        chunk = df_pg.iloc[start:start + BATCH_SIZE]
                        buf = StringIO()
                        chunk.to_csv(buf, index=False, header=False)
                        buf.seek(0)

                        with pg_cur.copy(copy_sql) as copy:
                            copy.write(buf.getvalue())

                pg_conn.commit()
            
            print(f"[POSTGRES] Uploaded {len(df_pg)} rows successfully")
            return True, None
            
        except Exception as e:
            last_error = e
            if attempt < retries:
                wait_time = 2 ** attempt
                print(f"  Postgres retry {attempt + 1}/{retries} after {wait_time}s: {e}")
                time.sleep(wait_time)
            else:
                print(f"  Postgres failed after {retries} retries: {e}")
    
    return False, str(last_error)


# =========================
# FINAL EXECUTION FOR DAG (WITH CRITICAL ALERTS ONLY)
# =========================
def run_equity_risk_pipeline():
    """
    Complete production-grade equity risk pipeline with:
    1. Retry on all external calls
    2. Idempotent Snowflake writes (MERGE, not append)
    3. Slack alerts ONLY for critical failures (Snowflake, pipeline crash)
    4. Partial success handling (Snowflake success doesn't require Postgres success)
    5. Safe model handling (WARNING, not HARD FAIL)
    """
    
    pipeline_start = time.time()
    run_id = datetime.utcnow().isoformat()
    snowflake_rows = 0
    postgres_success = False
    
    print(f"\n{'='*66}")
    print(f"  EQUITY RISK PIPELINE START - run_id={run_id}")
    print(f"{'='*66}\n")
    
    try:
        # =========================
        # STEP 1: Determine what dates need processing
        # =========================
        today = pd.Timestamp.today().normalize()

        # Get last date from Snowflake EQUITY table (with retry)
        def get_last_date():
            with get_snowflake_conn() as ctx:
                with ctx.cursor() as cs:
                    cs.execute('SELECT MAX("date") FROM "EQUITY"')
                    return cs.fetchone()[0]
        
        last_date = retry_with_backoff(get_last_date, retries=2, 
                                        critical_name="Snowflake watermark query", 
                                        run_id=run_id)
        
        if last_date is not None:
            last_date = pd.Timestamp(last_date).normalize()
        else:
            last_date = pd.Timestamp("1970-01-01")

        if last_date.date() >= today.date():
            print("  No new data - pipeline complete")
            return "NO_NEW_DATA"

        # =========================
        # STEP 2: Load market features (with retry - CRITICAL if fails)
        # =========================
        def load_market_data():
            market_obj = s3.get_object(Bucket=S3_BUCKET, Key="historical-equity/rolling/market_features_30d.parquet")
            market = pd.read_parquet(BytesIO(market_obj['Body'].read()))
            market["date"] = pd.to_datetime(market["date"], dayfirst=True)
            return market
        
        market = retry_with_backoff(load_market_data, retries=3, 
                                     critical_name="Market features S3 load", 
                                     run_id=run_id)
        
        new_market_rows = market[market["date"].dt.date > last_date.date()]
        if new_market_rows.empty:
            print("  No new market rows - pipeline complete")
            return "NO_NEW_MARKET_ROWS"
        
        print(f"  Loaded market rows: {len(new_market_rows)}")

        # =========================
        # STEP 3: Load fundamentals (with retry - CRITICAL if fails)
        # =========================
        def load_fundamentals():
            merged_obj = s3.get_object(Bucket=S3_BUCKET, Key="historical-equity/final_merged.parquet")
            merged = pd.read_parquet(BytesIO(merged_obj['Body'].read()))
            if "date" in merged.columns:
                merged = merged.drop(columns=["date"])
            return merged
        
        merged = retry_with_backoff(load_fundamentals, retries=3,
                                     critical_name="Fundamentals S3 load",
                                     run_id=run_id)
        
        df_new = pd.merge(new_market_rows, merged, on="ticker", how="inner")

        # =========================
        # STEP 4: Load macro data (with retry - NOT CRITICAL, continues without)
        # =========================
        def load_macro_data():
            macro_data_obj = s3.get_object(Bucket=S3_BUCKET, Key="historical-equity/macro_data.csv")
            macro_data = pd.read_csv(StringIO(macro_data_obj["Body"].read().decode("utf-8")))
            macro_data["month"] = pd.to_datetime(macro_data["date"], dayfirst=True).dt.to_period("M")
            macro_data = macro_data.drop(columns=["date"])
            return macro_data
        
        try:
            macro_data = retry_with_backoff(load_macro_data, retries=2)
        except Exception as e:
            print(f"  WARNING: Macro data load failed - continuing without: {e}")
            macro_data = pd.DataFrame()
        
        df_new["month"] = df_new["date"].dt.to_period("M")
        if not macro_data.empty:
            df_merged = pd.merge(df_new, macro_data, on="month", how="inner")
        else:
            df_merged = df_new.copy()
            for col in ["gdp", "unrate", "cpi", "fedfunds"]:
                if col not in df_merged.columns:
                    df_merged[col] = np.nan
        
        print(f"  Merged rows: {len(df_merged)}")
        df_merged = df_merged.drop(columns=["month"], errors="ignore")

        # =========================
        # STEP 5: Enrichments
        # =========================
        print("  Running financial enrichment...")
        df_merged = financial_enrichment(df_merged)
        
        print("  Running risk enrichment...")
        df_merged = risk_enrichment(df_merged)
        
        print(f"  Columns before model: {len(df_merged.columns)}")
        
        # =========================
        # STEP 6: Run ML models (safe - won't crash pipeline)
        # =========================
        print("  Running ML models...")
        df_merged = run_all_equity_predictions(df_merged)

        # =========================
        # STEP 7: Final transformations
        # =========================
        df_merged["total_value"] = df_merged["total_value"] * 1000
        df_merged["date"] = pd.to_datetime(df_merged["date"], dayfirst=True).dt.date
        df_merged = df_merged.sort_values(["date", "ticker"])
        df_merged = rename_source_lineage(df_merged)
        df_merged = drop_old_pipeline_metrics(df_merged)
        df_merged = add_current_pipeline_metadata(df_merged)
        
        if df_merged.empty:
            print("  No valid rows after processing")
            return "NO_VALID_ROWS"

        snowflake_rows = len(df_merged)
        print(f"  Final row count: {snowflake_rows}")

        # =========================
        # STEP 8: Write to Snowflake (IDEMPOTENT - MERGE) - CRITICAL if fails
        # =========================
        print("  Writing to Snowflake (idempotent MERGE)...")
        
        def do_snowflake_write():
            return write_to_snowflake_idempotent(
                df_merged, 
                "EQUITY", 
                key_columns=["ticker", "date"],
                run_id=run_id,
                chunk_size=20000
            )
        
        affected = retry_with_backoff(do_snowflake_write, retries=3,
                                       critical_name="Snowflake MERGE write",
                                       run_id=run_id)
        print(f"  Snowflake MERGE complete: {affected} rows affected")

        # =========================
        # STEP 9: Write to Postgres (WITH RETRY, non-critical - NO ALERTS)
        # =========================
        print("  Writing to Postgres...")
        postgres_ok, postgres_error = write_to_postgres_with_retry(df_merged, retries=3)
        postgres_success = postgres_ok
        
        if not postgres_ok:
            print(f"  WARNING: Postgres write failed: {postgres_error}")
            # Don't fail the pipeline - Snowflake is the source of truth
        
        # =========================
        # STEP 10: Success
        # =========================
        processing_time = round(time.time() - pipeline_start, 2)
        print(f"\n{'='*66}")
        print(f"  PIPELINE SUCCESS - run_id={run_id}")
        print(f"  Snowflake rows: {snowflake_rows}")
        print(f"  Postgres: {'SUCCESS' if postgres_success else 'FAILED'}")
        print(f"  Duration: {processing_time}s")
        print(f"{'='*66}\n")
        
        return f"UPDATED_ROWS_{snowflake_rows}"
        
    except Exception as e:
        # Send CRITICAL alert for any unhandled exception
        send_critical_alert(
            f"Pipeline failed with unhandled exception",
            context={"run_id": run_id, "error": str(e)}
        )
        
        processing_time = round(time.time() - pipeline_start, 2)
        print(f"\n{'='*66}")
        print(f"  PIPELINE FAILED - run_id={run_id}")
        print(f"  Error: {str(e)}")
        print(f"  Duration: {processing_time}s")
        print(f"{'='*66}\n")
        
        # Re-raise for Airflow to catch
        raise