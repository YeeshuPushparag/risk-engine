"""
equity_risk_prediction_pipeline.py
====================================
Production-grade equity risk pipeline — consistency-first architecture.

Storage architecture
--------------------
Snowflake EQUITY_HISTORY  (append-only, audit trail)
    - Always INSERT; never DELETE or UPDATE
    - Includes run_mode column for lineage tracing
    - Duplicates across runs are EXPECTED and intentional

Snowflake EQUITY          (clean, deterministic latest state)
    - Replay / Backfill:  DELETE window -> INSERT fresh rows (transactional)
    - Incremental:        MERGE (upsert on ticker + date)
    - Must NEVER contain conflicting rows for same (ticker, date)

Postgres equity_data      (serving layer — NOT source of truth)
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

DAG integration
---------------
master_dag.py passes start_date_override and replay to
run_equity_risk_pipeline(). Signature matches exactly.
"""

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
# CONSTANTS
# =========================
Z_95 = norm.ppf(0.05)
Z_99 = norm.ppf(0.01)
ALPHA_95 = 0.95
ALPHA_99 = 0.99

MIN_VOLATILITY = 0.01
MIN_VaR_95     = 0.005
MIN_VaR_99     = 0.01
MIN_CVaR_95    = 0.007
MIN_CVaR_99    = 0.015

S3_BUCKET = "yeeshu-equity-bucket"
s3 = boto3.client("s3")

# Snowflake table names — single source of truth for table identifiers
SNOWFLAKE_CLEAN_TABLE   = "EQUITY"           # deterministic latest-state table
SNOWFLAKE_HISTORY_TABLE = "EQUITY_HISTORY"   # append-only audit/lineage table


# =========================
# PRODUCTION-GRADE ALERTING (CRITICAL ONLY)
# =========================
def send_critical_alert(message: str, context: dict = None):
    """
    Send CRITICAL alert to Slack only for unrecoverable pipeline failures.

    Triggers for:
    - Any Snowflake write failure (HISTORY or CLEAN)
    - Any Postgres write failure
    - Complete pipeline failures
    - Data validation failures that prevent writes

    Does NOT trigger for:
    - Retry attempts (only on final failure after all retries exhausted)
    - Model failures (soft-fail: prediction columns set to NaN)
    - Missing macro data (pipeline continues without it)
    """
    payload = {
        "level":    "CRITICAL",
        "pipeline": "equity_risk_prediction_pipeline",
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
            run_id = context.get("run_id", "unknown") if context else "unknown"
            text = (
                f"*[CRITICAL]* equity_risk_prediction_pipeline\n"
                f"{truncated}\n"
                f"run_id: {run_id}"
            )
            requests.post(webhook, json={"text": text}, timeout=3)
        except Exception as e:
            print(f"[ALERT ERROR] Slack notification failed: {e}")


# =========================
# RETRY WITH EXPONENTIAL BACKOFF
# =========================
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

    Sends a CRITICAL alert only when all retries are exhausted AND
    critical_name is provided. Individual retry attempts are logged
    to stdout only.

    Args:
        func:           Zero-argument callable to retry.
        retries:        Maximum number of retry attempts (not counting the
                        first attempt).
        backoff_factor: Wait time multiplier: wait = backoff_factor ** attempt.
        exceptions:     Tuple of exception types to catch and retry.
        critical_name:  Human-readable operation name for alert messages.
                        Pass None for non-critical operations.
        run_id:         Pipeline run identifier for alert context.

    Returns:
        Return value of func() on success.

    Raises:
        The last caught exception when all retries are exhausted.
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
            f"historical-equity/features/"
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


def load_market_features_from_layer2(start_date, end_date):

    keys = get_latest_feature_keys(start_date, end_date)

    if not keys:
        return pd.DataFrame()

    frames = []

    for key in keys:

        print(f"  [LAYER2] Loading: {key}")

        obj = s3.get_object(
            Bucket=S3_BUCKET,
            Key=key,
        )

        df = pd.read_parquet(BytesIO(obj["Body"].read()))

        frames.append(df)

    market = pd.concat(frames, ignore_index=True)

    market["date"] = pd.to_datetime(
        market["date"],
        utc=True,
    )

    return market

# =========================
# FINANCIAL ENRICHMENT (UNCHANGED)
# =========================
def financial_enrichment(df):
    td = df["totalDebt"]
    ta = df["totalAssets"]

    df["interest_coverage_proxy"] = df["ebitda"] / (0.05 * td)
    df.loc[(td <= 0) | td.isna(), "interest_coverage_proxy"] = np.nan

    df["maturity_proxy"] = df["longTermDebt"] / td
    df.loc[(td <= 0) | td.isna(), "maturity_proxy"] = np.nan

    valid = (ta > 0) & (td > 0)

    wc_ta   = (ta - td) / ta
    re_ta   = df["netIncome"] / ta
    ebit_ta = (df["ebitda"] - 0.05 * td) / ta
    mve_tl  = df["marketCap"] / td
    s_ta    = df["revenue"] / ta

    df["Altman_Z"] = (
        1.2 * wc_ta +
        1.4 * re_ta +
        3.3 * ebit_ta +
        0.6 * mve_tl +
        1.0 * s_ta
    ).where(valid, np.nan)

    df["free_cash_flow"]       = df["ebitda"] - 0.10 * df["revenue"]
    df["cash_and_equivalents"] = df["totalAssets"] - df["totalDebt"] - 0.5 * df["revenue"]

    bins   = [-np.inf, 0.5, 0.8, 1.23, 1.8, 3.0, 4.2, 5.85, 7.0, 8.15, np.inf]
    labels = ["D", "C", "CC", "CCC", "B", "BB", "BBB", "A", "AA", "AAA"]
    df["credit_rating"] = pd.cut(df["Altman_Z"], bins=bins, labels=labels)

    df["mtm_value"] = df["close"] * df["Shares"]

    return df


# =========================
# RISK ENRICHMENT (UNCHANGED)
# =========================
def risk_enrichment(df):
    numeric_cols = [
        "total_value", "total_percent", "Shares", "volume", "mtm_value",
        "daily_return", "vol_5d", "vol_20d", "volatility_21d", "downside_risk",
        "sharpe_ratio", "sortino_ratio", "beta",
        "avg_volume_10d",
        "gdp", "unrate", "cpi", "fedfunds",
        "totalDebt", "shortTermDebt", "longTermDebt", "totalAssets",
        "ebitda", "revenue", "netIncome", "marketCap", "dividendYield",
        "Altman_Z", "free_cash_flow", "cash_and_equivalents",
    ]

    for c in numeric_cols:
        if c not in df.columns:
            df[c] = np.nan
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df["daily_pnl"] = df["mtm_value"] * df["daily_return"]

    total_mtm = df.groupby(["asset_manager", "date"])["mtm_value"].transform("sum")
    df["portfolio_weight"] = df["mtm_value"] / total_mtm
    df["portfolio_weight"] = df["portfolio_weight"].fillna(0)

    df["sector_exposure"]   = df.groupby(["asset_manager", "date", "sector"])["portfolio_weight"].transform("sum")
    df["industry_exposure"] = df.groupby(["asset_manager", "date", "industry"])["portfolio_weight"].transform("sum")

    df["beta"]              = df["beta"].fillna(0)
    df["beta_weighted"]     = df["portfolio_weight"] * df["beta"]
    df["sector_beta_weighted"] = df.groupby(["asset_manager", "date", "sector"])["beta_weighted"].transform("sum")

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

    g = df.groupby(["asset_manager", "date"])
    df["weighted_mu"]   = df["portfolio_weight"] * df["daily_mu"]
    port_mu             = g["weighted_mu"].transform("sum")
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

    df["sector_sum"]    = df.groupby(["asset_manager", "date", "sector"])["portfolio_weight"].transform("sum")
    df["sector_sum_sq"] = df["sector_sum"] ** 2
    df["HHI_sector"]    = g["sector_sum_sq"].transform("sum")

    df["diversification_score"] = 1 / df["HHI_sector"].replace(0, np.nan)

    df["avg_volume_10d"]   = df["avg_volume_10d"].fillna(0)
    df["Amihud_illiquidity"] = df["daily_return"].abs() / df["avg_volume_10d"].replace(0, np.nan)
    df["turnover_ratio"]   = df["volume"] / df["Shares"].replace(0, np.nan)

    df["liquidity_risk_score"] = df.groupby(["asset_manager", "date"])["Amihud_illiquidity"] \
        .transform(lambda x: (x - x.min()) / (x.max() - x.min()) if x.max() != x.min() else 0)

    df["Amihud_illiquidity"]   = df["Amihud_illiquidity"].fillna(0)
    df["liquidity_risk_score"] = df["liquidity_risk_score"].fillna(0)
    df["turnover_ratio"]       = df["turnover_ratio"].fillna(0)
    df["sector_exposure_pct"]  = df["sector_exposure"]

    df["portfolio_daily_pnl"] = g["daily_pnl"].transform("sum")

    return df


# =========================
# ML MODEL PREDICTIONS (UNCHANGED — SOFT FAIL)
# =========================
def run_all_equity_predictions(df):

 
    """
    Runs all equity prediction models.

    Flow:
    -----
    1. Generate transient inference-only features
    2. Load feature PKL + model from S3
    3. Predict
    4. Drop transient helper columns
    5. Return:
            original dataframe + prediction columns only
    """

    # =====================================================
    # MODEL REGISTRY
    # =====================================================

    model_map = {
        "ret_1d": (
            "models/equity_model_1d_xgb.json",
            "models/equity_features_1d.pkl",
            "pred_ret_1d"
        ),

        "ret_5d": (
            "models/equity_model_5d_xgb.json",
            "models/equity_features_5d.pkl",
            "pred_ret_5d"
        ),

        "ret_21d": (
            "models/equity_model_21d_xgb.json",
            "models/equity_features_21d.pkl",
            "pred_ret_21d"
        ),

        "vol_21d": (
            "models/equity_model_vol21_xgb.json",
            "models/equity_features_vol21.pkl",
            "pred_vol_21d"
        ),

        "down_21d": (
            "models/equity_model_down21_xgb.json",
            "models/equity_features_down21.pkl",
            "pred_downside_21d"
        ),

        "var_21d": (
            "models/equity_model_var21_xgb.json",
            "models/equity_features_var21.pkl",
            "pred_var_21d"
        ),

        "factor_21d": (
            "models/equity_model_factor21_xgb.json",
            "models/equity_features_factor21.pkl",
            "pred_factor_21d"
        ),

        "sector_rotation": (
            "models/equity_model_sector_rotation_xgb.json",
            "models/equity_features_sector_rotation.pkl",
            "pred_sector_rotation"
        ),

        "macro_regime": (
            "models/equity_model_macro_regime_xgb.json",
            "models/equity_features_macro_regime.pkl",
            "pred_macro_regime"
        ),

        "port_ret_1d": (
            "models/equity_model_portfolio1d_xgb.json",
            "models/equity_features_portfolio1d.pkl",
            "pred_port_ret_1d"
        ),

        "port_ret_5d": (
            "models/equity_model_portfolio5d_xgb.json",
            "models/equity_features_portfolio5d.pkl",
            "pred_port_ret_5d"
        ),

        "port_ret_21d": (
            "models/equity_model_portfolio21d_xgb.json",
            "models/equity_features_portfolio21d.pkl",
            "pred_port_ret_21d"
        ),

        "port_var_1d": (
            "models/equity_model_portfolio_var_1d_xgb.json",
            "models/equity_features_portfolio_var_1d.pkl",
            "pred_port_var_1d"
        ),

        "port_var_5d": (
            "models/equity_model_portfolio_var_5d_xgb.json",
            "models/equity_features_portfolio_var_5d.pkl",
            "pred_port_var_5d"
        ),

        "port_var_21d": (
            "models/equity_model_portfolio_var_21d_xgb.json",
            "models/equity_features_portfolio_var_21d.pkl",
            "pred_port_var_21d"
        ),
    }

    # =====================================================
    # SORT
    # =====================================================

    df = (
        df
        .sort_values(
            ["asset_manager", "ticker", "date"]
        )
        .copy()
    )

    # =====================================================
    # INFERENCE-TIME TRANSIENT FEATURES
    # =====================================================

    # -----------------------------------------------------
    # PORTFOLIO LAG FEATURES
    # -----------------------------------------------------

    for N in [1, 5, 21]:

        df[f"lag_ret_{N}d"] = (
            df.groupby(
                ["asset_manager", "ticker"]
            )["close"]
            .pct_change(N)
        )

        df[f"lag_port_ret_contrib_{N}d"] = (
            df["portfolio_weight"] *
            df[f"lag_ret_{N}d"]
        )

    # -----------------------------------------------------
    # FACTOR FEATURES
    # -----------------------------------------------------

    df["mkt_fwd_21d"] = (
        df.groupby("date")["daily_return"]
        .transform("mean")
    )

    # -----------------------------------------------------
    # SECTOR FEATURES
    # MUST MATCH TRAINING LOGIC EXACTLY
    # -----------------------------------------------------

    df["sector_avg_return"] = (
        df.groupby("sector")["daily_return"]
        .transform("mean")
    )

    df["sector_mom20"] = (
        df.groupby("sector")["sector_avg_return"]
        .transform(
            lambda s: s.pct_change(20)
        )
    )

    df["sector_vol21"] = (
        df.groupby("sector")["sector_avg_return"]
        .transform(
            lambda s: s.rolling(21).std()
        )
    )

    df["sector_turnover"] = (
        df.groupby("sector")["turnover_ratio"]
        .transform("mean")
    )

    # -----------------------------------------------------
    # MACRO TREND FEATURES
    # -----------------------------------------------------

    for col in [
        "gdp",
        "cpi",
        "unrate",
        "fedfunds"
    ]:

        if col in df.columns:

            df[f"{col}_trend"] = (
                df[col]
                .diff(63)
            )

    # =====================================================
    # CLEANUP
    # =====================================================

    df = df.replace(
        [np.inf, -np.inf],
        np.nan
    )

    failed_models = []

    # =====================================================
    # MODEL LOOP
    # =====================================================

    for name, (
        model_s3_path,
        feat_path,
        out_col
    ) in model_map.items():

        print(f"\n[MODEL] Running {name}")

        # -------------------------------------------------
        # LOAD FEATURES
        # -------------------------------------------------

        def load_features():

            obj = s3.get_object(
                Bucket=S3_BUCKET,
                Key=feat_path
            )

            return joblib.load(
                BytesIO(
                    obj["Body"].read()
                )
            )

        try:

            features = retry_with_backoff(
                load_features,
                retries=2
            )

        except Exception as e:

            print(
                f"[WARNING] "
                f"Feature load failed for {name}: {e}"
            )

            df[out_col] = np.nan

            failed_models.append(name)

            continue

        # -------------------------------------------------
        # ENSURE INFERENCE FEATURE SCHEMA
        # -------------------------------------------------

        missing = [
            c for c in features
            if c not in df.columns
        ]

        if missing:

            print(
                f"[WARNING] "
                f"{name} missing features: "
                f"{missing[:5]}"
            )

            for col in missing:
                df[col] = 0

        # -------------------------------------------------
        # NULL HANDLING
        # -------------------------------------------------

        for col in features:

            if col in df.columns:

                df[col] = (
                    df[col]
                    .replace(
                        [np.inf, -np.inf],
                        np.nan
                    )
                    .fillna(0)
                )

        X = df[features].copy()

        # -------------------------------------------------
        # LOAD MODEL
        # -------------------------------------------------

        def load_model():

            obj = s3.get_object(
                Bucket=S3_BUCKET,
                Key=model_s3_path
            )

            booster = xgb.Booster()

            booster.load_model(
                bytearray(
                    obj["Body"].read()
                )
            )

            return booster

        try:

            booster = retry_with_backoff(
                load_model,
                retries=2
            )

        except Exception as e:

            print(
                f"[WARNING] "
                f"Model load failed for {name}: {e}"
            )

            df[out_col] = np.nan

            failed_models.append(name)

            continue

        # -------------------------------------------------
        # PREDICT
        # -------------------------------------------------

        try:

            dtest = xgb.DMatrix(X)

            pred = booster.predict(dtest)

            if name == "macro_regime":

                df[out_col] = pred.argmax(axis=1)

            else:

                df[out_col] = pred

            print(f"[SUCCESS] {name} completed")

        except Exception as e:

            print(
                f"[WARNING] "
                f"Prediction failed for {name}: {e}"
            )

            df[out_col] = np.nan

            failed_models.append(name)

            continue

    # =====================================================
    # DROP TRANSIENT HELPER FEATURES
    # =====================================================

    helper_cols = [

        # lag features
        "lag_ret_1d",
        "lag_ret_5d",
        "lag_ret_21d",

        "lag_port_ret_contrib_1d",
        "lag_port_ret_contrib_5d",
        "lag_port_ret_contrib_21d",

        # factor
        "mkt_fwd_21d",

        # sector
        "sector_avg_return",
        "sector_mom20",
        "sector_vol21",
        "sector_turnover",

        # macro trends
        "gdp_trend",
        "cpi_trend",
        "unrate_trend",
        "fedfunds_trend",
    ]

    df = df.drop(
        columns=[
            c for c in helper_cols
            if c in df.columns
        ],
        errors="ignore"
    )

    # =====================================================
    # FINAL WARNINGS
    # =====================================================

    if failed_models:

        print(
            f"\n[WARNING] "
            f"{len(failed_models)} models failed: "
            f"{failed_models}"
        )

    return df

# =========================
# METADATA HELPERS (UNCHANGED)
# =========================
def rename_source_lineage(df):
    rename_map = {
        "pipeline_name":    "source_pipeline",
        "pipeline_run_id":  "source_run_id",
        "data_source":      "source_data_source",
        "input_source":     "source_input_source",
        "transformation":   "source_transformation",
        "record_created_at":"source_created_at",
        "feature_version":  "source_feature_version",
        "schema_version":   "source_schema_version",
        "schema_hash":      "source_schema_hash",
    }
    existing = {k: v for k, v in rename_map.items() if k in df.columns}
    return df.rename(columns=existing)


def drop_old_pipeline_metrics(df):
    drop_cols = [
    'data_date',
    'outlier_flag',
    'record_id',
    'ingestion_start_date',
    'ticker_universe_size',
    'input_rows',
    'output_rows',
    'processing_time_s',
    'replay_mode',
    'partial_run',
    'run_mode',
    'ingestion_ts'
    ]
    existing = [c for c in drop_cols if c in df.columns]
    return df.drop(columns=existing)


def add_current_pipeline_metadata(df):
    now = datetime.utcnow()
    df["pipeline_name"]    = "equity_risk_prediction_pipeline"
    df["pipeline_run_id"]  = now.isoformat()
    df["data_source"]      = "s3_market_features"
    df["input_source"]     = "market_features + mtm + financial_enrichment + risk_enrichment"
    df["transformation"]   = "equity_risk_v1"
    df["record_created_at"] = now
    return df


def drop_all_metadata_for_serving(df):
    """Remove all pipeline lineage columns before writing to the Postgres serving layer."""
    drop_cols = [
        "source_pipeline", "source_run_id", "source_data_source",
        "source_input_source", "source_transformation", "source_created_at",
        "source_feature_version", "source_schema_version", "source_schema_hash",
        "pipeline_name", "pipeline_run_id", "data_source", "input_source",
        "transformation", "record_created_at",
    ]
    existing = [c for c in drop_cols if c in df.columns]
    return df.drop(columns=existing)


# =========================
# SNOWFLAKE — HISTORY TABLE (append-only, always INSERT)
# =========================
def write_to_snowflake_history(df, run_mode, run_id=None, chunk_size=20_000):
    """
    Append rows to EQUITY_HISTORY. Never updates or deletes existing rows.

    Adds a run_mode column so every row is traceable to the pipeline run
    that produced it. Duplicates across runs are expected by design —
    this table is an immutable audit trail.

    Raises on failure (after retries). Pipeline does NOT continue if this
    write fails.

    Args:
        df:        Fully enriched and transformed DataFrame.
        run_mode:  One of "incremental", "backfill", "replay".
        run_id:    Pipeline run identifier for alert context.
        chunk_size: Rows per write_pandas chunk.
    """
    df_hist           = df.copy()
    df_hist["run_mode"] = run_mode   # lineage column — history table only

    def do_insert():
        with get_snowflake_conn() as ctx:
            success, nchunks, nrows, _ = write_pandas(
                ctx,
                df_hist,
                SNOWFLAKE_HISTORY_TABLE,
                chunk_size=chunk_size,
                quote_identifiers=True,
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
            context={"run_id": run_id, "table": SNOWFLAKE_HISTORY_TABLE,
                     "rows": len(df), "error": str(exc)},
        )
        raise


# =========================
# SNOWFLAKE — CLEAN TABLE — DELETE + INSERT (replay / backfill)
# =========================
def _snowflake_clean_delete_insert(df, start_date, end_date, run_id=None, chunk_size=20_000):
    """
    Atomically replace a date window in the EQUITY (clean) table.

    Protocol:
      1. Load new rows into a temp staging table (outside transaction —
         COPY INTO is not transactional in Snowflake, but temp table is
         session-scoped so it cannot leak).
      2. Open a Snowflake transaction.
      3. DELETE existing rows for the window [start_date, end_date].
      4. INSERT all rows from the temp staging table.
      5. COMMIT on success; ROLLBACK on any failure.

    This guarantees the EQUITY table never holds conflicting rows for the
    same (ticker, date) after a replay or backfill.

    Args:
        df:         Fully processed DataFrame for the replay/backfill window.
        start_date: Inclusive window start (date or date-like string).
        end_date:   Inclusive window end (date or date-like string).
        run_id:     Pipeline run identifier for alert context.
        chunk_size: Rows per write_pandas chunk to the staging temp table.

    Raises:
        Exception propagated after ROLLBACK + CRITICAL alert.
    """
    start_str  = pd.Timestamp(start_date).strftime("%Y-%m-%d")
    end_str    = pd.Timestamp(end_date).strftime("%Y-%m-%d")
    temp_table = f"{SNOWFLAKE_CLEAN_TABLE}_STAGE_{int(time.time())}"

    try:
        with get_snowflake_conn() as ctx:
            with ctx.cursor() as cs:

                # Step 1 — staging table (session-scoped, auto-dropped on close)
                cs.execute(f"""
                    CREATE TEMPORARY TABLE {temp_table}
                    LIKE "{SNOWFLAKE_CLEAN_TABLE}"
                """)
                write_pandas(
                    ctx, df, temp_table,
                    chunk_size=chunk_size,
                    quote_identifiers=True,
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
                    cs.execute(f"""
                        INSERT INTO "{SNOWFLAKE_CLEAN_TABLE}"
                        SELECT * FROM {temp_table}
                    """)
                    
                    inserted = cs.rowcount if cs.rowcount is not None else len(df)
                    
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


# =========================
# SNOWFLAKE — CLEAN TABLE — MERGE (incremental)
# =========================
def _snowflake_clean_merge(df, run_id=None, chunk_size=20_000):
    """
    Upsert rows into the EQUITY clean table using a Snowflake MERGE.

    Used for incremental runs only.

    Business uniqueness:
        asset_manager + ticker + date

    For each unique business key:
        - If the row already exists: UPDATE all non-key columns.
        - If the row is new:         INSERT.

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
        "asset_manager",
        "ticker",
        "date",
    ]

    temp_table = (
        f"{SNOWFLAKE_CLEAN_TABLE}_STAGE_"
        f"{int(time.time())}"
    )

    dupes = (
        df.groupby(["asset_manager", "ticker", "date"])
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
                )

                # =====================================================
                # FETCH COLUMN ORDER
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


# =========================
# SNOWFLAKE — UNIFIED WRITE DISPATCHER
# =========================
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


# =========================
# POSTGRES — SERVING LAYER WRITE (consistency-first: RAISES on failure)
# =========================

# Column rename map: Python/Snowflake naming -> Postgres snake_case convention
_POSTGRES_COLUMN_MAP = {
    "class":         "class_name",
    "CUSIP":         "cusip",
    "Shares":        "shares",
    "Discretion":    "discretion",
    "Sole":          "sole",
    "Shared":        "shared",
    "None":          "none_ownership",
    "totalAssets":   "total_assets",
    "totalDebt":     "total_debt",
    "shortTermDebt": "short_term_debt",
    "longTermDebt":  "long_term_debt",
    "netIncome":     "net_income",
    "marketCap":     "market_cap",
    "dividendYield": "dividend_yield",
    "Altman_Z":      "altman_z",
    "daily_VaR_95":           "daily_var_95",
    "daily_VaR_99":           "daily_var_99",
    "daily_CVaR_95":          "daily_cvar_95",
    "daily_CVaR_99":          "daily_cvar_99",
    "daily_portfolio_VaR_95": "daily_portfolio_var_95",
    "daily_portfolio_VaR_99": "daily_portfolio_var_99",
    "daily_portfolio_CVaR_95":"daily_portfolio_cvar_95",
    "daily_portfolio_CVaR_99":"daily_portfolio_cvar_99",
    "HHI_sector":             "hhi_sector",
    "Amihud_illiquidity":     "amihud_illiquidity",
}

# Columns that must be stored as integers in Postgres
_PG_INTEGER_COLS = [
    "volume", "other_manager", "sole", "shared",
    "none_ownership", "pred_macro_regime",
]


def write_to_postgres(df, mode, start_date, end_date, retries=3):
    """
    Write enriched equity rows to the Postgres serving layer.

    Steps (executed inside a single transaction per attempt):
      1. Replay / Backfill only:
            DELETE FROM equity_data WHERE date BETWEEN start_date AND end_date
         This purges stale data before inserting the recomputed window.

      2. INSERT new rows via COPY (batched, most efficient Postgres bulk load).

      3. Trim to last 2 calendar days:
            DELETE FROM equity_data
            WHERE date < (SELECT MAX(date) FROM equity_data) - INTERVAL '2 days'
         Postgres is a UI serving layer; it only needs recent data.

    All three steps are committed together. If any step fails, the transaction
    is not committed (connection close triggers implicit rollback via psycopg).

    RAISES on final failure — pipeline does NOT continue if Postgres fails.
    This enforces the consistency-first contract: both Snowflake AND Postgres
    must succeed for the pipeline run to be considered successful.

    Args:
        df:         Fully processed DataFrame (with all enrichment + predictions).
        mode:       "incremental" | "backfill" | "replay".
        start_date: Window start (used for DELETE in replay/backfill).
        end_date:   Window end (used for DELETE in replay/backfill).
        retries:    Maximum retry attempts after first failure.

    Raises:
        RuntimeError: When all retry attempts are exhausted.
    """
    if df.empty:
        print("  [POSTGRES] Empty DataFrame — nothing to write.")
        return

    start_str = pd.Timestamp(start_date).strftime("%Y-%m-%d")
    end_str   = pd.Timestamp(end_date).strftime("%Y-%m-%d")

    # Prepare the serving-layer DataFrame: strip metadata, rename columns
    df_pg = drop_all_metadata_for_serving(df.copy())
    df_pg = df_pg.rename(columns=_POSTGRES_COLUMN_MAP)

    last_error = None

    for attempt in range(retries + 1):
        try:
            with get_postgre_conn() as pg_conn:
                with pg_conn.cursor() as pg_cur:

                    # ── Step 1: delete stale window (replay / backfill only) ──
                    if mode in ("replay", "backfill"):
                        pg_cur.execute(
                            """
                            DELETE FROM public.equity_data
                            WHERE date BETWEEN %s::DATE AND %s::DATE
                            """,
                            (start_str, end_str),
                        )
                        deleted = pg_cur.rowcount if pg_cur.rowcount is not None else 0
                        print(
                            f"  [POSTGRES] Deleted {deleted:,} rows for "
                            f"window [{start_str}, {end_str}]"
                        )

                    # ── Step 2: validate schema against live DB ──
                    pg_cur.execute(
                        """
                        SELECT column_name
                        FROM   information_schema.columns
                        WHERE  table_schema = 'public'
                          AND  table_name   = 'equity_data'
                        ORDER  BY ordinal_position
                        """
                    )
                    pg_cols_order = [
                        row[0] for row in pg_cur.fetchall()
                        if row[0].lower() != "id"
                    ]

                    if not pg_cols_order:
                        raise RuntimeError(
                            "Schema query returned 0 columns for public.equity_data"
                        )

                    missing = set(pg_cols_order) - set(df_pg.columns)
                    if missing:
                        raise ValueError(
                            f"DataFrame is missing Postgres columns: {missing}"
                        )

                    # ── Step 3: cast integer columns ──
                    for col in _PG_INTEGER_COLS:
                        if col in df_pg.columns:
                            df_pg[col] = (
                                df_pg[col]
                                .fillna(0)
                                .astype(float)
                                .round()
                                .astype(int)
                            )

                    # Enforce exact Postgres column order for COPY
                    df_pg = df_pg[pg_cols_order]

                    # ── Step 4: bulk INSERT via COPY (batched) ──
                    BATCH_SIZE = 100_000
                    copy_sql   = (
                        f"COPY public.equity_data ({', '.join(pg_cols_order)}) "
                        f"FROM STDIN WITH CSV"
                    )

                    total_rows = 0
                    for start_idx in range(0, len(df_pg), BATCH_SIZE):
                        chunk = df_pg.iloc[start_idx : start_idx + BATCH_SIZE]
                        buf   = StringIO()
                        chunk.to_csv(buf, index=False, header=False)
                        buf.seek(0)

                        with pg_cur.copy(copy_sql) as copy:
                            copy.write(buf.getvalue())

                        total_rows += len(chunk)

                    # ── Step 5: trim to last 2 calendar days ──
                    pg_cur.execute(
                        """
                        DELETE FROM public.equity_data
                        WHERE date < (
                            SELECT MAX(date) FROM public.equity_data
                        ) - INTERVAL '2 days'
                        """
                    )
                    trimmed = pg_cur.rowcount if pg_cur.rowcount is not None else 0

                # Commit the entire transaction (delete + insert + trim)
                pg_conn.commit()

            print(
                f"  [POSTGRES] Success — inserted {total_rows:,} rows, "
                f"trimmed {trimmed:,} rows (keeping last 2 days)"
            )
            return  # success — exit the retry loop

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
        f"Postgres write failed after {retries} retries",
        context={"error": str(last_error), "rows": len(df_pg)},
    )
    raise RuntimeError(
        f"Postgres write to public.equity_data failed after {retries} retries: {last_error}"
    )


# =========================
# MAIN PIPELINE ENTRY POINT
# =========================
def run_equity_risk_pipeline(
    start_date_override: str  = None,
    replay:              bool = False,
):
    """
    Production-grade equity risk pipeline.

    Mode detection (derived from parameters ONLY — never from data columns):
    ┌─────────────────────────────────┬──────────────┐
    │ Condition                       │ mode         │
    ├─────────────────────────────────┼──────────────┤
    │ replay = True                   │ "replay"     │
    │ start_date_override provided    │ "backfill"   │
    │ default (neither)               │ "incremental"│
    └─────────────────────────────────┴──────────────┘

    Date filtering:
    ┌──────────────┬─────────────────────────────────────────────────────┐
    │ Mode         │ Filter applied to loaded market features            │
    ├──────────────┼─────────────────────────────────────────────────────┤
    │ replay       │ date BETWEEN start_date AND end_date (today)        │
    │ backfill     │ date BETWEEN start_date AND end_date (today)        │
    │ incremental  │ date > last_date_from_snowflake (EQUITY clean table)│
    └──────────────┴─────────────────────────────────────────────────────┘

    Snowflake writes (TWO TABLES):
    - EQUITY_HISTORY (append-only):  always INSERT, includes run_mode column
    - EQUITY (clean):                DELETE window + INSERT (replay/backfill)
                                     MERGE (incremental)

    Postgres writes (serving layer):
    - Replay/backfill:  DELETE window -> INSERT -> trim to last 2 days
    - Incremental:      INSERT -> trim to last 2 days

    Failure contract:
    - Pipeline fails if EITHER Snowflake write OR Postgres write fails.
    - Partial success is NOT allowed.
    - Model failures are soft-fail (NaN columns), never hard failures.

    Args:
        start_date_override: "YYYY-MM-DD" string. If provided and
                             replay is False -> backfill mode.
        replay:              True -> replay mode (deterministic re-run from
                             the S3 market features rolling file).

    Returns:
        str: Status string consumed by Airflow XCom
             ("NO_NEW_DATA" | "NO_NEW_MARKET_ROWS" | "NO_VALID_ROWS" |
              "UPDATED_ROWS_<n>")

    Raises:
        Any unhandled exception triggers a CRITICAL alert and re-raises for
        Airflow to catch and mark the task as failed.
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
    end_date = today   # processing window always ends at today

    print(f"\n{'=' * 66}")
    print(f"  EQUITY RISK PIPELINE START")
    print(f"  run_id            : {run_id}")
    print(f"  mode              : {mode}")
    print(f"  start_date_override: {start_date_override}")
    print(f"  replay            : {replay}")
    print(f"  end_date          : {end_date.date()}")
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
            # Watermark: last date successfully written to the EQUITY clean table
            def get_watermark():
                with get_snowflake_conn() as ctx:
                    with ctx.cursor() as cs:
                        cs.execute(f'SELECT MAX("date") FROM "{SNOWFLAKE_CLEAN_TABLE}"')
                        return cs.fetchone()[0]

            last_date = retry_with_backoff(
                get_watermark,
                retries=2,
                critical_name="Snowflake watermark query",
                run_id=run_id,
            )

            if last_date is not None:
                last_date = pd.Timestamp(last_date).normalize()
            else:
                last_date = pd.Timestamp("1970-01-01")

            if last_date.date() >= today.date():
                print("  [INCREMENTAL] Watermark is current — no new data to process.")
                return "NO_NEW_DATA"

            start_date = last_date  # filter will use date > last_date
            print(
                f"  [INCREMENTAL] Last processed date: {last_date.date()} "
                f"| Processing new rows with date > {last_date.date()}"
            )

        # ══════════════════════════════════════════════════════════════
        # STEP 2 — Load market features
        # replay/backfill -> Layer 2 versioned features
        # incremental     -> rolling serving layer
        # ══════════════════════════════════════════════════════════════

        def load_market_data():

            if mode in ("replay", "backfill"):

                market = load_market_features_from_layer2(
                    start_date=start_date,
                    end_date=end_date,
                )

            else:

                market_obj = s3.get_object(
                    Bucket=S3_BUCKET,
                    Key="historical-equity/rolling/market_features_30d.parquet",
                )

                market = pd.read_parquet(
                    BytesIO(market_obj["Body"].read())
                )

            market["date"] = pd.to_datetime(
                market["date"],
                utc=True,
            )

            return market


        market = retry_with_backoff(
            load_market_data,
            retries=3,
            critical_name="Market features S3 load",
            run_id=run_id,
        )

        # ══════════════════════════════════════════════════════════════
        # STEP 3 — Filter rows for the processing window
        # Filtering is driven purely by mode and the date window
        # derived from parameters — never from data-embedded flags.
        # ══════════════════════════════════════════════════════════════
        if mode in ("replay", "backfill"):
            # Inclusive window: start_date ≤ date ≤ end_date
            new_market_rows = market[
                (market["date"] >= start_date) &
                (market["date"] <= end_date)
            ].copy()
            print(
                f"  [{mode.upper()}] Filtered to window "
                f"[{start_date.date()}, {end_date.date()}]: "
                f"{len(new_market_rows):,} rows"
            )

        else:  # incremental
            # Exclusive lower bound: only rows strictly after the watermark
            new_market_rows = market[
                market["date"].dt.date > last_date.date()
            ].copy()
            print(
                f"  [INCREMENTAL] Filtered rows with date > {last_date.date()}: "
                f"{len(new_market_rows):,} rows"
            )

        if new_market_rows.empty:
            print("  No new market rows in the processing window — pipeline complete.")
            return "NO_NEW_MARKET_ROWS"

        # ══════════════════════════════════════════════════════════════
        # STEP 4 — Load and merge fundamentals
        # ══════════════════════════════════════════════════════════════
        def load_fundamentals():
            merged_obj = s3.get_object(
                Bucket=S3_BUCKET,
                Key="historical-equity/final_merged.parquet",
            )
            merged = pd.read_parquet(BytesIO(merged_obj["Body"].read()))
            if "date" in merged.columns:
                merged = merged.drop(columns=["date"])
            return merged

        merged = retry_with_backoff(
            load_fundamentals,
            retries=3,
            critical_name="Fundamentals S3 load",
            run_id=run_id,
        )

        df_new = pd.merge(new_market_rows, merged, on="ticker", how="inner")
        print(f"  Rows after fundamentals merge: {len(df_new):,}")

        # ══════════════════════════════════════════════════════════════
        # STEP 5 — Load macro data (non-critical: continues on failure)
        # ══════════════════════════════════════════════════════════════
        def load_macro_data():
            macro_obj = s3.get_object(
                Bucket=S3_BUCKET,
                Key="historical-equity/macro_data.csv",
            )
            macro         = pd.read_csv(
                StringIO(macro_obj["Body"].read().decode("utf-8"))
            )
            macro["month"] = pd.to_datetime(macro["date"], dayfirst=True).dt.to_period("M")
            macro          = macro.drop(columns=["date"])
            return macro

        try:
            macro_data = retry_with_backoff(load_macro_data, retries=2)
        except Exception as e:
            print(
                f"  [WARNING] Macro data load failed — continuing without "
                f"macro features: {e}"
            )
            macro_data = pd.DataFrame()

        df_new["month"] = df_new["date"].dt.to_period("M")

        if not macro_data.empty:
            df_merged = pd.merge(df_new, macro_data, on="month", how="inner")
        else:
            df_merged = df_new.copy()
            for col in ["gdp", "unrate", "cpi", "fedfunds"]:
                if col not in df_merged.columns:
                    df_merged[col] = np.nan

        df_merged = df_merged.drop(columns=["month"], errors="ignore")
        print(f"  Rows after macro merge: {len(df_merged):,}")

        # ══════════════════════════════════════════════════════════════
        # STEP 6 — Financial and risk enrichment (UNCHANGED logic)
        # ══════════════════════════════════════════════════════════════
        print("  Running financial enrichment...")
        df_merged = financial_enrichment(df_merged)

        print("  Running risk enrichment...")
        df_merged = risk_enrichment(df_merged)

        # ══════════════════════════════════════════════════════════════
        # STEP 7 — ML predictions (soft-fail: no pipeline crash on model errors)
        # ══════════════════════════════════════════════════════════════
        print("  Running ML model predictions...")
        df_merged = run_all_equity_predictions(df_merged)

        # ══════════════════════════════════════════════════════════════
        # STEP 8 — Final transformations and metadata
        # ══════════════════════════════════════════════════════════════
        df_merged["total_value"] = df_merged["total_value"] * 1000
        df_merged["date"]        = pd.to_datetime(
            df_merged["date"], dayfirst=True
        ).dt.date
        df_merged = df_merged.sort_values(["date", "ticker"])
        df_merged = rename_source_lineage(df_merged)
        df_merged = drop_old_pipeline_metrics(df_merged)
        df_merged = add_current_pipeline_metadata(df_merged)

        if df_merged.empty:
            print("  No valid rows after processing — pipeline complete.")
            return "NO_VALID_ROWS"

        final_row_count = len(df_merged)
        print(f"  Final row count: {final_row_count:,}")
        print(f"  Columns: {len(df_merged.columns)}")

        # ══════════════════════════════════════════════════════════════
        # STEP 9 — Write to Snowflake HISTORY (append-only, always INSERT)
        # Failure here -> pipeline FAILS.
        # ══════════════════════════════════════════════════════════════
        print(f"\n  Writing to Snowflake HISTORY table ({SNOWFLAKE_HISTORY_TABLE})...")
        write_to_snowflake_history(df_merged, run_mode=mode, run_id=run_id)

        # ══════════════════════════════════════════════════════════════
        # STEP 10 — Write to Snowflake CLEAN (deterministic latest state)
        # replay/backfill -> DELETE window + INSERT (transactional)
        # incremental     -> MERGE
        # Failure here -> pipeline FAILS.
        # ══════════════════════════════════════════════════════════════
        print(
            f"\n  Writing to Snowflake CLEAN table ({SNOWFLAKE_CLEAN_TABLE}) "
            f"using mode='{mode}'..."
        )
        write_to_snowflake_clean(
            df_merged,
            mode=mode,
            start_date=start_date,
            end_date=end_date,
            run_id=run_id,
        )

        # ══════════════════════════════════════════════════════════════
        # STEP 11 — Write to Postgres serving layer
        # replay/backfill -> DELETE window + INSERT + trim
        # incremental     -> INSERT + trim
        # Failure here -> pipeline FAILS (consistency-first).
        # ══════════════════════════════════════════════════════════════
        print(
            f"\n  Writing to Postgres serving layer (mode='{mode}')..."
        )
        write_to_postgres(
            df_merged,
            mode=mode,
            start_date=start_date,
            end_date=end_date,
        )

        # ══════════════════════════════════════════════════════════════
        # STEP 12 — Pipeline success
        # ══════════════════════════════════════════════════════════════
        processing_time = round(time.time() - pipeline_start, 2)

        print(f"\n{'=' * 66}")
        print(f"  PIPELINE SUCCESS")
        print(f"  run_id          : {run_id}")
        print(f"  mode            : {mode}")
        print(f"  rows processed  : {final_row_count:,}")
        print(f"  window          : {start_date if hasattr(start_date, 'date') else start_date} -> {end_date.date()}")
        print(f"  duration        : {processing_time}s")
        print(f"  Snowflake HISTORY: OK")
        print(f"  Snowflake CLEAN  : OK")
        print(f"  Postgres         : OK")
        print(f"{'=' * 66}\n")

        return f"UPDATED_ROWS_{final_row_count}"

    except Exception as exc:
        processing_time = round(time.time() - pipeline_start, 2)

        send_critical_alert(
            f"Pipeline failed with unhandled exception",
            context={
                "run_id":   run_id,
                "mode":     mode,
                "duration": f"{processing_time}s",
                "error":    str(exc),
            },
        )

        print(f"\n{'=' * 66}")
        print(f"  PIPELINE FAILED")
        print(f"  run_id   : {run_id}")
        print(f"  mode     : {mode}")
        print(f"  error    : {exc}")
        print(f"  duration : {processing_time}s")
        print(f"{'=' * 66}\n")

        # Re-raise so Airflow marks the task as FAILED
        raise