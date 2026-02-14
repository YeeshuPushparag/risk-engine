import os
import pandas as pd
import numpy as np
import datetime
import joblib
import xgboost as xgb
import boto3
from io import BytesIO, StringIO
from connections.postgre_conn import get_postgre_conn
from connections.snowflake_conn import get_snowflake_conn
from snowflake.connector.pandas_tools import write_pandas





S3_BUCKET = "pushparag-fx-bucket"
s3 = boto3.client("s3")

# -----------------------------
# Constants
# -----------------------------
TRADING_DAYS = 252
BASE_HEDGE = 0.10
W_VOL = 0.75
W_CARRY = 0.15
Z_95, Z_99 = 1.65, 2.33
TRADING_DAYS_PER_Q = 63

# ============================================================
# FX Enrichment (same logic as original code)
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


# ============================================================
# FX Update Pipeline with S3 + Snowflake (Airflow-friendly)
# ============================================================
def update_fx_snowflake():
    """
    Updates the FX Snowflake table using enriched FX exposure data from S3
    and an XGBoost model for 21d volatility.
    Returns a status string for Airflow/XCom.
    """
    today = datetime.date.today()


    # -----------------------------
    # Get last date from Snowflake FX table
    # -----------------------------
    with get_snowflake_conn() as ctx:
        with ctx.cursor() as cs:
            cs.execute('SELECT MAX("date") FROM "FX"')
            last_date_sf = cs.fetchone()[0]

    if last_date_sf is not None:
        last_date_sf = pd.Timestamp(last_date_sf).date()
    else:
        last_date_sf = pd.Timestamp("1970-01-01").date()

    if last_date_sf >= today:
        return "NO_UPDATE_NEEDED"

    # -----------------------------
    # Load FX file from S3
    # -----------------------------
    fx_obj = s3.get_object(
        Bucket=S3_BUCKET,
        Key="historical-fx/fx_exposure_with_interest_diff.parquet",
    )
    fx = pd.read_parquet(BytesIO(fx_obj["Body"].read()))
    fx["date"] = pd.to_datetime(fx["date"]).dt.date

    # -----------------------------
    # Take last 41 rows per ticker (same as original logic)
    # -----------------------------
    fx_sorted = fx.sort_values(["ticker", "date"])

    fx_for_enrichment = (
        fx_sorted
        .groupby("ticker", group_keys=False)
        .apply(lambda x: x.tail(
            max(41, (x["date"] > last_date_sf).sum())
        ))
        .reset_index(drop=True)
    )

    # -----------------------------
    # FX enrichment
    # -----------------------------
    df_enriched = fx_enrichment(fx_for_enrichment)

    # Filter only new rows
    filtered_enriched_fx = df_enriched[df_enriched["date"] > last_date_sf]
    if filtered_enriched_fx.empty:
        print("No new FX rows to process.")
        return
    print("New FX rows to process:", len(filtered_enriched_fx))

    # -----------------------------
    # Load XGBoost model & features from S3
    # -----------------------------
    model_obj = s3.get_object(Bucket=S3_BUCKET, Key="models/fx_model_vol21_xgb.json")
    booster = xgb.Booster()
    booster.load_model(bytearray(model_obj["Body"].read()))

    feat_obj = s3.get_object(Bucket=S3_BUCKET, Key="models/fx_features_vol21.pkl")
    feature_cols = joblib.load(BytesIO(feat_obj["Body"].read()))

    # ---- CRITICAL FIX: force exact training feature order ----
    X = filtered_enriched_fx.reindex(columns=feature_cols).fillna(0)

    filtered_enriched_fx["predicted_volatility_21d"] = booster.predict(
        xgb.DMatrix(X)
    )


    # -----------------------------
    # Write enriched FX to Snowflake (filtered columns)
    # -----------------------------
    final_cols = [
        "ticker","sector","industry","currency_pair","foreign_revenue_ratio","date",
        "fx_rate","fx_return","fx_volatility_20d","fx_volatility_30d","fx_volatility",
        "interest_diff","carry_daily","return_carry_adj",
        "position_size","hedge_ratio","exposure_amount",
        "fx_pnl","carry_pnl","total_pnl","expected_pnl",
        "VaR_95","VaR_99","value_at_risk",
        "volume","sharpe_like_ratio","is_warmup",
        "gdp", "unrate", "fedfunds", "cpi",
        "predicted_volatility_21d"
    ]

    # Keep only columns that exist in df_enriched
    df_to_upload = filtered_enriched_fx[[c for c in final_cols if c in filtered_enriched_fx.columns]]
    # -----------------------------
    # Write enriched FX to Snowflake
    # -----------------------------
    with get_snowflake_conn() as ctx:
        success, nchunks, nrows, _ = write_pandas(
            ctx,
            df_to_upload,
            "FX",
            chunk_size=20000,
            quote_identifiers=True,
        )

    if not success:
        return "UPLOAD_FAILED"


    try:
        df_pg = df_to_upload.copy()
        

        integer_cols = ["volume", "position_size", "exposure_amount"]

        for col in integer_cols:
            if col in df_pg.columns:
                df_pg[col] = (
                    df_pg[col]
                    .fillna(0)
                    .astype(float)
                    .round()
                    .astype(int)
                )

        with get_postgre_conn() as pg_conn:

            with pg_conn.cursor() as pg_cur:

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

                df_pg = df_pg[pg_cols_order]

                buf = StringIO()
                df_pg.to_csv(buf, index=False, header=False)
                buf.seek(0)
                columns_quoted = [f'"{col}"' for col in pg_cols_order]
                copy_sql = (
                    f"COPY public.fx_data ({','.join(columns_quoted)}) "
                    f"FROM STDIN WITH CSV"
                )

                with pg_cur.copy(copy_sql) as copy:
                    copy.write(buf.getvalue())

                pg_conn.commit()

        print("Postgres FX upload complete")
        print("Rows inserted:", len(df_pg))

    except Exception as e:
        print("Error inserting FX into PostgreSQL:", e)

    return f"UPLOAD_SUCCESS_{nrows}_ROWS"
