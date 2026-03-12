import os
import pandas as pd
from datetime import datetime
import joblib
import xgboost as xgb
import numpy as np
from connections.snowflake_conn import get_snowflake_conn
from snowflake.connector.pandas_tools import write_pandas
import boto3
from io import BytesIO, StringIO
from connections.postgre_conn import get_postgre_conn

S3_BUCKET = "pushparag-commodity-bucket"
s3 = boto3.client("s3")

# === S3 FILES ===
INPUT_COMMOD = "commodities_daily.parquet"
SYM = "unique_tickers_sector.csv"
MACRO = "macro_data.csv"
MODEL_FILE = "commodities_model_vol21_xgb.json"
FEATURE_FILE = "commodities_features_vol21.pkl"


def load_s3_csv(key, parse_dates=None, dayfirst=True):
    """Load CSV from S3"""
    obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
    return pd.read_csv(BytesIO(obj["Body"].read()), parse_dates=parse_dates, dayfirst=dayfirst)


def load_s3_parquet(key):
    """Load parquet from S3"""
    obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
    df = pd.read_parquet(BytesIO(obj["Body"].read()))
    return df


def process_commodities():

    # === Load company and macro data ===
    companies = load_s3_csv(SYM, dayfirst=True)
    macro = load_s3_csv(MACRO, parse_dates=["date"], dayfirst=True)

    # === Connect to Snowflake ===
    with get_snowflake_conn() as ctx:
        with ctx.cursor() as cs:

            cs.execute('SELECT MAX("date") FROM "COMMODITY"')
            max_date_commo = cs.fetchone()[0]

            if max_date_commo is not None:
                max_date_commo = pd.Timestamp(max_date_commo).date()
            else:
                max_date_commo = pd.Timestamp("1970-01-01").date()

            cs.execute(
                f"""
                SELECT "ticker","date","mtm_value","asset_manager"
                FROM EQUITY
                WHERE "date" > '{max_date_commo}'
                """
            )

            mtm_rows = cs.fetchall()
            mtm_cols = ["ticker", "date", "mtm_value", "asset_manager"]

            mtm = pd.DataFrame(mtm_rows, columns=mtm_cols)

            mtm["date"] = pd.to_datetime(mtm["date"])

    if mtm.empty:
        return "NO_NEW_MTM_ROWS"

    # === Load commodity parquet ===
    commod_base = load_s3_parquet(INPUT_COMMOD)

    commod_base["date"] = pd.to_datetime(commod_base["date"])

    commod_base = commod_base[commod_base["date"].dt.date > max_date_commo]

    if commod_base.empty:
        return "NO_NEW_COMMOD_ROWS"

    commod_base.rename(columns={"commodity_symbol": "commodity"}, inplace=True)

    # === Sector → Commodity mapping ===
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

    rows = []

    for _, r in companies.iterrows():

        mapping = sector_to_commodities.get(r["sector"])

        if not mapping:
            continue

        for comm, weight in mapping.items():

            rows.append(
                {
                    "ticker": r["ticker"],
                    "sector": r["sector"],
                    "industry": r["industry"],
                    "commodity": comm,
                    "sensitivity": weight,
                }
            )

    exp = pd.DataFrame(rows)

    # === Merge commodity data ===
    seg = exp.merge(commod_base, on="commodity", how="left", validate="m:m")

    # === Merge MTM ===
    seg = seg.merge(mtm, on=["ticker", "date"], how="inner")

    seg["mtm_value"] = pd.to_numeric(seg["mtm_value"], errors="coerce").fillna(0)

    seg["vol_20d"] = pd.to_numeric(seg["vol_20d"], errors="coerce")

    seg["daily_return"] = pd.to_numeric(seg["daily_return"], errors="coerce").fillna(0.0)

    seg["hedge_ratio"] = (0.2 + 0.6 * seg["vol_20d"].rank(pct=True)).clip(0, 1)

    seg["exposure_amount"] = seg["sensitivity"] * seg["mtm_value"]

    seg["commodity_pnl"] = (
        seg["exposure_amount"]
        * seg["daily_return"]
        * (1 - seg["hedge_ratio"])
    )

    # === Merge macro ===
    seg["date"] = pd.to_datetime(seg["date"])
    macro["date"] = pd.to_datetime(macro["date"])

    seg["mm_yy"] = seg["date"].dt.strftime("%m-%y")
    macro["mm_yy"] = macro["date"].dt.strftime("%m-%y")

    macro_for_merge = macro.drop(columns=["date"])

    seg = seg.merge(macro_for_merge, on="mm_yy", how="left").drop(columns=["mm_yy"])

    # === Final column selection ===
    cols = [
        "ticker","asset_manager","sector","industry","commodity","date",
        "open","high","low","close","volume","daily_return","log_return",
        "vol_20d","sensitivity","hedge_ratio","mtm_value","exposure_amount",
        "commodity_pnl","VaR_95","VaR_99","gdp","unrate","cpi","fedfunds",
    ]

    available_cols = [c for c in cols if c in seg.columns]

    final_new = seg[available_cols].sort_values(
        ["commodity","date","ticker"]
    ).reset_index(drop=True)

    # === Load XGBoost model ===
    model_obj = s3.get_object(Bucket=S3_BUCKET, Key=MODEL_FILE)

    booster = xgb.Booster()
    booster.load_model(bytearray(model_obj["Body"].read()))

    feat_obj = s3.get_object(Bucket=S3_BUCKET, Key=FEATURE_FILE)

    feature_cols = joblib.load(BytesIO(feat_obj["Body"].read()))

    X_all = final_new.reindex(columns=feature_cols).replace([np.inf, -np.inf], 0).fillna(0)

    dmat = xgb.DMatrix(X_all)

    final_new["pred_vol21"] = booster.predict(dmat)

    final_new["date"] = final_new["date"].dt.date

    # === Upload to Snowflake ===
    with get_snowflake_conn() as ctx:

        success, nchunks, nrows, _ = write_pandas(
            ctx,
            final_new,
            "COMMODITY",
            chunk_size=20000,
            quote_identifiers=True,
        )

    if not success:
        return "UPLOAD_FAILED"

    # === Upload to PostgreSQL ===
    try:

        df_pg = final_new.copy()

        df_pg["volume"] = df_pg["volume"].fillna(0).astype(float).round().astype(int)

        BATCH_SIZE = 100000

        with get_postgre_conn() as pg_conn:

            with pg_conn.cursor() as pg_cur:

                pg_cur.execute("""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema='public'
                    AND table_name='commodity_data'
                    ORDER BY ordinal_position
                """)

                pg_cols_order = [
                    r[0] for r in pg_cur.fetchall()
                    if r[0] != "id"
                ]

                quoted_cols = [f'"{c}"' for c in pg_cols_order]

                copy_sql = (
                    f"COPY public.commodity_data ({','.join(quoted_cols)}) "
                    f"FROM STDIN WITH CSV"
                )

                for start in range(0, len(df_pg), BATCH_SIZE):

                    chunk = df_pg.iloc[start:start+BATCH_SIZE][pg_cols_order]

                    buf = StringIO()
                    chunk.to_csv(buf, index=False, header=False)
                    buf.seek(0)

                    with pg_cur.copy(copy_sql) as copy:
                        copy.write(buf.getvalue())

                pg_conn.commit()

        print(f"PostgreSQL commodity_data upload complete. Rows: {len(df_pg)}")

    except Exception as e:

        print("Error inserting commodity data into PostgreSQL:", e)

    return f"UPLOAD_SUCCESS_{nrows}_ROWS"
