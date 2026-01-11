import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import xgboost as xgb
import joblib
import boto3
from connections.snowflake_conn import get_snowflake_conn
from snowflake.connector.pandas_tools import write_pandas
from io import BytesIO, StringIO
from fredapi import Fred
from connections.postgre_conn import get_postgre_conn


S3_BUCKET = "daily-bonds-pushparag"
s3 = boto3.client("s3")
FRED_API_KEY = os.getenv("FRED_API_KEY")




def create_daily_bond_data(start_date, end_date):
    """Create daily bond data with market dynamics + macro + DGS10 enrichment"""

    # -------- Load base bond data from S3 -------- #
    bond_obj = s3.get_object(Bucket=S3_BUCKET, Key="synthetic_bond.csv")
    base = pd.read_csv(BytesIO(bond_obj["Body"].read()))

    # -------- Generate repeated daily rows -------- #
    dates = pd.date_range(start_date, end_date, freq="B")
    n_days = len(dates)

    daily = base.loc[base.index.repeat(n_days)].copy()
    daily["date"] = np.tile(dates, len(base))

    # -------- volatility mapping -------- #
    vol_map = {
        "AAA": 2, "AA+": 3, "AA": 4, "A+": 5, "A": 6, "A-": 8,
        "BBB+": 10, "BBB": 12, "BBB-": 15,
        "BB+": 20, "BB": 25, "B": 35, "CCC": 50
    }
    daily["vol"] = daily["credit_rating"].map(vol_map).fillna(10) / 100

    # ======================================================
    # FETCH DGS10 FROM FRED + ALIGN BUSINESS DATES
    # ======================================================
    fred = Fred(api_key=FRED_API_KEY)
    dgs10 = fred.get_series(
        "DGS10",
        observation_start=start_date,
        observation_end=end_date
    )

    dgs10 = dgs10.to_frame("DGS10").reset_index().rename(columns={"index": "date"})
    business_days = pd.DataFrame({"date": dates})

    dgs10 = business_days.merge(dgs10, on="date", how="left").ffill()

    dgs10["DGS10_ma"] = dgs10["DGS10"].rolling(20, min_periods=1).mean()
    dgs10["dgs10_anom"] = dgs10["DGS10"] - dgs10["DGS10_ma"]

    # merge dgs10 into daily
    daily = daily.merge(dgs10, on="date", how="left")

    # ======================================================
    # MERGE MACRO DATA WITH MONTH-YEAR ALIGNMENT (S3)
    # ======================================================
    try:
        macro_obj = s3.get_object(Bucket=S3_BUCKET, Key="macro_data.csv")
        macro = pd.read_csv(BytesIO(macro_obj["Body"].read()), parse_dates=["date"])

        macro["mm_yy"] = macro["date"].dt.strftime("%m-%y")
        daily["mm_yy"] = daily["date"].dt.strftime("%m-%y")

        merged = daily.merge(
            macro.drop(columns=["date"]),
            on="mm_yy",
            how="inner"
        ).drop(columns=["mm_yy"])

    except Exception as e:
        print("Warning: macro merge failed:", e)
        merged = daily

    return merged



def run_predictions(new_rows):
    """Same prediction logic, no changes."""
    if new_rows.empty:
        return new_rows

    try:
        model_spread = xgb.Booster()
        model_spread.load_model(bytearray(
            s3.get_object(Bucket=S3_BUCKET, Key="bond_model_spread5d_xgb.json")["Body"].read()
        ))
        model_pd = xgb.Booster()
        model_pd.load_model(bytearray(
            s3.get_object(Bucket=S3_BUCKET, Key="bond_model_pd21d_xgb.json")["Body"].read()
        ))

        features_spread = joblib.load(BytesIO(
            s3.get_object(Bucket=S3_BUCKET, Key="bond_features_spread5d.pkl")["Body"].read()
        ))
        features_pd = joblib.load(BytesIO(
            s3.get_object(Bucket=S3_BUCKET, Key="bond_features_pd21d.pkl")["Body"].read()
        ))

    except Exception:
        return new_rows

    if all(f in new_rows.columns for f in features_spread):
        new_rows["pred_spread_5d"] = model_spread.predict(
            xgb.DMatrix(new_rows.reindex(columns=features_spread))
        )

    if all(f in new_rows.columns for f in features_pd):
        new_rows["pred_pd_21d"] = model_pd.predict(
            xgb.DMatrix(new_rows.reindex(columns=features_pd))
        )

    return new_rows

def update_bonds_snowflake():
    """Airflow-callable wrapper. Returns status string."""
    today = datetime.today().date()

    with get_snowflake_conn() as ctx:
        with ctx.cursor() as cs:
            cs.execute('SELECT MAX("date") FROM "BONDS"')
            last_date_sf = cs.fetchone()[0]

    last_date_sf = pd.Timestamp(last_date_sf).date() if last_date_sf else pd.Timestamp("1970-01-01").date()

    if last_date_sf >= today:
        return "ALREADY_UPDATED"

    start_date = last_date_sf + timedelta(days=1)
    df = create_daily_bond_data(start_date.strftime("%Y-%m-%d"), today.strftime("%Y-%m-%d"))
    # Exclude weekends
    new_rows = df[df["date"].dt.weekday < 5]  # weekdays are 0 to 4 (Mon-Fri)
    new_rows = new_rows[new_rows["date"].dt.date > last_date_sf]
    if new_rows.empty:
        return "NO_NEW_ROWS"

    predicted_df = run_predictions(new_rows)
    predicted_df["date"] = predicted_df["date"].dt.date

    final_cols = [
        "bond_id","ticker","sector","industry","credit_rating","coupon_rate",
        "issue_date","maturity_date","maturity_years","date","benchmark_yield",
        "corporate_yield","credit_spread","bond_price","yield_to_maturity",
        "implied_hazard","implied_pd_annual","implied_pd_multi_year","implied_rating",
        "market_synthetic_score","DGS10","DGS10_ma","dgs10_anom",
        "gdp","unrate","fedfunds","cpi",
        "pred_spread_5d","pred_pd_21d"
    ]

    df_to_upload = predicted_df[[c for c in final_cols if c in predicted_df.columns]]
    
    with get_snowflake_conn() as ctx:
        success, _, nrows, _ = write_pandas(ctx, df_to_upload, "BONDS",
                                            chunk_size=20000, quote_identifiers=True)

    if not success:
        return "UPLOAD_FAILED"

    try:
        df_pg = df_to_upload.copy()
        print("FX columns:", df_pg.columns.tolist())

        # convert volumes and numeric positions to integer types if needed later
        # (bonds probably don’t have integer columns but pipeline keeps consistency)

        # get ordered columns from DB
        with get_postgre_conn() as pg_conn:
            with pg_conn.cursor() as pg_cur:

                pg_cur.execute("""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema='public'
                    AND table_name='bond_data'
                    ORDER BY ordinal_position
                """)

                pg_cols_order = [
                    row[0] for row in pg_cur.fetchall()
                    if row[0] != "id"
                ]

                # enforce same ordering in dataframe
                df_pg = df_pg[pg_cols_order]

                # write df as CSV to buffer
                buf = StringIO()
                df_pg.to_csv(buf, index=False, header=False)
                buf.seek(0)

                # *** QUOTE identifiers because of mixed-case columns ***
                quoted_cols = [f'"{col}"' for col in pg_cols_order]

                copy_sql = (
                    f"COPY public.bond_data ({','.join(quoted_cols)}) "
                    f"FROM STDIN WITH CSV"
                )

                with pg_cur.copy(copy_sql) as copy:
                    copy.write(buf.getvalue())

                pg_conn.commit()

        print(f"PostgreSQL bond_data upload complete. Rows: {len(df_pg)}")

    except Exception as e:
        print("Error inserting Bond data into PostgreSQL:", e)



    return f"SUCCESS_{nrows}_ROWS"
