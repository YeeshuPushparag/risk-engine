import os
import hashlib
from io import BytesIO, StringIO
import numpy as np
import pandas as pd
from dateutil import parser
import boto3

from connections.snowflake_conn import get_snowflake_conn
from connections.postgre_conn import get_postgre_conn
from snowflake.connector.pandas_tools import write_pandas


# ================== CONSTANTS ==================
MACRO_BUCKET = "daily-equity-portfolio"
MACRO_KEY = "historical-equity/macro_data.csv"

s3 = boto3.client("s3")

INITIAL_MARGIN = {"FX": 0.05, "Commodity": 0.08, "Bond": 0.04, "Equity": 0.10}
HAIRCUT = {"Cash": 0.00, "Treasury": 0.02, "CorporateBond": 0.10, "Equity": 0.15}
COLLATERAL_TYPE = {"FX": "Cash", "Commodity": "Treasury", "Bond": "Treasury", "Equity": "Equity"}


# ================== UTILS ==================
def generate_trade_id(row):
    parts = [row["derivative_type"], row["ticker"], row["date"].strftime("%Y%m%d")]

    if pd.notna(row.get("commodity_sym")):
        parts.append(str(row["commodity_sym"]).replace("=", "_"))

    if pd.notna(row.get("asset_manager")):
        parts.append(str(row["asset_manager"]).split()[0])

    return "_".join(parts)


def parse_dates_safe(series):
    d1 = pd.to_datetime(series, errors="coerce")
    if d1.notna().sum() >= len(series) * 0.8:
        return d1
    return pd.to_datetime(series, errors="coerce", dayfirst=True)


def fast_counterparty(series):
    pool = np.array(["CP_A", "CP_B", "CP_C", "CP_D", "CP_E"])
    return series.astype(str).apply(
        lambda s: pool[int(hashlib.sha256(s.encode()).hexdigest(), 16) % len(pool)]
    )


def ensure_cols(df, cols):
    for c in cols:
        if c not in df.columns:
            df[c] = np.nan
    return df


def load_table(table):
    q = f'''
        SELECT *
        FROM "{table}"
        WHERE "date" > (SELECT MAX("date") FROM "DERIVATIVES")
    '''
    with get_snowflake_conn() as ctx:
        with ctx.cursor() as cs:
            cs.execute(q)
            return cs.fetch_pandas_all()


def load_macro():
    obj = s3.get_object(Bucket=MACRO_BUCKET, Key=MACRO_KEY)
    macro = pd.read_csv(BytesIO(obj["Body"].read()))
    macro["date"] = pd.to_datetime(macro["date"], errors="coerce")
    return macro[macro["date"].notna()]


# ================== MAIN AIRFLOW TASK ==================
def run_derivatives_processing():
    frames = []

    # ---------- FX ----------
    fx = load_table("FX")
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

    # ---------- COMMODITY ----------
    cmd = load_table("COMMODITY")
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

    # ---------- BONDS ----------
    bnd = load_table("BONDS")
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

    # ---------- EQUITY ----------
    eq = load_table("EQUITY")
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

    # ---------- COMBINE ----------
    non_empty_frames = [f for f in frames if not f.empty]

    if not non_empty_frames:
        return "NO_NEW_ROWS"

    df = pd.concat(non_empty_frames, ignore_index=True)
    df["counterparty"] = fast_counterparty(df["ticker"])
    df["trade_id"] = df.apply(generate_trade_id, axis=1)

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

    mask_all = df[["delta", "gamma", "vega"]].isna().all(axis=1)
    df.loc[mask_all, ["delta", "gamma", "vega"]] = [1.0, 0.0, 0.0]

    for c in ["delta", "gamma", "vega"]:
        df[c] = df[c].fillna(df[c].median(skipna=True)).clip(lower=0)

    df["delta_equivalent_exposure"] = df["delta"] * df["notional"]

    np.random.seed(42)
    rand_years = np.random.choice(np.arange(0.5, 5.1, 0.5), size=len(df))
    df["date"] = pd.to_datetime(df["date"])
    df["maturity_date"] = df["date"] + pd.to_timedelta((rand_years * 365).round(), unit="D")
    df["tenor_years"] = rand_years

    vol = df.groupby("asset_class")["underlying_price"].transform("std").fillna(0)
    df["pnl"] = df["delta_equivalent_exposure"] * 0.001 * (vol / (vol.max() or 1))

    df = df.drop(columns=["commodity_sym", "asset_manager"], errors="ignore")

    macro = load_macro()
    df["month_year"] = df["date"].dt.to_period("M").astype(str)
    macro["month_year"] = macro["date"].dt.to_period("M").astype(str)

    merged = df.merge(
        macro.drop(columns=["date"]),
        on="month_year",
        how="inner",
    ).drop(columns=["month_year"])

    merged["date"] = pd.to_datetime(merged["date"]).dt.date
    merged["maturity_date"] = pd.to_datetime(merged["maturity_date"]).dt.date

    with get_snowflake_conn() as ctx:
        success, _, nrows, _ = write_pandas(
            ctx,
            merged,
            "DERIVATIVES",
            chunk_size=100000,
            quote_identifiers=True,
        )

    # ---------- POSTGRES ----------
    df_pg = merged.copy()
    df_pg["margin_call_flag"] = df_pg["margin_call_flag"].astype(bool)

    for col in df_pg.columns:
        if col not in [
            "trade_id", "counterparty", "asset_class", "derivative_type",
            "ticker", "sector", "industry", "collateral_type",
            "date", "maturity_date", "margin_call_flag"
        ]:
            df_pg[col] = pd.to_numeric(df_pg[col], errors="coerce")

    BATCH_SIZE = 100_000

    with get_postgre_conn() as pg_conn:
        with pg_conn.cursor() as cur:
            cur.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema='public'
                AND table_name='derivative_data'
                ORDER BY ordinal_position
            """)
            cols = [r[0] for r in cur.fetchall() if r[0] != "id"]

            copy_sql = f'''
                COPY public.derivative_data ({",".join(f'"{c}"' for c in cols)})
                FROM STDIN WITH CSV
            '''

            for start in range(0, len(df_pg), BATCH_SIZE):
                end = start + BATCH_SIZE
                chunk = df_pg.iloc[start:end]

                buf = StringIO()
                chunk[cols].to_csv(buf, index=False, header=False)
                buf.seek(0)

                with cur.copy(copy_sql) as copy:
                    copy.write(buf.getvalue())

        pg_conn.commit()

    return f"SUCCESS_{nrows}"
